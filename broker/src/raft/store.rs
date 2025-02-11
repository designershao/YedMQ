use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::AnyError;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use rocksdb::ColumnFamily;
use rocksdb::DB;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;
use yedmq::topic::Subscription;

use crate::topic::TopicManager;

use super::typ;
use super::SnapshotData;
use super::TypeConfig;
use super::{Node, NodeId};

type StorageResult<T> = Result<T, StorageError<NodeId>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    // Subscribe topic
    SubscribeTopic {
        node: Node,
        tenant_id: String,
        client_identifier: String,
        topic: String,
        qos: u8,
    },
    // Unsubscribe topic
    UnsubscribeTopic {
        node: Node,
        tenant_id: String,
        client_identifier: String,
        topic: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {

    get_topic_subscriptions_response { subscriptions: Vec<Subscription> },

    get_topic_router_response { nodes: Vec<Node> },

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,

    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotWrapper {

    pub topic_manager_snapshot: Vec<u8>,

    pub topic_router_snapshot: Vec<u8>,

}

#[derive(Debug, Clone)]
pub struct State {

    pub topic_manager: Arc<RwLock<TopicManager>>,

    pub topic_router: Arc<RwLock<BTreeMap<String, Vec<Node>>>>,

}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    pub last_applied_log_id: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, Node>,

    pub state: State,
}

#[derive(Debug, Clone)]
pub struct StateMachineStore {
    pub data: StateMachineData,

    snapshot_idx: u64,

    db: Arc<DB>,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {

    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let last_applied_log = self.data.last_applied_log_id;
        let last_membership = self.data.last_membership.clone();

        let snapshot_json = {
            let topic_manager = self.data.state.topic_manager.read().await;
            let topic_manager_serialized = serde_json::to_vec(&*topic_manager)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            let topic_router = self.data.state.topic_router.read().await;
            let topic_router_serialized = serde_json::to_vec(&*topic_router)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;

            let snapshot_data = SnapshotWrapper {
                topic_manager_snapshot: topic_manager_serialized,
                topic_router_snapshot: topic_router_serialized,
            };
            serde_json::to_vec(&snapshot_data)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, self.snapshot_idx)
        } else {
            format!("--{}", self.snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };
        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot_json.clone(),
        };
        self.set_current_snapshot_(snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_json)),
        })
    }
}

impl StateMachineStore {
    fn set_current_snapshot_(&self, snap: StoredSnapshot) -> StorageResult<()> {
        self.db
            .put_cf(
                self.store(),
                b"snapshot",
                serde_json::to_vec(&snap).unwrap().as_slice(),
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
            })?;
        self.flush(
            ErrorSubject::Snapshot(Some(snap.meta.signature())),
            ErrorVerb::Write,
        )?;
        Ok(())
    }

    async fn update_state_machine_(&mut self, snapshot: StoredSnapshot) -> Result<(), StorageError<NodeId>> {
        let state: SnapshotWrapper = serde_json::from_slice(&snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(snapshot.meta.signature()), &e))?;

        self.data.last_applied_log_id = snapshot.meta.last_log_id;
        self.data.last_membership = snapshot.meta.last_membership.clone();

        let mut topic_manager = self.data.state.topic_manager.write().await;
        let mut topic_router = self.data.state.topic_router.write().await;

        *topic_router = serde_json::from_slice(&state.topic_router_snapshot)
            .map_err(|e| StorageIOError::read_snapshot(Some(snapshot.meta.signature()), &e))?;
        *topic_manager = serde_json::from_slice(&state.topic_manager_snapshot)
            .map_err(|e| StorageIOError::read_snapshot(Some(snapshot.meta.signature()), &e))?;

        Ok(())
    }

    fn get_current_snapshot_(&self) -> StorageResult<Option<StoredSnapshot>> {
        Ok(self
            .db
            .get_cf(self.store(), b"snapshot")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }


    fn flush(
        &self,
        subject: ErrorSubject<NodeId>,
        verb: ErrorVerb,
    ) -> Result<(), StorageIOError<NodeId>> {
        self.db
            .flush_wal(true)
            .map_err(|e| StorageIOError::new(subject, verb, AnyError::new(&e)))?;
        Ok(())
    }

    fn store(&self) -> &ColumnFamily {
        self.db.cf_handle("store").unwrap()
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        Ok((self.data.last_applied_log_id, self.data.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = typ::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let replies = Vec::with_capacity(entries.size_hint().0);

        for ent in entries {
            self.data.last_applied_log_id = Some(ent.log_id);

            //let mut resp_value = None;

            match ent.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(req) => match req {
                    Request::SubscribeTopic { node, tenant_id,client_identifier, topic, qos } => {
                        let mut topic_manager = self.data.state.topic_manager.write().await;
                        if !topic_manager.contains_tenant(&tenant_id) {
                            topic_manager.create_tenant(tenant_id.clone());
                        }
                        let _= topic_manager.subscription(
                            tenant_id,
                            client_identifier,
                            topic.clone(),
                            qos
                        );
                        
                        let mut topic_router = self.data.state.topic_router.write().await;
                        if topic_router.contains_key(&topic) {
                            let nodes = topic_router.get_mut(&topic).unwrap();
                            nodes.push(node);
                        } else {
                            topic_router.insert(topic.clone(), vec![node]);
                        }
                    }
                    Request::UnsubscribeTopic { node, tenant_id, client_identifier,topic } => {
                        let mut topic_manager = self.data.state.topic_manager.write().await;

                        let _ = topic_manager.unsubscription(
                            tenant_id,
                            client_identifier,
                            topic.clone()
                        );

                        let mut topic_router = self.data.state.topic_router.write().await;
                        if topic_router.contains_key(&topic) {
                            let nodes = topic_router.get_mut(&topic).unwrap();
                            nodes.retain(|x| *x != node);
                        }
                    }
                },
                EntryPayload::Membership(mem) => {
                    self.data.last_membership = StoredMembership::new(Some(ent.log_id), mem);
                }
            }
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        self.update_state_machine_(new_snapshot.clone()).await?;

        self.set_current_snapshot_(new_snapshot)?;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let x = self.get_current_snapshot_()?;
        Ok(x.map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}
