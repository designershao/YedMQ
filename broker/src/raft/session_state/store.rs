use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::raft::Node;
use crate::raft::NodeId;
use crate::raft::SnapshotData;
use crate::session::session_state_storage::SessionStateStorage;
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use log::{debug, warn};
use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::AnyError;
use openraft::Entry;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::LogState;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;
use rocksdb::ColumnFamily;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::Direction;
use rocksdb::Options;
use rocksdb::DB;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use super::types;
use super::types::SessionStateResponse;
use super::types::SessionStateTypeConfig;
type StorageResult<T> = Result<T, StorageError<NodeId>>;

#[derive(Debug, Clone)]
pub struct State {
    pub session_state_storage: Arc<RwLock<SessionStateStorage>>,
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    pub last_applied_log_id: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, Node>,

    pub state: State,
}

use crate::raft::payload::PayloadStore;

use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone)]
pub struct StateMachineStore {
    pub data: StateMachineData,

    snapshot_idx: u64,

    db: Arc<DB>,

    payload_store: Arc<dyn PayloadStore>,
    payload_gc_queue: Arc<RwLock<Vec<PayloadGcItem>>>,

    settings: Arc<crate::settings::Settings>,

    pub is_ready: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,

    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotWrapper {
    pub session_state_storage_snapshot: Vec<u8>,
}

#[derive(Debug, Clone)]
struct PayloadGcItem {
    delete_after_log_index: u64,
    key: String,
}

impl RaftSnapshotBuilder<SessionStateTypeConfig> for StateMachineStore {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<SessionStateTypeConfig>, StorageError<NodeId>> {
        let last_applied_log = self.data.last_applied_log_id;
        let last_membership = self.data.last_membership.clone();

        let snapshot_json = {
            let snapshot_data = SnapshotWrapper {
                session_state_storage_snapshot: self
                    .data
                    .state
                    .session_state_storage
                    .read()
                    .await
                    .to_snapshot(),
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
    pub async fn get_active_payload_keys(&self) -> std::collections::HashSet<String> {
        let storage = self.data.state.session_state_storage.read().await;
        storage.ref_counts.keys().cloned().collect()
    }

    async fn new(
        db: Arc<DB>,
        session_state_storage: Arc<RwLock<SessionStateStorage>>,
        payload_store: Arc<dyn PayloadStore>,
        payload_gc_queue: Arc<RwLock<Vec<PayloadGcItem>>>,
        settings: Arc<crate::settings::Settings>,
    ) -> Result<StateMachineStore, StorageError<NodeId>> {
        let mut sm = Self {
            data: StateMachineData {
                last_applied_log_id: None,
                last_membership: Default::default(),
                state: State {
                    session_state_storage,
                },
            },
            snapshot_idx: 0,
            db,
            payload_store,
            payload_gc_queue,
            settings,
            is_ready: Arc::new(AtomicBool::new(true)),
        };

        let snapshot = sm.get_current_snapshot_()?;
        if let Some(snap) = snapshot {
            sm.update_state_machine_(snap).await?;
        }

        Ok(sm)
    }

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

    async fn update_state_machine_(
        &mut self,
        snapshot: StoredSnapshot,
    ) -> Result<(), StorageError<NodeId>> {
        let state: SnapshotWrapper = serde_json::from_slice(&snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(snapshot.meta.signature()), &e))?;

        self.data.last_applied_log_id = snapshot.meta.last_log_id;
        self.data.last_membership = snapshot.meta.last_membership.clone();

        let mut session_state_storage = self.data.state.session_state_storage.write().await;

        *session_state_storage =
            SessionStateStorage::from_snapshot(state.session_state_storage_snapshot);

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

    async fn schedule_payload_gc(&self, log_index: u64, key: String) {
        self.payload_gc_queue.write().await.push(PayloadGcItem {
            delete_after_log_index: log_index,
            key,
        });
    }
}

impl RaftStateMachine<SessionStateTypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        Ok((
            self.data.last_applied_log_id,
            self.data.last_membership.clone(),
        ))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<SessionStateResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = types::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);

        for ent in entries {
            self.data.last_applied_log_id = Some(ent.log_id);
            let current_log_index = ent.log_id.index;

            match ent.payload {
                openraft::EntryPayload::Blank => {
                    replies.push(SessionStateResponse::None);
                }

                openraft::EntryPayload::Normal(req) => match req {
                    types::SessionStateRequest::InflightRegisterRxPacket {
                        tenant_id,
                        client_id,
                        packet_id,
                        qos,
                        packet_key,
                    } => {
                        let freed_key = self
                            .data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .inflight_register_rx_packet(
                                &tenant_id,
                                &client_id,
                                packet_id,
                                qos,
                                &packet_key,
                            )
                            .await;
                        if let Some(key) = freed_key {
                            self.schedule_payload_gc(current_log_index, key).await;
                        }
                        replies.push(SessionStateResponse::InflightRegisterRxPacketResponse(Ok(
                            (),
                        )));
                    }
                    types::SessionStateRequest::InflightRegisterTxPacket {
                        tenant_id,
                        client_id,
                        packet_id,
                        qos,
                        packet_key,
                    } => {
                        let r = self
                            .data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .inflight_register_tx_packet(
                                &tenant_id,
                                &client_id,
                                packet_id,
                                qos,
                                &packet_key,
                            )
                            .await;

                        match r {
                            Ok(freed_key) => {
                                if let Some(key) = freed_key {
                                    self.schedule_payload_gc(current_log_index, key).await;
                                }
                                replies.push(
                                    SessionStateResponse::InflightRegisterTxPacketResponse(Ok(())),
                                );
                            }
                            Err(e) => {
                                replies.push(
                                    SessionStateResponse::InflightRegisterTxPacketResponse(Err(e)),
                                );
                            }
                        }
                    }
                    types::SessionStateRequest::InflightGetCurrentPacket {
                        tenant_id,
                        client_id,
                        packet_identifier,
                    } => {
                        let packet_key = self
                            .data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .inflight_get_current_packet_key(
                                tenant_id,
                                client_id,
                                packet_identifier.try_into().unwrap(),
                            )
                            .await;
                        replies.push(SessionStateResponse::InflightGetCurrentPacketResult(
                            packet_key,
                        ));
                    }
                    types::SessionStateRequest::InflightNextState {
                        tenant_id,
                        client_id,
                        packet_identifier,
                    } => {
                        let freed_key = self
                            .data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .inflight_next_state(
                                tenant_id,
                                client_id,
                                packet_identifier.try_into().unwrap(),
                            )
                            .await;
                        if let Some(key) = freed_key {
                            self.schedule_payload_gc(current_log_index, key).await;
                        }
                        replies.push(SessionStateResponse::None);
                    }
                    types::SessionStateRequest::InflightCleanFinishItems {
                        tenant_id,
                        client_id,
                    } => {
                        let freed_keys = self
                            .data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .inflight_clean_finished_items(tenant_id, client_id)
                            .await;
                        for key in freed_keys {
                            self.schedule_payload_gc(current_log_index, key).await;
                        }
                        replies.push(SessionStateResponse::None);
                    }
                    types::SessionStateRequest::AppendToPendingQueue {
                        tenant_id,
                        client_id,
                        packet_key,
                    } => {
                        self.data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .append_to_pending_queue(&tenant_id, &client_id, &packet_key)
                            .await;
                        replies.push(SessionStateResponse::None);
                    }
                    types::SessionStateRequest::PopFromPendingQueue {
                        tenant_id,
                        client_id,
                    } => {
                        let (packet_key, freed_key) = self
                            .data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .pop_from_pending_queue(&tenant_id, &client_id)
                            .await;

                        let mut payload_data = None;
                        if let Some(key) = freed_key {
                            if let Ok(Some(bytes)) = self.payload_store.get(&key).await {
                                payload_data = Some(bytes.to_vec());
                            }
                            self.schedule_payload_gc(current_log_index, key).await;
                        }
                        replies.push(SessionStateResponse::PopFromPendingQueueResult(
                            packet_key,
                            payload_data,
                        ));
                    }
                    types::SessionStateRequest::SubscribeTopic {
                        tenant_id,
                        client_id,
                        topic,
                        qos,
                    } => {
                        self.data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .subscribe_topic(&tenant_id, &client_id, &topic, qos.into())
                            .await;
                        replies.push(SessionStateResponse::None);
                    }
                    types::SessionStateRequest::UnsubscribeTopic {
                        tenant_id,
                        client_id,
                        topic,
                    } => {
                        self.data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .unsubscribe_topic(tenant_id, client_id, topic)
                            .await;
                        replies.push(SessionStateResponse::None);
                    }
                    types::SessionStateRequest::CreateSessionState {
                        tenant_id,
                        client_id,
                        inflight_duration_secs,
                    } => {
                        self.data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .create_session_state(
                                &tenant_id,
                                &client_id,
                                Duration::from_secs(inflight_duration_secs),
                            )
                            .await;
                        replies.push(SessionStateResponse::None);
                    }
                    types::SessionStateRequest::DeleteSessionState {
                        tenant_id,
                        client_id,
                        expected_disconnected_at,
                    } => {
                        let should_delete = if let Some(expected) = expected_disconnected_at {
                            let storage = self.data.state.session_state_storage.read().await;
                            if let Some(session_arc) =
                                storage.get_session_state(&tenant_id, &client_id).await
                            {
                                session_arc.read().await.disconnected_at == Some(expected)
                            } else {
                                false
                            }
                        } else {
                            true
                        };

                        if should_delete {
                            let freed_keys = self
                                .data
                                .state
                                .session_state_storage
                                .write()
                                .await
                                .delete_session_state(&tenant_id, &client_id)
                                .await;
                            for key in freed_keys {
                                self.schedule_payload_gc(current_log_index, key).await;
                            }
                        }
                        replies.push(SessionStateResponse::None);
                    }
                    types::SessionStateRequest::UpdateSessionConnectionState {
                        tenant_id,
                        client_id,
                        disconnected_at,
                    } => {
                        self.data
                            .state
                            .session_state_storage
                            .write()
                            .await
                            .update_connection_state(tenant_id, client_id, disconnected_at)
                            .await;
                        replies.push(SessionStateResponse::None);
                    }
                },
                openraft::EntryPayload::Membership(membership) => {
                    self.data.last_membership = StoredMembership::new(Some(ent.log_id), membership);
                    replies.push(SessionStateResponse::None);
                }
            }
        }
        Ok(replies)
    }
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let snapshot_data = snapshot.into_inner();

        // 0. Set is_ready to false during sync
        self.is_ready.store(false, Ordering::SeqCst);

        // 1. Parse snapshot data to find all packet keys
        let wrapper: SnapshotWrapper = serde_json::from_slice(&snapshot_data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        let storage_data: crate::session::session_state_storage::SerializableSessionStateStorage =
            serde_json::from_slice(&wrapper.session_state_storage_snapshot)
                .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        let mut keys_to_sync = std::collections::HashSet::new();
        for tenant_map in storage_data.inner.values() {
            for session_state in tenant_map.values() {
                for key in &session_state.pending_messages {
                    keys_to_sync.insert(key.clone());
                }
                for key in session_state.inflight.get_all_packet_keys() {
                    keys_to_sync.insert(key);
                }
            }
        }

        // 2. Diff with local store
        let mut missing_keys = Vec::new();
        for key in keys_to_sync {
            if !self.payload_store.contains(&key).await.unwrap_or(false) {
                missing_keys.push(key);
            }
        }

        if !missing_keys.is_empty() {
            log::info!(
                "Snapshot installation: found {} missing payloads, starting BulkSync",
                missing_keys.len()
            );

            // 3. Try to sync from peers
            let client = crate::raft::payload::PayloadClient::new(self.payload_store.clone());
            let mut synced = false;

            // Try all nodes in membership
            for (node_id, node) in meta.last_membership.nodes() {
                if *node_id == self.settings.cluster.node_id {
                    continue;
                }

                log::info!(
                    "Trying to sync payloads from node {} at {}",
                    node_id,
                    node.rpc_addr
                );

                // Construct manifest for what we want to fetch
                // For simplicity, we just ask for the keys we know are missing
                let manifest = missing_keys.iter().map(|k| (k.clone(), 0, 0)).collect();

                match client.bulk_sync(&node.rpc_addr, manifest).await {
                    Ok(_) => {
                        log::info!("BulkSync from node {} succeed", node_id);
                        synced = true;
                        break;
                    }
                    Err(e) => {
                        log::warn!("BulkSync from node {} failed: {}", node_id, e);
                    }
                }
            }

            if !synced {
                log::error!(
                    "Failed to sync missing payloads for snapshot. Data integrity compromised."
                );
                // NOTE: We keep is_ready = false here
                return Err(StorageError::IO {
                    source: StorageIOError::read_snapshot(
                        Some(meta.signature()),
                        AnyError::new(&std::io::Error::other("Payload sync failed")),
                    ),
                });
            }
        }

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot_data,
        };

        self.update_state_machine_(new_snapshot.clone()).await?;

        self.set_current_snapshot_(new_snapshot)?;

        // 4. Set is_ready back to true
        self.is_ready.store(true, Ordering::SeqCst);

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<SessionStateTypeConfig>>, StorageError<NodeId>> {
        let x = self.get_current_snapshot_()?;
        Ok(x.map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct LogStore {
    db: Arc<DB>,
    payload_store: Arc<dyn PayloadStore>,
    payload_gc_queue: Arc<RwLock<Vec<PayloadGcItem>>>,
}

fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

impl LogStore {
    fn store(&self) -> &ColumnFamily {
        self.db.cf_handle("store").unwrap()
    }

    fn logs(&self) -> &ColumnFamily {
        self.db.cf_handle("logs").unwrap()
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

    fn get_last_purged_(&self) -> StorageResult<Option<LogId<u64>>> {
        Ok(self
            .db
            .get_cf(self.store(), b"last_purged_log_id")
            .map_err(|e| StorageIOError::read(&e))?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_last_purged_(&self, log_id: LogId<u64>) -> StorageResult<()> {
        self.db
            .put_cf(
                self.store(),
                b"last_purged_log_id",
                serde_json::to_vec(&log_id).unwrap().as_slice(),
            )
            .map_err(|e| StorageIOError::write(&e))?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;
        Ok(())
    }

    fn set_committed_(
        &self,
        committed: &Option<LogId<NodeId>>,
    ) -> Result<(), StorageIOError<NodeId>> {
        let json = serde_json::to_vec(committed).unwrap();

        self.db
            .put_cf(self.store(), b"committed", json)
            .map_err(|e| StorageIOError::write(&e))?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;
        Ok(())
    }

    fn get_committed_(&self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(self
            .db
            .get_cf(self.store(), b"committed")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_vote_(&self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.db
            .put_cf(self.store(), b"vote", serde_json::to_vec(vote).unwrap())
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_vote(&e),
            })?;

        self.flush(ErrorSubject::Vote, ErrorVerb::Write)?;
        Ok(())
    }

    fn get_vote_(&self) -> StorageResult<Option<Vote<NodeId>>> {
        Ok(self
            .db
            .get_cf(self.store(), b"vote")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_vote(&e),
            })?
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }
}

impl RaftLogReader<SessionStateTypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + core::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<SessionStateTypeConfig>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };
        self.db
            .iterator_cf(
                self.logs(),
                rocksdb::IteratorMode::From(&start, Direction::Forward),
            )
            .map(|res| {
                let (id, val) = res.unwrap();
                let entry: StorageResult<Entry<_>> =
                    serde_json::from_slice(&val).map_err(|e| StorageError::IO {
                        source: StorageIOError::read_logs(&e),
                    });
                let id = bin_to_id(&id);

                assert_eq!(Ok(id), entry.as_ref().map(|e| e.log_id.index));
                (id, entry)
            })
            .take_while(|(id, _)| range.contains(id))
            .map(|x| x.1)
            .collect()
    }
}

impl RaftLogStorage<SessionStateTypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<SessionStateTypeConfig>> {
        let last = self
            .db
            .iterator_cf(self.logs(), rocksdb::IteratorMode::End)
            .next()
            .and_then(|res| {
                let (_, ent) = res.unwrap();
                Some(
                    serde_json::from_slice::<Entry<SessionStateTypeConfig>>(&ent)
                        .ok()?
                        .log_id,
                )
            });

        let last_purged_log_id = self.get_last_purged_()?;

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_committed(
        &mut self,
        _committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.set_committed_(&_committed)?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let c = self.get_committed_()?;
        Ok(c)
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.set_vote_(vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.get_vote_()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<SessionStateTypeConfig>,
    ) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<SessionStateTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        if entries.is_empty() {
            callback.log_io_completed(Ok(()));
            return Ok(());
        }

        // Get current log state to identify the safe boundary
        let log_state = self.get_log_state().await?;
        let last_persisted_index = log_state.last_log_id.map(|l| l.index).unwrap_or(0);

        for entry in &entries {
            // Safety Check: Payload Barrier
            // We must have the payload for:
            // 1. Any entry with index > last_persisted_index (New append)
            // 2. Any entry that OVERWRITES an existing entry (index <= last_persisted_index)
            //    unless it's exactly the same entry (same term).

            let mut need_payload_check = entry.log_id.index > last_persisted_index;

            if !need_payload_check {
                // Check if it's an overwrite with a different term
                let existing = self
                    .try_get_log_entries(entry.log_id.index..=entry.log_id.index)
                    .await?;
                if let Some(existing_entry) = existing.first() {
                    if existing_entry.log_id.leader_id.term != entry.log_id.leader_id.term {
                        need_payload_check = true;
                    }
                } else {
                    // Gap in log? Should not happen in standard Raft, but be safe.
                    need_payload_check = true;
                }
            }

            if need_payload_check {
                if let openraft::EntryPayload::Normal(req) = &entry.payload {
                    let key = match req {
                        types::SessionStateRequest::InflightRegisterRxPacket {
                            packet_key, ..
                        } => Some(packet_key),
                        types::SessionStateRequest::InflightRegisterTxPacket {
                            packet_key, ..
                        } => Some(packet_key),
                        types::SessionStateRequest::AppendToPendingQueue { packet_key, .. } => {
                            Some(packet_key)
                        }
                        _ => None,
                    };
                    if let Some(k) = key {
                        let mut found = false;
                        // Tiny local retry loop to handle concurrent writes to the same local store
                        for _i in 0..5 {
                            match self.payload_store.contains(k).await {
                                Ok(true) => {
                                    found = true;
                                    break;
                                }
                                _ => {
                                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                                }
                            }
                        }

                        if !found {
                            // Critical Safety Failure
                            let msg = format!(
                                "Payload missing for key: {} (Index: {}, Term: {})",
                                k, entry.log_id.index, entry.log_id.leader_id.term
                            );
                            // log::error!("{}", msg);
                            // Attempt to list keys or debug info?
                            // For now just log strictly
                            log::error!("Consistency Check Failed: {}", msg);

                            let err_cb =
                                std::io::Error::new(std::io::ErrorKind::NotFound, msg.clone());
                            let err_ret = std::io::Error::new(std::io::ErrorKind::NotFound, msg);

                            callback.log_io_completed(Err(err_cb));
                            return Err(StorageIOError::write_logs(&err_ret).into());
                        }
                    }
                }
            }

            let id = id_to_bin(entry.log_id.index);
            self.db
                .put_cf(
                    self.logs(),
                    id,
                    serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?,
                )
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }

        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        debug!("delete_log: [{:?}, +oo)", log_id);

        let from = id_to_bin(log_id.index);
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);
        self.db
            .delete_range_cf(self.logs(), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        debug!("delete_log: [0, {:?}]", log_id);

        self.set_last_purged_(log_id)?;
        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);
        self.db
            .delete_range_cf(self.logs(), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e))?;

        let gc_keys = {
            let mut queue = self.payload_gc_queue.write().await;
            let mut keep = Vec::with_capacity(queue.len());
            let mut delete_keys = Vec::new();

            for item in queue.drain(..) {
                if item.delete_after_log_index <= log_id.index {
                    delete_keys.push(item.key);
                } else {
                    keep.push(item);
                }
            }
            *queue = keep;
            delete_keys
        };

        for key in gc_keys {
            if let Err(e) = self.payload_store.delete(&key).await {
                warn!("payload gc delete failed for key {}: {}", key, e);
            }
        }

        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

pub(crate) async fn new_storage<P: AsRef<Path>>(
    db_path: P,
    topic_storage: Arc<RwLock<SessionStateStorage>>,
    payload_store: Arc<dyn PayloadStore>,
    settings: Arc<crate::settings::Settings>,
) -> (LogStore, StateMachineStore, Arc<AtomicBool>) {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let store = ColumnFamilyDescriptor::new("store", Options::default());
    let logs = ColumnFamilyDescriptor::new("logs", Options::default());

    let session_actor_map_db_path = db_path.as_ref().join("session_state");

    let db =
        DB::open_cf_descriptors(&db_opts, session_actor_map_db_path, vec![store, logs]).unwrap();
    let db = Arc::new(db);
    let payload_gc_queue = Arc::new(RwLock::new(Vec::new()));

    let log_store = LogStore {
        db: db.clone(),
        payload_store: payload_store.clone(),
        payload_gc_queue: payload_gc_queue.clone(),
    };
    let sm_store = StateMachineStore::new(
        db,
        topic_storage,
        payload_store,
        payload_gc_queue,
        settings,
    )
        .await
        .unwrap();
    let is_ready = sm_store.is_ready.clone();

    (log_store, sm_store, is_ready)
}
