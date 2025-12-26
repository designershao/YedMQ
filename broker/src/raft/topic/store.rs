use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use crate::raft::SnapshotData;
use crate::topic::topic_storage::TopicStorage;
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use log::debug;
use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::AnyError;
use openraft::Entry;
use openraft::EntryPayload;
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
use parking_lot::RwLock;

use super::types;
use super::types::{Response,Request};
use super::types::TypeConfig;
use crate::raft::{Node, NodeId};

type StorageResult<T> = Result<T, StorageError<NodeId>>;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,

    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotWrapper {
    pub topic_storage_snapshot: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct State {
    pub topic_storage: Arc<RwLock<TopicStorage>>,
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
            let snapshot_data = SnapshotWrapper {
                topic_storage_snapshot: self.data.state.topic_storage.read().to_snapshot(),
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
    async fn new(
        db: Arc<DB>,
        topic_storage: Arc<RwLock<TopicStorage>>,
    ) -> Result<StateMachineStore, StorageError<NodeId>> {
        let mut sm = Self {
            data: StateMachineData {
                last_applied_log_id: None,
                last_membership: Default::default(),
                state: State { topic_storage },
            },
            snapshot_idx: 0,
            db,
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

        println!("update state machin in topic state machine store");

        let mut topic_storage = self.data.state.topic_storage.write();

        *topic_storage = TopicStorage::from_snapshot(state.topic_storage_snapshot);

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
        Ok((
            self.data.last_applied_log_id,
            self.data.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = types::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);

        for ent in entries {
            self.data.last_applied_log_id = Some(ent.log_id);

            //let mut resp_value = None;

            match ent.payload {
                EntryPayload::Blank => {
                    replies.push(Response::None);
                },
                EntryPayload::Normal(req) => match req {
                    Request::SubscribeTopic {
                        tenant_id,
                        client_identifier,
                        topic,
                        qos,
                    } => {
                        let topic_storage = self.data.state.topic_storage.read();
                        if !topic_storage.contains_tenant(&tenant_id) {
                            topic_storage.create_tenant(&tenant_id);
                        }
                        let _ = topic_storage.subscribe(
                            tenant_id,
                            client_identifier,
                            topic.clone(),
                            qos,
                        );
                        replies.push(Response::None);
                    }
                    Request::UnsubscribeTopic {
                        tenant_id,
                        client_identifier,
                        topic,
                    } => {
                        let topic_storage = self.data.state.topic_storage.read();

                        let _ = topic_storage.unsubscribe(
                            &tenant_id,
                            &client_identifier,
                            &topic,
                        );
                        replies.push(Response::None);
                    }
                    Request::RegisterRetainPublishPacket {
                        tenant_id,
                        source_client_identifier,
                        publish_packet,
                    } => {
                        let topic_storage = self.data.state.topic_storage.read();

                        topic_storage.register_retain_publish_packet(
                            tenant_id,
                            source_client_identifier,
                            &publish_packet,
                        ).unwrap();
                        replies.push(Response::None);
                    },
                    Request::CleanRetainPublishPacket {
                        tenant_id,
                        topic_filter,
                    } => {
                        let topic_storage = self.data.state.topic_storage.read();
                        let _ = topic_storage.clean_retain_publish_packet(tenant_id, &topic_filter);
                        replies.push(Response::None);
                    },
                    Request::CreateTenant { tenant_id } => {
                        let topic_storage = self.data.state.topic_storage.read();
                        topic_storage.create_tenant(&tenant_id);
                        replies.push(Response::None);
                    }
                },
                EntryPayload::Membership(mem) => {
                    self.data.last_membership = StoredMembership::new(Some(ent.log_id), mem);
                    replies.push(Response::None);
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
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        self.update_state_machine_(new_snapshot.clone()).await?;

        self.set_current_snapshot_(new_snapshot)?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
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

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
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

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last = self
            .db
            .iterator_cf(self.logs(), rocksdb::IteratorMode::End)
            .next()
            .and_then(|res| {
                let (_, ent) = res.unwrap();
                Some(
                    serde_json::from_slice::<Entry<TypeConfig>>(&ent)
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

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);
            assert_eq!(bin_to_id(&id), entry.log_id.index);
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
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

pub(crate) async fn new_storage<P: AsRef<Path>>(
    db_path: P,
    topic_storage: Arc<RwLock<TopicStorage>>,
) -> (LogStore, StateMachineStore) {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let store = ColumnFamilyDescriptor::new("store", Options::default());
    let logs = ColumnFamilyDescriptor::new("logs", Options::default());

    let topic_db_path = db_path.as_ref().join("topic");

    let db = DB::open_cf_descriptors(&db_opts, topic_db_path, vec![store, logs]).unwrap();
    let db = Arc::new(db);

    let log_store = LogStore { db: db.clone() };
    let sm_store = StateMachineStore::new(db, topic_storage).await.unwrap();

    (log_store, sm_store)
}
