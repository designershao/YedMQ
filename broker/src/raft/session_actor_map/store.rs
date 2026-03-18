use std::{io, io::Cursor, ops::RangeBounds, path::Path, sync::Arc};

use actix::SystemService;
use log::debug;
use openraft::{
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
    AnyError, Entry, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader,
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
    Vote,
};
use parking_lot::RwLock;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Direction, Options, DB};
use serde::{Deserialize, Serialize};

use crate::{
    raft::{Node, NodeId},
    session::session_actor_map_storage::{
        SessionActorMapError, SessionActorMapStorage, SessionClock,
    },
};

use super::types::{self, SessionActorMapResponse, SessionActorMapTypeConfig};

type StorageResult<T> = Result<T, StorageError<NodeId>>;
type BoxedStorageError = Box<StorageError<NodeId>>;
type BoxedStorageResult<T> = Result<T, BoxedStorageError>;
type BoxedStorageIOError = Box<StorageIOError<NodeId>>;
type BoxedStorageIOResult<T> = Result<T, BoxedStorageIOError>;

pub type SnapshotData = Cursor<Vec<u8>>;

#[derive(Debug, Clone)]
pub struct State {
    pub session_actor_map: Arc<RwLock<SessionActorMapStorage>>,
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    pub last_applied_log_id: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, Node>,

    pub state: State,
}

#[derive(Debug, Clone)]
pub struct StateMachineStore {
    node_id: NodeId,

    pub data: StateMachineData,

    snapshot_idx: u64,

    session_clock: Arc<SessionClock>,

    session_ttl: u64,

    db: Arc<DB>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,

    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotWrapper {
    pub session_actor_map_snapshot: Vec<u8>,
}

impl RaftSnapshotBuilder<SessionActorMapTypeConfig> for StateMachineStore {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<SessionActorMapTypeConfig>, StorageError<NodeId>> {
        let last_applied_log = self.data.last_applied_log_id;
        let last_membership = self.data.last_membership.clone();

        let snapshot_json = {
            let snapshot_data = SnapshotWrapper {
                session_actor_map_snapshot: self
                    .data
                    .state
                    .session_actor_map
                    .read()
                    .to_snapshot()
                    .map_err(|e| StorageIOError::write_state_machine(&e))?,
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
        self.set_current_snapshot_(snapshot).map_err(|e| *e)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_json)),
        })
    }
}

impl StateMachineStore {
    async fn new(
        db: Arc<DB>,
        session_actor_map: Arc<RwLock<SessionActorMapStorage>>,
        node_id: NodeId,
        session_clock: Arc<SessionClock>,
        session_ttl: u64,
    ) -> Result<StateMachineStore, StorageError<NodeId>> {
        let mut sm = Self {
            data: StateMachineData {
                last_applied_log_id: None,
                last_membership: Default::default(),
                state: State { session_actor_map },
            },
            session_ttl,
            node_id,
            snapshot_idx: 0,
            db,
            session_clock,
        };

        let snapshot = sm.get_current_snapshot_().map_err(|e| *e)?;
        if let Some(snap) = snapshot {
            sm.update_state_machine_(snap).await?;
        }

        Ok(sm)
    }

    fn set_current_snapshot_(&self, snap: StoredSnapshot) -> BoxedStorageResult<()> {
        let snapshot_json = serde_json::to_vec(&snap).map_err(|e| {
            Box::new(StorageError::IO {
                source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
            })
        })?;

        self.db
            .put_cf(self.store()?, b"snapshot", snapshot_json)
            .map_err(|e| {
                Box::new(StorageError::IO {
                    source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
                })
            })?;
        self.flush(
            ErrorSubject::Snapshot(Some(snap.meta.signature())),
            ErrorVerb::Write,
        )
        .map_err(|e| Box::new(StorageError::IO { source: *e }))?;
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

        let mut session_actor_map_storage = self.data.state.session_actor_map.write();

        *session_actor_map_storage =
            SessionActorMapStorage::from_snapshot(state.session_actor_map_snapshot)
                .map_err(|e| StorageIOError::write_state_machine(&e))?;

        Ok(())
    }

    fn get_current_snapshot_(&self) -> BoxedStorageResult<Option<StoredSnapshot>> {
        let snapshot = self
            .db
            .get_cf(self.store()?, b"snapshot")
            .map_err(|e| {
                Box::new(StorageError::IO {
                    source: StorageIOError::read(&e),
                })
            })?;

        snapshot
            .map(|v| {
                serde_json::from_slice(&v).map_err(|e| {
                    Box::new(StorageError::IO {
                        source: StorageIOError::read_snapshot(None, &e),
                    })
                })
            })
            .transpose()
    }

    fn flush(&self, subject: ErrorSubject<NodeId>, verb: ErrorVerb) -> BoxedStorageIOResult<()> {
        self.db
            .flush_wal(true)
            .map_err(|e| Box::new(StorageIOError::new(subject, verb, AnyError::new(&e))))?;
        Ok(())
    }

    fn store(&self) -> BoxedStorageResult<&ColumnFamily> {
        self.db.cf_handle("store").ok_or_else(|| {
            Box::new(StorageError::IO {
                source: StorageIOError::read(&io::Error::other("column family not found: store")),
            })
        })
    }
}

impl RaftStateMachine<SessionActorMapTypeConfig> for StateMachineStore {
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
    ) -> Result<Vec<SessionActorMapResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = types::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);

        for ent in entries {
            self.data.last_applied_log_id = Some(ent.log_id);

            match ent.payload {
                openraft::EntryPayload::Blank => {
                    replies.push(SessionActorMapResponse::None);
                }
                openraft::EntryPayload::Normal(req) => match req {
                    types::SessionActorMapRequest::RegisterSession {
                        tenant_id,
                        session_id,
                        node_id,
                        version,
                    } => {
                        let register_result = {
                            let mut session_actor_map_storage =
                                self.data.state.session_actor_map.write();
                            session_actor_map_storage.register_session_actor(
                                tenant_id.clone(),
                                session_id.clone(),
                                node_id,
                                &version,
                                self.session_ttl,
                            )
                        };
                        match register_result {
                            Ok(()) => {
                                // Check local node , force stop the session if the session version is older
                                if node_id != self.node_id {
                                    let session_manager_actor_addr = crate::session::session_manager_actor::SessionManagerActor::from_registry();
                                    session_manager_actor_addr
                                        .send(crate::session::session_manager_actor::RemoveDuplicateSessionsByClock {
                                            tenant_id,
                                            client_id: session_id,
                                            session_version: version.clone(),
                                        })
                                        .await
                                        .map_err(|e| StorageError::IO {
                                            source: StorageIOError::write_state_machine(&e),
                                        })?;
                                }
                                //

                                // update current node session clock
                                self.session_clock.bump(&version);
                                self.session_clock.persist().await.map_err(|e| StorageError::IO {
                                    source: StorageIOError::write_state_machine(&e),
                                })?;
                                replies.push(SessionActorMapResponse::None)
                            }
                            Err(e) => match e {
                                SessionActorMapError::SessionVersionRejected {
                                    current_version,
                                    existing_version,
                                } => {
                                    replies.push(SessionActorMapResponse::Rejected {
                                        current_version,
                                        existing_version,
                                    });
                                }
                            },
                        }
                    }
                    types::SessionActorMapRequest::UnregisterSession {
                        tenant_id,
                        session_id,
                        session_version,
                    } => {
                        {
                            let mut session_actor_map_storage =
                                self.data.state.session_actor_map.write();
                            session_actor_map_storage.unregister_session_actor(
                                tenant_id.clone(),
                                session_id.clone(),
                                &session_version,
                            );
                            self.session_clock.bump(&session_version);
                        }
                        self.session_clock.persist().await.map_err(|e| StorageError::IO {
                            source: StorageIOError::write_state_machine(&e),
                        })?;
                        replies.push(SessionActorMapResponse::None);
                    }
                    types::SessionActorMapRequest::CleanExpiredSessions { sessions } => {
                        let mut session_actor_map_storage =
                            self.data.state.session_actor_map.write();
                        for session in sessions {
                            session_actor_map_storage.unregister_session_actor(
                                session.tenant_id.clone(),
                                session.session_id.clone(),
                                &session.session_version,
                            );
                            // Force stop the session if the session is expired
                            if session.node_id == self.node_id {
                                let session_manager_actor_addr = crate::session::session_manager_actor::SessionManagerActor::from_registry();
                                session_manager_actor_addr.do_send(
                                    crate::session::session_manager_actor::RemoveExpiredSession {
                                        tenant_id: session.tenant_id,
                                        client_id: session.session_id,
                                    },
                                );
                            }
                        }
                        replies.push(SessionActorMapResponse::None);
                    }
                    types::SessionActorMapRequest::SessionLeaseRenewRequest { sessions } => {
                        let mut session_actor_map_storage =
                            self.data.state.session_actor_map.write();
                        session_actor_map_storage.session_lease_renew(sessions, self.session_ttl);
                        replies.push(SessionActorMapResponse::None);
                    }
                },
                openraft::EntryPayload::Membership(membership) => {
                    self.data.last_membership = StoredMembership::new(Some(ent.log_id), membership);
                    replies.push(SessionActorMapResponse::None);
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

        self.set_current_snapshot_(new_snapshot).map_err(|e| *e)?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<SessionActorMapTypeConfig>>, StorageError<NodeId>> {
        let x = self.get_current_snapshot_().map_err(|e| *e)?;
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
    id.to_be_bytes().to_vec()
}

fn bin_to_id(buf: &[u8]) -> StorageResult<u64> {
    let bytes: [u8; 8] = buf.try_into().map_err(|_| StorageError::IO {
        source: StorageIOError::read_logs(&io::Error::other("invalid log key length")),
    })?;
    Ok(u64::from_be_bytes(bytes))
}

impl LogStore {
    fn store_io(&self, subject: ErrorSubject<NodeId>, verb: ErrorVerb) -> BoxedStorageIOResult<&ColumnFamily> {
        self.db.cf_handle("store").ok_or_else(|| {
            Box::new(StorageIOError::new(
                subject,
                verb,
                AnyError::new(&io::Error::other("column family not found: store")),
            ))
        })
    }

    fn store(&self) -> BoxedStorageResult<&ColumnFamily> {
        self.db.cf_handle("store").ok_or_else(|| {
            Box::new(StorageError::IO {
                source: StorageIOError::read(&io::Error::other("column family not found: store")),
            })
        })
    }

    fn logs(&self) -> BoxedStorageResult<&ColumnFamily> {
        self.db.cf_handle("logs").ok_or_else(|| {
            Box::new(StorageError::IO {
                source: StorageIOError::read(&io::Error::other("column family not found: logs")),
            })
        })
    }

    fn flush(&self, subject: ErrorSubject<NodeId>, verb: ErrorVerb) -> BoxedStorageIOResult<()> {
        self.db
            .flush_wal(true)
            .map_err(|e| Box::new(StorageIOError::new(subject, verb, AnyError::new(&e))))?;
        Ok(())
    }

    fn get_last_purged_(&self) -> BoxedStorageResult<Option<LogId<u64>>> {
        let last_purged = self
            .db
            .get_cf(self.store()?, b"last_purged_log_id")
            .map_err(|e| {
                Box::new(StorageError::IO {
                    source: StorageIOError::read(&e),
                })
            })?;

        last_purged
            .map(|v| {
                serde_json::from_slice(&v).map_err(|e| {
                    Box::new(StorageError::IO {
                        source: StorageIOError::read_logs(&e),
                    })
                })
            })
            .transpose()
    }

    fn set_last_purged_(&self, log_id: LogId<u64>) -> BoxedStorageResult<()> {
        let payload = serde_json::to_vec(&log_id).map_err(|e| {
            Box::new(StorageError::IO {
                source: StorageIOError::write(&e),
            })
        })?;

        self.db
            .put_cf(self.store()?, b"last_purged_log_id", payload)
            .map_err(|e| {
                Box::new(StorageError::IO {
                    source: StorageIOError::write(&e),
                })
            })?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)
            .map_err(|e| Box::new(StorageError::IO { source: *e }))?;
        Ok(())
    }

    fn set_committed_(&self, committed: &Option<LogId<NodeId>>) -> BoxedStorageIOResult<()> {
        let json = serde_json::to_vec(committed)
            .map_err(|e| Box::new(StorageIOError::write(&e)))?;

        self.db
            .put_cf(self.store_io(ErrorSubject::Store, ErrorVerb::Write)?, b"committed", json)
            .map_err(|e| Box::new(StorageIOError::write(&e)))?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;
        Ok(())
    }

    fn get_committed_(&self) -> BoxedStorageResult<Option<LogId<NodeId>>> {
        let committed = self
            .db
            .get_cf(self.store()?, b"committed")
            .map_err(|e| {
                Box::new(StorageError::IO {
                    source: StorageIOError::read(&e),
                })
            })?;

        committed
            .map(|v| {
                serde_json::from_slice(&v).map_err(|e| {
                    Box::new(StorageError::IO {
                        source: StorageIOError::read(&e),
                    })
                })
            })
            .transpose()
    }

    fn set_vote_(&self, vote: &Vote<NodeId>) -> BoxedStorageResult<()> {
        let payload = serde_json::to_vec(vote).map_err(|e| {
            Box::new(StorageError::IO {
                source: StorageIOError::write_vote(&e),
            })
        })?;

        self.db
            .put_cf(self.store()?, b"vote", payload)
            .map_err(|e| {
                Box::new(StorageError::IO {
                    source: StorageIOError::write_vote(&e),
                })
            })?;

        self.flush(ErrorSubject::Vote, ErrorVerb::Write)
            .map_err(|e| Box::new(StorageError::IO { source: *e }))?;
        Ok(())
    }

    fn get_vote_(&self) -> BoxedStorageResult<Option<Vote<NodeId>>> {
        let vote = self
            .db
            .get_cf(self.store()?, b"vote")
            .map_err(|e| {
                Box::new(StorageError::IO {
                    source: StorageIOError::write_vote(&e),
                })
            })?;

        vote.map(|v| {
            serde_json::from_slice(&v).map_err(|e| {
                Box::new(StorageError::IO {
                    source: StorageIOError::read_vote(&e),
                })
            })
        })
        .transpose()
    }
}

impl RaftLogReader<SessionActorMapTypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + core::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<SessionActorMapTypeConfig>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };
        let logs_cf = self.logs().map_err(|e| *e)?;

        self.db
            .iterator_cf(
                logs_cf,
                rocksdb::IteratorMode::From(&start, Direction::Forward),
            )
            .map(|res| {
                let (id, val) = res.map_err(|e| StorageError::IO {
                    source: StorageIOError::read_logs(&e),
                })?;
                let entry: StorageResult<Entry<_>> =
                    serde_json::from_slice(&val).map_err(|e| StorageError::IO {
                        source: StorageIOError::read_logs(&e),
                    });
                let id = bin_to_id(&id)?;

                assert_eq!(Ok(id), entry.as_ref().map(|e| e.log_id.index));
                Ok((id, entry))
            })
            .take_while(|item| match item {
                Ok((id, _)) => range.contains(id),
                Err(_) => true,
            })
            .map(|x| x.and_then(|(_, entry)| entry))
            .collect()
    }
}

impl RaftLogStorage<SessionActorMapTypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<SessionActorMapTypeConfig>> {
        let logs_cf = self.logs().map_err(|e| *e)?;
        let last = self
            .db
            .iterator_cf(logs_cf, rocksdb::IteratorMode::End)
            .next()
            .transpose()
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(&e),
            })?
            .map(|(_, ent)| {
                serde_json::from_slice::<Entry<SessionActorMapTypeConfig>>(&ent).map_err(|e| {
                    StorageError::IO {
                        source: StorageIOError::read_logs(&e),
                    }
                })
            })
            .transpose()?
            .map(|entry| entry.log_id);

        let last_purged_log_id = self.get_last_purged_().map_err(|e| *e)?;

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
        self.set_committed_(&_committed)
            .map_err(|e| StorageError::IO { source: *e })?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let c = self.get_committed_().map_err(|e| *e)?;
        Ok(c)
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.set_vote_(vote).map_err(|e| *e)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.get_vote_().map_err(|e| *e)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<SessionActorMapTypeConfig>,
    ) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<SessionActorMapTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);
            assert_eq!(bin_to_id(&id)?, entry.log_id.index);
            self.db
                .put_cf(
                    self.logs().map_err(|e| *e)?,
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
            .delete_range_cf(self.logs().map_err(|e| *e)?, &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        debug!("delete_log: [0, {:?}]", log_id);

        self.set_last_purged_(log_id).map_err(|e| *e)?;
        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);
        self.db
            .delete_range_cf(self.logs().map_err(|e| *e)?, &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

pub(crate) async fn new_storage<P: AsRef<Path>>(
    db_path: P,
    topic_storage: Arc<RwLock<SessionActorMapStorage>>,
    current_node_id: NodeId,
    session_clock: Arc<SessionClock>,
    session_ttl: u64,
) -> StorageResult<(LogStore, StateMachineStore)> {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let store = ColumnFamilyDescriptor::new("store", Options::default());
    let logs = ColumnFamilyDescriptor::new("logs", Options::default());

    let session_actor_map_db_path = db_path.as_ref().join("session_actor_map");

    let db = DB::open_cf_descriptors(&db_opts, session_actor_map_db_path, vec![store, logs])
        .map_err(|e| StorageError::IO {
            source: StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)),
        })?;
    let db = Arc::new(db);

    let log_store = LogStore { db: db.clone() };
    let sm_store = StateMachineStore::new(
        db,
        topic_storage,
        current_node_id,
        session_clock,
        session_ttl,
    )
    .await?;

    Ok((log_store, sm_store))
}
