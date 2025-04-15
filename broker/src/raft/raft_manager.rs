use std::{fmt, sync::Arc};

use actix::Recipient;
use log::info;
use mockall::automock;
use thiserror::Error;
use tokio::sync::{mpsc::Sender, watch, Mutex, OnceCell, RwLock};

use crate::{
    protobuf::raft_service_server::RaftServiceServer, router::RouterCmd,
    session::{session_actor_map_storage::{SessionActorMapStorage, SessionClock}, session_state_storage::SessionStateStorage}, settings::{Cluster, Settings},
    topic::topic_storage::TopicStorage,
};

use super::service::raft_service::RaftServiceImpl;

#[derive(Debug, Error)]
pub enum RaftManagerError {
    NodeUnavailable(String),
    ElectionFailure(String),
    LogSyncError(String),
    TimeoutError(String),
    NetworkError(String),
    InternalError(String),
    Unknown(String),
}

impl fmt::Display for RaftManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            RaftManagerError::NodeUnavailable(ref msg) => write!(f, "Node Unavailable: {}", msg),
            RaftManagerError::ElectionFailure(ref msg) => write!(f, "Election Failure: {}", msg),
            RaftManagerError::LogSyncError(ref msg) => write!(f, "Log Sync Error: {}", msg),
            RaftManagerError::TimeoutError(ref msg) => write!(f, "Timeout Error: {}", msg),
            RaftManagerError::NetworkError(ref msg) => write!(f, "Network Error: {}", msg),
            RaftManagerError::InternalError(ref msg) => write!(f, "Internal Error: {}", msg),
            RaftManagerError::Unknown(ref msg) => write!(f, "Unknown Error: {}", msg),
        }
    }
}

#[automock]
pub trait RaftManagerTrait {

    fn session_actor_map_raft(&self) -> &dyn crate::raft::session_actor_map::SessionActorMapRaftManagerTrait;

    fn topic_raft(&self) -> &dyn crate::raft::topic::TopicRaftManagerTrait;

    fn session_state_raft(&self) -> &dyn crate::raft::session_state::SessionStateRaftManagerTrait;

}

impl RaftManagerTrait for RaftManager {
    fn session_actor_map_raft(&self) -> &dyn crate::raft::session_actor_map::SessionActorMapRaftManagerTrait {
        self.session_actor_map_raft.get().unwrap()
    }

    fn topic_raft(&self) -> &dyn crate::raft::topic::TopicRaftManagerTrait {
        self.topic_raft.get().unwrap()
    }

    fn session_state_raft(&self) -> &dyn crate::raft::session_state::SessionStateRaftManagerTrait {
        self.session_state_raft.get().unwrap()
    }
}

pub struct RaftManager {
    topic_raft: OnceCell<crate::raft::topic::RaftManager>,

    session_actor_map_raft: OnceCell<crate::raft::session_actor_map::SessionActorMapRaftManager>,

    session_state_raft: OnceCell<crate::raft::session_state::SessionStateRaftManager>,

    join_handles: Mutex<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>>,

    settings: Arc<Settings>,

    running_rx: watch::Receiver<()>,

    running_tx: watch::Sender<()>,
}

impl Drop for RaftManager {
    fn drop(&mut self) {
        println!("Raft drop: id={}", self.settings.cluster.node_id);
    }
}

impl RaftManager {

    pub fn topic_raft(&self) -> &crate::raft::topic::RaftManager {
        self.topic_raft.get().unwrap()
    }

    pub fn session_actor_map_raft(&self) -> &dyn crate::raft::session_actor_map::SessionActorMapRaftManagerTrait {
        self.session_actor_map_raft.get().unwrap()
    }

    pub fn session_state_raft(&self) -> &crate::raft::session_state::SessionStateRaftManager {
        self.session_state_raft.get().unwrap()
    }

    pub async fn init_topic_raft(&self, topic_storage: Arc<RwLock<TopicStorage>>) {

        let topic_raft_manager =
            crate::raft::topic::RaftManager::new(self.settings.clone(), topic_storage.clone()).await;
        let _ = self.topic_raft.set(topic_raft_manager);
    }

    pub async fn init_session_state_raft(&self, session_state_storage: Arc<RwLock<SessionStateStorage>>) {
        let session_state_raft_manager = 
            crate::raft::session_state::SessionStateRaftManager::new(
                self.settings.clone(),
                session_state_storage.clone(),
            )
            .await;
        let _ = self.session_state_raft.set(session_state_raft_manager);
    }

    pub async fn init_session_actor_map_raft(&self,
        session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
         session_manager_actor_recipient: Recipient<crate::session::session_manager_actor::ForceStop>,
         session_clock: Arc<SessionClock>,
        ) {

        let session_actor_map_raft_manager =
            crate::raft::session_actor_map::SessionActorMapRaftManager::new(
                self.settings.clone(),
                session_actor_map_storage.clone(),
                session_manager_actor_recipient,
                session_clock
            )
            .await;
        let _ = self.session_actor_map_raft.set(session_actor_map_raft_manager);
    }

    pub async fn new(
        settings: Arc<Settings>,
    ) -> Self {
        let (tx, rx) = watch::channel::<()>(());

        let manager = RaftManager {
            topic_raft: OnceCell::new(),
            session_actor_map_raft: OnceCell::new(),
            session_state_raft: OnceCell::new(),
            running_rx: rx,
            running_tx: tx,
            join_handles: Mutex::new(vec![]),
            settings,
        };

        manager
    }
    pub async fn start_grpc(
        raft_manager: Arc<RaftManager>,
        router_sender: Sender<RouterCmd>,
        session_manager_actor_force_disconnect_recipient: Recipient<crate::session::session_manager_actor::ForceDisconnect>,
        session_manager_actor_force_stop_recipient: Recipient<crate::session::session_manager_actor::ForceStop>
    ) -> anyhow::Result<()> {
        let mut rx = raft_manager.running_rx.clone();

        let raft_service = RaftServiceImpl {
            raft_manager: raft_manager.clone(),
            router_sender,
            session_manager_actor_force_disconnect_recipient,
            session_manager_actor_force_stop_recipient
        };

        let addr_str = raft_manager.settings.cluster.rpc.external.to_string();
        let ret = addr_str.parse::<std::net::SocketAddr>();

        let addr = match ret {
            Ok(addr) => addr,
            Err(e) => {
                return Err(anyhow::anyhow!("parse address error: {}", e));
            }
        };

        let svc = RaftServiceServer::new(raft_service);
        let srv = tonic::transport::server::Server::builder().add_service(svc);

        info!("about to start raft grpc on resolved addr {}", addr);

        let node_id = raft_manager.settings.cluster.node_id;

        let h = actix::spawn(async move {
            srv.serve_with_shutdown(addr, async move {
                let _ = rx.changed().await;
                info!("signal receivbed, shutting down: id={} {}", addr, node_id);
            })
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
            Ok::<(), anyhow::Error>(())
        });

        let mut jh = raft_manager.join_handles.lock().await;

        jh.push(h);

        Ok(())
    }
}
