use std::{fmt, sync::Arc};

use log::info;
use tokio::sync::{mpsc::Sender, watch, Mutex, RwLock};

use crate::{
    protobuf::raft_service_server::RaftServiceServer, router::RouterCmd,
    session::session_actor_map_storage::SessionActorMapStorage, settings::Cluster,
    topic::topic_storage::TopicStorage,
};

use super::service::raft_service::RaftServiceImpl;

#[derive(Debug)]
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

pub struct RaftManager {
    pub topic_raft: crate::raft::topic::RaftManager,

    pub session_actor_map_raft: crate::raft::session_actor_map::SessionActorMapRaftManager,

    join_handles: Mutex<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>>,

    cluster_cfg: Cluster,

    running_rx: watch::Receiver<()>,

    running_tx: watch::Sender<()>,
}

impl Drop for RaftManager {
    fn drop(&mut self) {
        println!("Raft drop: id={}", self.cluster_cfg.node_id);
    }
}

impl RaftManager {
    pub async fn new(
        cluster_cfg: Cluster,
        topic_storage: Arc<RwLock<TopicStorage>>,
        session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
    ) -> Self {
        let (tx, rx) = watch::channel::<()>(());

        let topic_raft_manager =
            crate::raft::topic::RaftManager::new(cluster_cfg.clone(), topic_storage.clone()).await;

        let session_actor_map_raft_manager =
            crate::raft::session_actor_map::SessionActorMapRaftManager::new(
                cluster_cfg.clone(),
                session_actor_map_storage.clone(),
            )
            .await;

        let manager = RaftManager {
            topic_raft: topic_raft_manager,
            session_actor_map_raft: session_actor_map_raft_manager,
            running_rx: rx,
            running_tx: tx,
            join_handles: Mutex::new(vec![]),
            cluster_cfg,
        };

        manager
    }
    pub async fn start_grpc(
        raft_manager: Arc<RaftManager>,
        router_sender: Sender<RouterCmd>,
    ) -> anyhow::Result<()> {
        let mut rx = raft_manager.running_rx.clone();

        let raft_service = RaftServiceImpl {
            raft_manager: raft_manager.clone(),
            router_sender,
        };

        let addr_str = raft_manager.cluster_cfg.rpc.external.to_string();
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

        let node_id = raft_manager.cluster_cfg.node_id;

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
