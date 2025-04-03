use std::collections::BTreeMap;
use std::path::Path;
use std::{collections::HashMap, fmt, sync::Arc};

use actix::Recipient;
use log::{info, warn};
use mockall::automock;
use openraft::Config;
use raft_network_impl::Network;
use store::new_storage;
use tokio::sync::{watch, Mutex, RwLock};
use types::{SessionActorMapRequest, SessionActorMapTypeConfig};

use crate::protobuf::raft_service_client::RaftServiceClient;
use crate::protobuf::{AppendEntriesRequest, RaftType};
use crate::{session::session_actor_map_storage::SessionActorMapStorage, settings::Cluster};

use super::raft_manager::RaftManagerError;
use super::Node;
use super::NodeId;

pub mod raft_network_impl;
pub mod store;
pub mod types;

pub type SessionActorMapRaft = openraft::Raft<SessionActorMapTypeConfig>;

#[async_trait::async_trait]
#[automock]
pub trait SessionActorMapRaftManagerTrait {
    async fn register_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
    ) -> Result<(), RaftManagerError>;

    async fn unregister_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
        keep_alive: bool,
    ) -> Result<(), RaftManagerError>;

    fn current_node_id(&self) -> NodeId;

    async fn get_session_actor_map_node_id(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Option<NodeId>;

    async fn get_node_by_id(&self, id: NodeId) -> Option<Node>;

    fn raft(&self) -> &SessionActorMapRaft;

    async fn init_cluster(&self) -> Result<(), RaftManagerError>;
}

#[async_trait::async_trait]
impl SessionActorMapRaftManagerTrait for SessionActorMapRaftManager {
    async fn init_cluster(&self) -> Result<(), RaftManagerError> {
        let mut cluster_nodes = BTreeMap::new();
        cluster_nodes.insert(
            self.cluster_cfg.node_id,
            Node {
                rpc_addr: self.cluster_cfg.rpc.external.to_string(),
                api_addr: self.cluster_cfg.rpc.external.to_string(),
            },
        );

        self.raft.initialize(cluster_nodes).await.map_err(|e| {
            RaftManagerError::InternalError(format!("Failed to initialize cluster, {:?}", e))
        })
    }

    fn raft(&self) -> &SessionActorMapRaft {
        &self.raft
    }

    fn current_node_id(&self) -> NodeId {
        self.cluster_cfg.node_id
    }

    async fn register_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
    ) -> Result<(), RaftManagerError> {
        self.execute_command(types::SessionActorMapRequest::RegisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            node_id,
        })
        .await
    }

    async fn unregister_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
        keep_alive: bool,
    ) -> Result<(), RaftManagerError> {
        self.execute_command(types::SessionActorMapRequest::UnregisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            node_id,
            keep_alive,
        })
        .await
    }

    async fn get_node_by_id(&self, id: NodeId) -> Option<Node> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .find(|x| *x.0 == id)
            .and_then(|x| Some(x.1.clone()))
    }

    async fn get_session_actor_map_node_id(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Option<NodeId> {
        if !self.is_leader().await {
            let leader_node = self.get_leader();

            if leader_node.is_none() {
                warn!("GetSessionActorMap failed, leader not found");
                return None;
            } else {
                let leader_node = leader_node.unwrap();

                let addr = format!("http://{}", leader_node.rpc_addr);

                let mut client = RaftServiceClient::connect(addr.clone()).await.unwrap();

                let actor_map_response = client
                    .get_session_actor_map(crate::protobuf::GetSessionActorMapRequest {
                        tenant_id: tenant_id.to_string(),
                        client_id: client_id.to_string(),
                    })
                    .await;

                if actor_map_response.is_err() {
                    warn!("GetSessionActorMap failed, res={:?}", actor_map_response);
                    return None;
                } else {
                    return actor_map_response.unwrap().into_inner().node_id;
                }
            }
        } else {
            let session_map_storage_guard = self.session_actor_map_storage.read().await;
            session_map_storage_guard.get_session_actor_map(tenant_id, client_id)
        }
    }
}

pub struct SessionActorMapRaftManager {
    session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,

    pub raft: SessionActorMapRaft,

    current_leader: Arc<RwLock<Option<NodeId>>>,

    nodes: Arc<RwLock<HashMap<NodeId, Node>>>,

    join_handles: Mutex<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>>,

    cluster_cfg: Cluster,

    running_rx: watch::Receiver<()>,

    running_tx: watch::Sender<()>,
}

impl SessionActorMapRaftManager {
    pub async fn session_exist_in_current_node(&self, tenant_id: &str, client_id: &str) -> bool {
        let session_map_storage_guard = self.session_actor_map_storage.read().await;
        let node_id_option = session_map_storage_guard.get_session_actor_map(tenant_id, client_id);
        if let Some(node_id) = node_id_option {
            node_id == self.current_node_id()
        } else {
            false
        }
    }

    pub async fn register_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
    ) -> Result<(), RaftManagerError> {
        let request = SessionActorMapRequest::RegisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            node_id,
        };

        self.execute_command(request).await
    }

    pub async fn unregister_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
        keep_alive: bool,
    ) -> Result<(), RaftManagerError> {
        let request = SessionActorMapRequest::UnregisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            node_id,
            keep_alive,
        };

        self.execute_command(request).await
    }

    pub fn current_node_id(&self) -> NodeId {
        self.cluster_cfg.node_id
    }

    pub async fn stop(&self) -> Result<(), RaftManagerError> {
        let mut rx = self.raft.metrics();

        self.raft.shutdown().await.map_err(|e| {
            RaftManagerError::InternalError(format!("Failed to shutdown raft, {}", e))
        })?;

        if let Err(e) = self.running_tx.send(()) {
            return Err(RaftManagerError::InternalError(format!(
                "Failed to shutdown raft, {}",
                e
            )));
        }

        loop {
            let r = rx.changed().await;
            if r.is_err() {
                break;
            }
        }

        for j in self.join_handles.lock().await.iter_mut() {
            let _rst = j
                .await
                .map_err(|e| RaftManagerError::InternalError(format!("{}", e)))?;
        }

        info!("Raft shutdown: id={}", self.cluster_cfg.node_id);
        Ok(())
    }

    pub async fn new(
        cluster_cfg: Cluster,
        session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
        session_manager_force_stop_recipient: Recipient<
            crate::session::session_manager_actor::ForceStop,
        >,
    ) -> Self {
        let raft_config = Self::get_raft_config(cluster_cfg.heartbeat_interval.into()).await;

        let dir = Path::new(&cluster_cfg.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let (log_store, state_machine_store) = new_storage(
            &dir,
            session_actor_map_storage.clone(),
            cluster_cfg.node_id,
            session_manager_force_stop_recipient,
        )
        .await;

        let network = Network {};

        let raft = openraft::Raft::new(
            cluster_cfg.node_id,
            config.clone(),
            network,
            log_store,
            state_machine_store,
        )
        .await
        .unwrap();
        let (tx, rx) = watch::channel::<()>(());

        let manager = SessionActorMapRaftManager {
            raft,
            current_leader: Arc::new(RwLock::new(None)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            join_handles: Mutex::new(vec![]),
            running_rx: rx,
            running_tx: tx,
            cluster_cfg,
            session_actor_map_storage: session_actor_map_storage.clone(),
        };

        manager.start_monitor_raft_metrics();
        manager
    }

    fn start_monitor_raft_metrics(&self) {
        let leader_state = self.current_leader.clone();
        let mut rx = self.raft.metrics();

        actix::spawn(async move {
            loop {
                let _ = rx.changed().await;
                let mut state = leader_state.write().await;
                *state = rx.borrow().current_leader;
            }
        });
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().state == openraft::ServerState::Leader
    }

    pub fn get_leader_node_id(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    pub fn get_leader(&self) -> Option<Node> {
        self.get_leader_node_id().and_then(|id| {
            self.raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .find(|x| *x.0 == id)
                .and_then(|x| Some(x.1.clone()))
        })
    }

    pub async fn execute_command(
        &self,
        command: SessionActorMapRequest,
    ) -> Result<(), RaftManagerError> {
        if !self.is_leader().await {
            let leader_node = self.get_leader();

            if leader_node.is_none() {
                return Err(RaftManagerError::InternalError(
                    "No leader available".into(),
                ));
            } else {
                let leader_node = leader_node.unwrap();

                let addr = format!("http://{}", leader_node.rpc_addr);

                let mut client = RaftServiceClient::connect(addr.clone()).await.unwrap();

                let append_request = AppendEntriesRequest {
                    data: serde_json::to_string(&command).unwrap(),
                    raft_type: RaftType::SessionActorMap.into(),
                };

                let res = client.append_entries(append_request).await;
                if res.is_err() {
                    warn!("AppendEntries failed, res={:?}", res);
                    return Err(RaftManagerError::InternalError(
                        "AppendEntries failed".into(),
                    ));
                }
            }
        } else {
            // current node is leader
            let res = self.raft.client_write(command).await;
            if res.is_err() {
                return Err(RaftManagerError::InternalError(format!(
                    "ClientWrite failed: {:?}",
                    res
                )));
            }
        }
        Ok(())
    }

    async fn get_raft_config(heartbeat_interval: u64) -> Config {
        let election_timeout_min = heartbeat_interval * 1000 * 8;
        let election_timeout_max = heartbeat_interval * 1000 * 12;
        let heartbeat_interval = heartbeat_interval * 1000;

        Config {
            heartbeat_interval,
            election_timeout_min,
            election_timeout_max,
            ..Default::default()
        }
    }
}
