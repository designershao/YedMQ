use std::collections::BTreeMap;
use std::path::Path;
use std::{collections::HashMap, sync::Arc};

use actix::Recipient;
use log::{info, warn};
use mockall::automock;
use openraft::Config;
use raft_network_impl::Network;
use store::new_storage;
use tokio::sync::{watch, Mutex, RwLock};
use tonic::transport::Channel;
use types::{SessionActorMapRequest, SessionActorMapResponse, SessionActorMapTypeConfig};

use crate::protobuf::raft_service_client::RaftServiceClient;
use crate::protobuf::{AppendEntriesRequest, RaftType};
use crate::session::session_actor_map_storage::{SessionActorMapEntry, SessionClock, SessionVersion};
use crate::settings::Settings;
use crate::session::session_actor_map_storage::SessionActorMapStorage;

use super::raft_manager::RaftManagerError;
use super::NodeId;
use super::{Node, RaftCommandExecutor};

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
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftManagerError>;

    async fn unregister_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftManagerError>;

    fn current_node_id(&self) -> NodeId;

    async fn get_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Option<SessionActorMapEntry>;

    async fn get_node_by_id(&self, id: NodeId) -> Option<Node>;

    fn raft(&self) -> &SessionActorMapRaft;

    async fn init_cluster(&self) -> Result<(), RaftManagerError>;

    async fn get_leader_node_id(&self) -> Option<NodeId>; 

    async fn get_leader(&self) -> Option<Node>;
    
}

#[async_trait::async_trait]
impl SessionActorMapRaftManagerTrait for SessionActorMapRaftManager {

    async fn get_leader_node_id(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    async fn get_leader(&self) -> Option<Node> {
        self.get_leader_node_id().await.and_then(|id| {
            self.raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .find(|x| *x.0 == id)
                .and_then(|x| Some(x.1.clone()))
        })
    }


    async fn init_cluster(&self) -> Result<(), RaftManagerError> {
        let mut cluster_nodes = BTreeMap::new();
        cluster_nodes.insert(
            self.settings.cluster.node_id,
            Node {
                rpc_addr: self.settings.cluster.rpc.external.to_string(),
                api_addr: self.settings.listener.api.external.to_string(),
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
        self.settings.cluster.node_id
    }

    async fn register_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftManagerError> {
        self.execute_command(types::SessionActorMapRequest::RegisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            node_id,
            version,
        })
        .await
    }

    async fn unregister_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftManagerError> {
        self.execute_command(types::SessionActorMapRequest::UnregisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            session_version:version,
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

    async fn get_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Option<SessionActorMapEntry> {
        if !self.is_leader().await {
            let leader_node = self.get_leader().await;

            if leader_node.is_none() {
                warn!("GetSessionActorMap failed, leader not found");
                return None;
            } else {
                let leader_node = leader_node.unwrap();

                let addr = format!("http://{}", leader_node.rpc_addr);

                let client = super::create_rpc_client_with_retry(addr.clone()).await;

                if client.is_err() {
                    warn!("Create rpc client failed, res={:?}", client);
                    return None;
                }

                let mut client = client.unwrap();

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
                    let response = actor_map_response.unwrap().into_inner();

                    if response.success {
                        if response.node_id.is_none() {
                            return None;
                        }
                        let node_id = response.node_id.unwrap();
                        let version = response.version.unwrap();
                        Some(SessionActorMapEntry {
                            node_id,
                            version: SessionVersion {
                                counter: version.counter,
                                node_id: version.node_id,
                            },
                        })
                    } else {
                        return None;
                    }
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

    settings: Arc<Settings>,

    running_rx: watch::Receiver<()>,

    running_tx: watch::Sender<()>,
}

#[async_trait::async_trait]
impl RaftCommandExecutor<SessionActorMapRequest, SessionActorMapResponse>
    for SessionActorMapRaftManager
{
    // Check if the current node is the leader
    async fn is_leader(&self) -> bool {
        let leader_node_id = self.get_leader_node_id().await;
        if leader_node_id.is_none() {
            return false;
        } else {
            let leader_node_id = leader_node_id.unwrap();
            let current_node_id = self.current_node_id();
            leader_node_id == current_node_id
        }
    }

    // Get the current leader node information
    async fn get_leader(&self) -> Option<Node> {
        self.get_leader_node_id().await.and_then(|id| {
            self.raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .find(|x| *x.0 == id)
                .and_then(|x| Some(x.1.clone()))
        })
    }

    // Execute command as leader
    async fn execute_as_leader(
        &self,
        command: SessionActorMapRequest,
    ) -> Result<SessionActorMapResponse, super::raft_manager::RaftManagerError> {
        let res = self.raft.client_write(command).await;
        if let Err(e) = res {
            return Err(super::raft_manager::RaftManagerError::InternalError(
                format!("ClientWrite failed: {:?}", e),
            ));
        }

        let r = res.unwrap();
        let res = r.data;
        Ok(res)
    }

    // Create an RPC client for communication with a target node
    async fn create_rpc_client(
        &self,
        addr: String,
    ) -> Result<RaftServiceClient<Channel>, super::raft_manager::RaftManagerError> {
        match super::create_rpc_client_with_retry(addr).await {
            Ok(client) => Ok(client),
            Err(_) => Err(super::raft_manager::RaftManagerError::InternalError(
                "Create rpc client failed".into(),
            )),
        }
    }

    // Send command to another node
    async fn send_command_to_node(
        &self,
        mut client: RaftServiceClient<Channel>,
        command: SessionActorMapRequest,
    ) -> Result<SessionActorMapResponse, super::raft_manager::RaftManagerError> {
        let append_request = AppendEntriesRequest {
            data: serde_json::to_string(&command).unwrap(),
            raft_type: RaftType::SessionActorMap.into(),
        };

        match client.append_entries(append_request).await {
            Ok(rpc_response) => {
                let response = rpc_response.into_inner();
                let data = serde_json::from_str(&response.data).unwrap();
                Ok(data)
            }
            Err(e) => {
                return Err(super::raft_manager::RaftManagerError::InternalError(
                    format!("AppendEntries failed: {:?}", e),
                ));
            }
        }
    }
}

impl SessionActorMapRaftManager {
    pub async fn session_exist_in_current_node(&self, tenant_id: &str, client_id: &str) -> bool {
        let session_map_storage_guard = self.session_actor_map_storage.read().await;
        let node_id_option = session_map_storage_guard.get_session_actor_map(tenant_id, client_id);
        if let Some(session_actor_map_entry) = node_id_option {
            session_actor_map_entry.node_id == self.current_node_id()
        } else {
            false
        }
    }

    pub async fn register_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftManagerError> {
        let request = SessionActorMapRequest::RegisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            node_id,
            version,
        };

        self.execute_command(request).await
    }

    pub async fn unregister_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftManagerError> {
        let request = SessionActorMapRequest::UnregisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            session_version: version,
        };

        self.execute_command(request).await
    }

    pub fn current_node_id(&self) -> NodeId {
        self.settings.cluster.node_id
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

        info!("Raft shutdown: id={}", self.settings.cluster.node_id);
        Ok(())
    }

    pub async fn new(
        settings: Arc<Settings>,
        session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
        session_manager_force_stop_recipient: Recipient<
            crate::session::session_manager_actor::ForceStop,
        >,
        session_clock: Arc<SessionClock>,
    ) -> Self {
        let cluster_cfg = &settings.cluster;
        let raft_config = Self::get_raft_config(cluster_cfg.heartbeat_interval.into()).await;

        let dir = Path::new(&cluster_cfg.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let (log_store, state_machine_store) = new_storage(
            &dir,
            session_actor_map_storage.clone(),
            cluster_cfg.node_id,
            session_manager_force_stop_recipient,
            session_clock,
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
            settings: settings.clone(),
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
        let leader_node_id = self.get_leader_node_id().await;
        if leader_node_id.is_none() {
            return false;
        } else {
            let leader_node_id = leader_node_id.unwrap();
            let current_node_id = self.current_node_id();
            leader_node_id == current_node_id
        }
    }

    pub async fn get_leader_node_id(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    pub async fn get_leader(&self) -> Option<Node> {
        self.get_leader_node_id().await.and_then(|id| {
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
    ) -> Result<SessionActorMapResponse, RaftManagerError> {
        super::execute_raft_command(self, command, 3).await
    }

    async fn get_raft_config(heartbeat_interval: u64) -> Config {
        Config {
            cluster_name: "yedmq_session_actor_map_cluster".to_string(),
            ..Default::default()
        }
    }
}
