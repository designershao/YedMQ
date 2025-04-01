use std::collections::BTreeMap;
use std::path::Path;
use std::{collections::HashMap, fmt, sync::Arc};

use log::{info, warn};
use mockall::automock;
use openraft::Config;
use raft_network_impl::Network;
use store::new_storage;
use tokio::sync::{watch, Mutex, RwLock};
use types::{SessionStateRequest, SessionStateTypeConfig};
use yedmq_mqtt::MqttPacketV3;

use crate::listener::tcp_listener::MqttTcpListener;
use crate::protobuf::raft_service_client::RaftServiceClient;
use crate::protobuf::{AppendEntriesRequest, RaftType};
use crate::session::session_state_storage::{SessionState, SessionStateStorage};
use crate::{session::session_actor_map_storage::SessionActorMapStorage, settings::Cluster};

use super::Node;
use super::NodeId;

pub mod raft_network_impl;
pub mod store;
pub mod types;

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

pub type SessionStateRaft = openraft::Raft<SessionStateTypeConfig>;

#[async_trait::async_trait]
#[automock]
pub trait SessionStateRaftManagerTrait {
    async fn get_session_state(&self, tenant_id: &str, client_id: &str) -> Option<SessionState>;

   async fn create_session_state(&self, tenant_id: &str, client_id: &str, inflight_duration: u64); 

   async fn delete_session_state(&self, tenant_id: &str, client_id: &str);

   async fn inflight_register_rx_packet(&self, tenant_id: &str, client_id: &str, packet: MqttPacketV3);

   async fn inflight_register_tx_packet(&self, tenant_id: &str, client_id: &str, packet: MqttPacketV3);

   async fn inflight_get_current_packet(&self, tenant_id: &str, client_id: &str, packet_identifier: u16) -> Result<Option<MqttPacketV3>, RaftManagerError>;

   async fn inflight_next_state(&self, tenant_id: &str, client_id: &str, packet_identifier: u16);

   async fn inflight_clean_finished_items(&self, tenant_id: &str, client_id: &str);

   async fn append_to_pending_queue(&self, tenant_id: &str, client_id: &str, packet: MqttPacketV3);

   async fn subscribe_topic(&self, tenant_id: String, client_id: String, topic: String, qos: u8);

   async fn unsubscribe_topic(&self, tenant_id: String, client_id: String, topic: String);

   async fn session_state_exists(&self, tenant_id: &str, client_id: &str) -> bool;
}

#[async_trait::async_trait]
impl SessionStateRaftManagerTrait for SessionStateRaftManager {

    // get session existed from leader
    async fn session_state_exists(&self, tenant_id: &str, client_id: &str) -> bool {
        let current_leader_node_id = self.get_leader_node_id();
        if current_leader_node_id.is_none() {
            warn!("raft get session state leader not found");
            return false;
        }
        let mut client = self.get_grpc_client(current_leader_node_id.unwrap()).await;
        let res = client.session_state_existed(crate::protobuf::SessionExistedRequest {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        }).await;

        match res {
            Ok(r) => {
                let response = r.into_inner();
                if response.success {
                    response.session_existed
                } else {
                    false
                }
            }
            Err(e) => {
                warn!("raft get session state grpc error: {}", e);
                false
            }
        }
    }

    async fn get_session_state(&self, tenant_id: &str, client_id: &str) -> Option<SessionState> {
        let current_leader_node_id = self.get_leader_node_id();
        if current_leader_node_id.is_none() {
            warn!("raft get session state leader not found");
            return None;
        }
        let mut client = self.get_grpc_client(current_leader_node_id.unwrap()).await;
        let res = client.get_session_state(crate::protobuf::GetSessionStateRequest {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        }).await;

        match res {
            Ok(r) => {
                let response = r.into_inner();
                if response.success {
                    response.session_state_data.and_then(|data| {
                        let session_state: SessionState = serde_json::from_str(&data).unwrap();
                        Some(session_state)
                    })
                } else {
                    None
                }
            }
            Err(e) => {
                warn!("raft get session state grpc error: {}", e);
                None
            }
        }
    }

    async fn create_session_state(&self, tenant_id: &str, client_id: &str, inflight_duration: u64) {
        self.execute_command(SessionStateRequest::CreateSessionState {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            inflight_duration_secs: inflight_duration,
        }).await;
    }

    async fn delete_session_state(&self, tenant_id: &str, client_id: &str) {
        self.execute_command(SessionStateRequest::DeleteSessionState {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        }).await;
    }

    async fn inflight_register_rx_packet(&self, tenant_id: &str, client_id: &str, packet: MqttPacketV3) {
        self.execute_command(SessionStateRequest::InflightRegisterRxPacket {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            packet,
        }).await;
    }

    async fn inflight_register_tx_packet(&self, tenant_id: &str, client_id: &str, packet: MqttPacketV3) {
        self.execute_command(SessionStateRequest::InflightRegisterTxPacket {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            packet,
        }).await;
    }

    async fn inflight_get_current_packet(&self, tenant_id: &str, client_id: &str, packet_identifier: u16) -> Result<Option<MqttPacketV3>, RaftManagerError> {
        
        let leader_node_id = self.get_leader_node_id();

        if leader_node_id.is_none() {
            return Err(RaftManagerError::InternalError(
                "No leader available".into(),
            ));
        } else {
            let nodes = self.nodes.read().await;

            let leader_node = nodes.get(&leader_node_id.unwrap()).unwrap();

            let addr = format!("http://{}", leader_node.rpc_addr);

            let mut client = RaftServiceClient::connect(addr.clone()).await.unwrap();

            let response = client.inflight_get_current_packet(crate::protobuf::InflightGetCurrentPacketRequest {
                tenant_id: tenant_id.to_string(),
                client_id: client_id.to_string(),
                packet_id: packet_identifier.into(),
            }).await.unwrap();

            let inner = response.into_inner();

            if inner.success {
                let r = inner.packet.and_then(|packet| {
                    let packet: MqttPacketV3 = serde_json::from_str(&packet).unwrap();
                    Some(packet)
                });
                return Ok(r)
            } else {
                return Err(
                    RaftManagerError::InternalError(
                        format!("Failed to get current packet: {} from node {}", inner.error.unwrap().message, leader_node_id.unwrap())
                    )
                )
            }
        }
    }

    async fn inflight_next_state(&self, tenant_id: &str, client_id: &str, packet_identifier: u16) {
        self.execute_command(SessionStateRequest::InflightNextState {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            packet_identifier: packet_identifier.into(),
        }).await;
    }

    async fn inflight_clean_finished_items(&self, tenant_id: &str, client_id: &str) {
        self.execute_command(SessionStateRequest::InflightCleanFinishItems { 
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        }).await;
    }

    async fn append_to_pending_queue(&self, tenant_id: &str, client_id: &str, packet: MqttPacketV3) {
        self.execute_command(SessionStateRequest::AppendToPendingQueue {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            packet,
        }).await;
    }

    async fn subscribe_topic(&self, tenant_id: String, client_id: String, topic: String, qos: u8) {
        self.execute_command(SessionStateRequest::SubscribeTopic {
            tenant_id,
            client_id,
            topic,
            qos,
        }).await;
    }

    async fn unsubscribe_topic(&self, tenant_id: String, client_id: String, topic: String) {
        self.execute_command(SessionStateRequest::UnsubscribeTopic {
            tenant_id,
            client_id,
            topic,
        }).await;
    }

}


pub struct SessionStateRaftManager {
    pub session_state_storage: Arc<RwLock<SessionStateStorage>>,

    pub raft: SessionStateRaft,

    current_leader: Arc<RwLock<Option<NodeId>>>,

    nodes: Arc<RwLock<HashMap<NodeId, Node>>>,

    join_handles: Mutex<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>>,

    cluster_cfg: Cluster,

    running_rx: watch::Receiver<()>,

    running_tx: watch::Sender<()>,
}

impl SessionStateRaftManager {

    pub async fn create_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
        inflight_duration: u64,
    ) {
        self.execute_command(SessionStateRequest::CreateSessionState {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            inflight_duration_secs: inflight_duration,
        })
        .await;
    }

    pub async fn delete_session_state(&self, tenant_id: &str, client_id: &str) {
        self.execute_command(SessionStateRequest::DeleteSessionState {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        })
        .await;
    }

    async fn get_grpc_client(&self, node_id: NodeId) -> RaftServiceClient<tonic::transport::Channel> {
        let node = self.get_node_by_id(node_id).await.unwrap();
        let addr = format!("http://{}", node.rpc_addr);
        let client = RaftServiceClient::connect(addr.clone()).await.unwrap();
        client
    }

    // get session state from leader
    pub async fn get_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Option<SessionState> {
        let current_leader_node_id = self.get_leader_node_id();
        if current_leader_node_id.is_none() {
            warn!("raft get session state leader not found");
            return None;
        }
        let mut client = self.get_grpc_client(current_leader_node_id.unwrap()).await;
        let res = client.get_session_state(crate::protobuf::GetSessionStateRequest {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        }).await;

        match res {
            Ok(r) => {
                let response = r.into_inner();
                if response.success {
                    response.session_state_data.and_then(|data| {
                        let session_state: SessionState = serde_json::from_str(&data).unwrap();
                        Some(session_state)
                    })
                } else {
                    None
                }
            }
            Err(e) => {
                warn!("raft get session state grpc error: {}", e);
                None
            }
        }
    }

    // get session state from local raft store
    pub async fn get_session_state_from_local_raft_store(
        &self,
        tenant_id: &str,
        session_id: &str,
    ) -> Option<Arc<RwLock<SessionState>>> {
        let session_state_guard = self.session_state_storage.read().await;
        session_state_guard
            .get_session_state(tenant_id, session_id)
            .await
    }

    pub async fn session_state_exists_from_local_raft_store(&self, tenant_id: &str, session_id: &str) -> bool {
        let session_state_guard = self.session_state_storage.read().await;
        session_state_guard
            .session_state_exists(tenant_id, session_id)
            .await
    }

    pub fn current_node_id(&self) -> NodeId {
        self.cluster_cfg.node_id
    }

    pub async fn get_node_by_id(&self, id: NodeId) -> Option<Node> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .find(|x| *x.0 == id)
            .and_then(|x| Some(x.1.clone()))
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
        session_state_storage: Arc<RwLock<SessionStateStorage>>,
    ) -> Self {
        let raft_config = Self::get_raft_config(cluster_cfg.heartbeat_interval.into()).await;

        let dir = Path::new(&cluster_cfg.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let (log_store, state_machine_store) =
            new_storage(&dir, session_state_storage.clone()).await;

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

        let manager = SessionStateRaftManager {
            raft,
            current_leader: Arc::new(RwLock::new(None)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            join_handles: Mutex::new(vec![]),
            running_rx: rx,
            running_tx: tx,
            cluster_cfg,
            session_state_storage,
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

    pub async fn init_cluster(&self) -> Result<(), RaftManagerError> {
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
        command: SessionStateRequest,
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
                    raft_type: RaftType::SessionState.into(),
                };

                let res = client.append_entries(append_request).await;
                if res.is_err() {
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
