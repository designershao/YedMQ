pub mod raft_network_impl;
pub mod store;
pub mod types;

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    path::Path,
    sync::Arc,
};

use log::{debug, info};
use mockall::automock;
use openraft::Config;
use raft_network_impl::Network;
use tokio::sync::{watch, Mutex, RwLock};
use yedmq_mqtt::MqttPacketV3;

use crate::{
    protobuf::{raft_service_client::RaftServiceClient, AppendEntriesRequest, RaftType},
    settings::Cluster,
    topic::topic_storage::TopicStorage,
};

use super::{
    topic::{
        store::new_storage,
        types::{Request, TopicRaft},
    },
    Node, NodeId,
};

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

#[async_trait::async_trait]
#[automock]
pub trait TopicRaftManagerTrait {
    async fn subscribe_topic(
        &self,
        node_id: NodeId,
        tenant_id: String,
        client_identifier: String,
        topic: String,
        qos: u8,
    );

    async fn unsubscribe_topic(
        &self,
        node_id: NodeId,
        tenant_id: String,
        client_identifier: String,
        topic: String,
    );

    async fn register_retain_publish_packet(
        &self,
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: MqttPacketV3,
    );

    async fn clean_retain_publish_packet(&self, tenant_id: String, topic_filter: String);

    async fn create_tenant(&self, tenant_id: String);

    fn current_node_id(&self) -> NodeId;

    async fn get_node_by_id(&self, id: NodeId) -> Option<Node>;
}

#[async_trait::async_trait]
impl TopicRaftManagerTrait for RaftManager {
    fn current_node_id(&self) -> NodeId {
        self.cluster_cfg.node_id
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

    async fn subscribe_topic(
        &self,
        node_id: NodeId,
        tenant_id: String,
        client_identifier: String,
        topic: String,
        qos: u8,
    ) {
        self.execute_command(Request::SubscribeTopic {
            node_id,
            tenant_id,
            client_identifier,
            topic,
            qos,
        })
        .await;
    }

    async fn unsubscribe_topic(
        &self,
        node_id: NodeId,
        tenant_id: String,
        client_identifier: String,
        topic: String,
    ) {
        self.execute_command(Request::UnsubscribeTopic {
            node_id,
            tenant_id,
            client_identifier,
            topic,
        })
        .await;
    }

    async fn register_retain_publish_packet(
        &self,
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: MqttPacketV3,
    ) {
        self.execute_command(Request::RegisterRetainPublishPacket {
            tenant_id,
            source_client_identifier,
            publish_packet,
        })
        .await;
    }

    async fn clean_retain_publish_packet(&self, tenant_id: String, topic_filter: String) {
        self.execute_command(Request::CleanRetainPublishPacket {
            tenant_id,
            topic_filter,
        })
        .await;
    }

    async fn create_tenant(&self, tenant_id: String) {
        self.execute_command(Request::CreateTenant { tenant_id })
            .await;
    }
}

pub struct RaftManager {
    pub raft: TopicRaft,

    current_leader: Arc<RwLock<Option<NodeId>>>,

    nodes: Arc<RwLock<HashMap<NodeId, Node>>>,

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

    pub async fn new(cluster_cfg: Cluster, topic_storage: Arc<RwLock<TopicStorage>>) -> Self {
        let raft_config = Self::get_raft_config(cluster_cfg.heartbeat_interval.into()).await;

        let dir = Path::new(&cluster_cfg.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let (log_store, state_machine_store) = new_storage(&dir, topic_storage.clone()).await;

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

        let manager = RaftManager {
            raft,
            current_leader: Arc::new(RwLock::new(None)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            join_handles: Mutex::new(vec![]),
            running_rx: rx,
            running_tx: tx,
            cluster_cfg,
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

    pub async fn execute_command(&self, command: Request) -> Result<(), RaftManagerError> {
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
                    raft_type: RaftType::Topic.into(),
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
