pub mod raft_network_impl;
pub mod store;
pub mod types;
pub mod topic_raft_actor;

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
};

use log::info;
use mockall::automock;
use openraft::{
    error::{CheckIsLeaderError, ClientWriteError, Infallible, InitializeError},
    Config,
};
use raft_network_impl::Network;
use tokio::sync::{watch, Mutex, RwLock};
use yedmq_mqtt::MqttPacketV3;

use crate::{
    settings::Settings,
    topic::topic_storage::{Subscription, TopicStorage},
};

use super::{
    raft_manager::RaftManagerError,
    topic::{
        store::new_storage,
        types::{Request, TopicRaft},
    },
    Node, NodeId,
};

#[async_trait::async_trait]
#[automock]
pub trait TopicRaftManagerTrait {

    async fn get_leader(&self) -> Option<Node>;

    async fn get_node_by_id(&self, node_id: NodeId) -> Option<Node>;

    async fn init_cluster(&self) -> Result<(), RaftManagerError<InitializeError<NodeId, Node>>>;

    async fn subscribe_topic(
        &self,
        node_id: NodeId,
        tenant_id: String,
        client_identifier: String,
        topic: String,
        qos: u8,
    ) -> Result<(), RaftManagerError<openraft::error::ClientWriteError<NodeId, Node>>>;

    async fn unsubscribe_topic(
        &self,
        node_id: NodeId,
        tenant_id: String,
        client_identifier: String,
        topic: String,
    ) -> Result<(), RaftManagerError<openraft::error::ClientWriteError<NodeId, Node>>>;

    async fn register_retain_publish_packet(
        &self,
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: MqttPacketV3,
    ) -> Result<(), RaftManagerError<openraft::error::ClientWriteError<NodeId, Node>>>;

    async fn clean_retain_publish_packet(
        &self,
        tenant_id: String,
        topic_filter: String,
    ) -> Result<(), RaftManagerError<openraft::error::ClientWriteError<NodeId, Node>>>;

    async fn create_tenant(
        &self,
        tenant_id: String,
    ) -> Result<(), RaftManagerError<openraft::error::ClientWriteError<NodeId, Node>>>;

    async fn get_subscriptions_ensure_linearizable(
        &self,
        tenant_id: String,
        topic: String,
    ) -> std::result::Result<
        Vec<Arc<Subscription>>,
        RaftManagerError<openraft::error::CheckIsLeaderError<NodeId, Node>>,
    >;

    fn raft(&self) -> &TopicRaft;
}

#[async_trait::async_trait]
impl TopicRaftManagerTrait for RaftManager {

    async fn get_node_by_id(&self, id: NodeId) -> Option<Node> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .find(|x| *x.0 == id)
            .and_then(|x| Some(x.1.clone()))
    }

    async fn get_leader(&self) -> Option<Node>{
        self.raft.current_leader().await.and_then(|id| {
            self.raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .find(|x| *x.0 == id)
                .and_then(|x| Some(x.1.clone()))
        })
    }

    async fn init_cluster(
        &self,
    ) -> Result<(), RaftManagerError<InitializeError<NodeId, Node>>> {
        let mut cluster_nodes = BTreeMap::new();
        cluster_nodes.insert(
            self.settings.cluster.node_id,
            Node {
                rpc_addr: self.settings.cluster.rpc.external.to_string(),
                api_addr: self.settings.listener.api.external.to_string(),
            },
        );

        self.raft.initialize(cluster_nodes).await?;
        Ok(())
    }

    fn raft(&self) -> &TopicRaft {
        &self.raft
    }

    async fn get_subscriptions_ensure_linearizable(
        &self,
        tenant_id: String,
        topic: String,
    ) -> std::result::Result<
        Vec<Arc<Subscription>>,
        RaftManagerError<CheckIsLeaderError<NodeId, Node>>,
    > {
        self.raft.ensure_linearizable().await?;
        let topic_storage_guard = self.topic_storage.read().await;
        let subscriptions = topic_storage_guard
            .get_subscriptions(tenant_id, topic)
            .unwrap();
        Ok(subscriptions)
    }

    async fn subscribe_topic(
        &self,
        node_id: NodeId,
        tenant_id: String,
        client_identifier: String,
        topic: String,
        qos: u8,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let command = Request::SubscribeTopic {
            tenant_id,
            client_identifier,
            topic,
            qos,
        };
        let res = self.raft.client_write(command).await?;
        match res.data {
            types::Response::None => return Ok(()),
            _ => {
                return Err(RaftManagerError::InternalError(
                    "response is not topic response None".to_string(),
                ))
            }
        }
    }

    async fn unsubscribe_topic(
        &self,
        node_id: NodeId,
        tenant_id: String,
        client_identifier: String,
        topic: String,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let command = Request::UnsubscribeTopic {
            node_id,
            tenant_id,
            client_identifier,
            topic,
        };

        let res = self.raft.client_write(command).await?;
        match res.data {
            types::Response::None => return Ok(()),
            _ => {
                return Err(RaftManagerError::InternalError(
                    "response is not topic response None".to_string(),
                ))
            }
        }
    }

    async fn register_retain_publish_packet(
        &self,
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: MqttPacketV3,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let command = Request::RegisterRetainPublishPacket {
            tenant_id,
            source_client_identifier,
            publish_packet,
        };

        let res = self.raft.client_write(command).await?;
        match res.data {
            types::Response::None => return Ok(()),
            _ => {
                return Err(RaftManagerError::InternalError(
                    "response is not topic response None".to_string(),
                ))
            }
        }
    }

    async fn clean_retain_publish_packet(
        &self,
        tenant_id: String,
        topic_filter: String,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let command = Request::CleanRetainPublishPacket {
            tenant_id,
            topic_filter,
        };

        let res = self.raft.client_write(command).await?;
        match res.data {
            types::Response::None => return Ok(()),
            _ => {
                return Err(RaftManagerError::InternalError(
                    "response is not topic response None".to_string(),
                ))
            }
        }
    }

    async fn create_tenant(
        &self,
        tenant_id: String,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let command = Request::CreateTenant { tenant_id };
        let res = self.raft.client_write(command).await?;
        match res.data {
            types::Response::None => return Ok(()),
            _ => {
                return Err(RaftManagerError::InternalError(
                    "response is not topic response None".to_string(),
                ))
            }
        }
    }
}

pub struct RaftManager {
    pub raft: Arc<TopicRaft>,

    pub topic_storage: Arc<RwLock<TopicStorage>>,

    current_leader: Arc<RwLock<Option<NodeId>>>,

    nodes: Arc<RwLock<HashMap<NodeId, Node>>>,

    join_handles: Mutex<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>>,

    settings: Arc<Settings>,

    running_rx: watch::Receiver<()>,

    running_tx: watch::Sender<()>,
    
    raft_client: Arc<dyn crate::raft::client::topic::TopicRaftClientTrait>
}

impl Drop for RaftManager {
    fn drop(&mut self) {
        println!("Raft drop: id={}", self.settings.cluster.node_id);
    }
}

impl RaftManager {
    pub fn get_raft_client(&self) -> Arc<dyn crate::raft::client::topic::TopicRaftClientTrait> {
        self.raft_client.clone()
    }

    async fn get_raft_config(heartbeat_interval: u64) -> Config {
        Config {
            cluster_name: "yedmq_topic_cluster".to_string(),
            ..Default::default()
        }
    }

    pub async fn stop(&self) -> Result<(), super::raft_manager::RaftManagerError<Infallible>> {
        let mut rx = self.raft.metrics();

        self.raft.shutdown().await.map_err(|e| {
            super::raft_manager::RaftManagerError::InternalError(format!(
                "Failed to shutdown raft, {}",
                e
            ))
        })?;

        if let Err(e) = self.running_tx.send(()) {
            return Err(super::raft_manager::RaftManagerError::InternalError(
                format!("Failed to shutdown raft, {}", e),
            ));
        }

        loop {
            let r = rx.changed().await;
            if r.is_err() {
                break;
            }
        }

        for j in self.join_handles.lock().await.iter_mut() {
            let _rst = j.await.map_err(|e| {
                super::raft_manager::RaftManagerError::InternalError(format!("{}", e))
            })?;
        }

        info!("Raft shutdown: id={}", self.settings.cluster.node_id);
        Ok(())
    }

    pub async fn new(settings: Arc<Settings>, topic_storage: Arc<RwLock<TopicStorage>>) -> Self {
        let raft_config = Self::get_raft_config(settings.cluster.heartbeat_interval.into()).await;

        let dir = Path::new(&settings.cluster.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let (log_store, state_machine_store) = new_storage(&dir, topic_storage.clone()).await;

        let network = Network {};

        let raft = openraft::Raft::new(
            settings.cluster.node_id,
            config.clone(),
            network,
            log_store,
            state_machine_store,
        )
        .await
        .unwrap();

        // initialize raft cluster
        let mut cluster_nodes = BTreeMap::new();
        for item in settings.cluster.nodes.iter() {
            cluster_nodes.insert(
                item.id,
                Node {
                    rpc_addr: item.rpc_address.to_string(),
                    api_addr: item.api_address.to_string(),
                },
            );
        }
        if !raft.is_initialized().await.unwrap() {
            raft.initialize(cluster_nodes).await.unwrap();
        }
        //

        let raft_arc = Arc::new(raft);
        let (tx, rx) = watch::channel::<()>(());

        let raft_client = crate::raft::client::topic::TopicRaftClient::new(raft_arc.clone());

        let manager = RaftManager {
            raft: raft_arc.clone(),
            current_leader: Arc::new(RwLock::new(None)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            join_handles: Mutex::new(vec![]),
            running_rx: rx,
            running_tx: tx,
            settings,
            topic_storage,
            raft_client: Arc::new(raft_client)
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
}
