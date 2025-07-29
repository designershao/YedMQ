use std::{cell::OnceCell, path::Path, sync::Arc};

use actix::prelude::*;
use openraft::{
    error::{ClientWriteError, RaftError},
    Config,
};
use tokio::sync::RwLock;
use yedmq_mqtt::MqttPacketV3;

use crate::{
    protobuf::{raft_service_client::RaftServiceClient, AppendEntriesRequest, RaftType},
    raft::{
        topic::{raft_network_impl::Network, store::new_storage, types::TopicRaft},
        Node, NodeId,
    },
    topic::topic_storage::TopicStorage,
};

#[derive(Debug, Clone)]
pub enum ActorState {
    Initializing,
    Running,
    Failed(TopicRaftError),
    Stopped,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum TopicRaftError {
    #[error("Invalid topic name: {topic}")]
    InvalidTopicName { topic: String },

    #[error("Not leader, current leader: {leader:?}")]
    NotLeader { leader: Option<Node> },

    #[error("Raft errror: {0}")]
    Raft(#[from] RaftError<NodeId, ClientWriteError<NodeId, Node>>),

    #[error("Network error: {0}")]
    Network(#[from] openraft::error::NetworkError),

    #[error("gRPC error: {0}")]
    GRPC(String),

    #[error("Actor not initialized")]
    NotInitialized,

    #[error("Actor not ready: {0}")]
    NotReady(String),

    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("Raft initialization error: {0}")]
    RaftInitializationError(String),

    #[error("No leader available")]
    NoLeaderAvailable,
}

pub struct TopicRaftActor {
    raft: OnceCell<Arc<TopicRaft>>,
    settings: Arc<crate::settings::Settings>,
    state: ActorState,
    pending_messages: Vec<Box<dyn std::any::Any + Send>>,
    topic_storage: OnceCell<Arc<RwLock<TopicStorage>>>,
}

impl TopicRaftActor {
    async fn initialize_raft(
        settings: Arc<crate::settings::Settings>,
    ) -> Result<(TopicRaft, Arc<RwLock<TopicStorage>>), TopicRaftError> {
        let raft_config = Config {
            cluster_name: "yedmq_topic_cluster".to_string(),
            ..Default::default()
        };

        let dir = Path::new(&settings.cluster.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let topic_storage = Arc::new(RwLock::new(TopicStorage::new()));

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
        .map_err(|e| TopicRaftError::RaftInitializationError(e.to_string()))?;
        Ok((raft, topic_storage))
    }

    fn process_pending_messages(&mut self, ctx: &mut Context<Self>) {
        for msg in self.pending_messages.drain(..) {
            if let Some(subscribe_msg) = msg.downcast_ref::<Subscribe>() {
                ctx.address().do_send(subscribe_msg.clone());
            } else if let Some(unsubscribe_msg) = msg.downcast_ref::<Unsubscribe>() {
                ctx.address().do_send(unsubscribe_msg.clone());
            } else if let Some(register_msg) = msg.downcast_ref::<RegisterRetainPublishPacket>() {
                ctx.address().do_send(register_msg.clone());
            } else if let Some(clean_msg) = msg.downcast_ref::<CleanRetainPublishPacket>() {
                ctx.address().do_send(clean_msg.clone());
            } else {
                log::warn!("Unknown pending message type: {:?}", msg);
            }
        }
    }

    async fn try_local_write(
        raft: &TopicRaft,
        command: crate::raft::topic::types::Request,
    ) -> Result<(), TopicRaftError> {
        raft.client_write(command).await.map_err(|e| {
            log::error!("Failed to write command to raft: {}", e);
            if let RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(
                e_inner,
            )) = e
            {
                TopicRaftError::NotLeader {
                    leader: e_inner.leader_node,
                }
            } else {
                TopicRaftError::Raft(e)
            }
        })?;
        Ok(())
    }

    async fn handle_raft_write(
        raft: &TopicRaft,
        request: crate::raft::topic::types::Request,
    ) -> Result<(), TopicRaftError> {
        match Self::try_local_write(raft, request.clone()).await {
            Ok(_) => Ok(()),
            Err(TopicRaftError::NotLeader { leader }) => {
                log::warn!("Not leader, forwarding request to leader: {:?}", leader);
                if let Some(leader_node) = leader {
                    Self::forward_to_leader(leader_node.rpc_addr, request).await
                } else {
                    Err(TopicRaftError::NoLeaderAvailable)
                }
            }
            Err(e) => {
                log::error!("Failed to handle raft write: {}", e);
                Err(e)
            }
        }
    }

    async fn forward_to_leader(
        current_leader_rpc_addr: String,
        msg: crate::raft::topic::types::Request,
    ) -> Result<(), TopicRaftError> {
        Self::send_to_remote_actor(current_leader_rpc_addr, msg).await
    }

    async fn send_to_remote_actor(
        leader_addr: String,
        msg: crate::raft::topic::types::Request,
    ) -> Result<(), TopicRaftError> {
        let mut client = RaftServiceClient::connect(leader_addr).await.map_err(|e| {
            log::error!("Failed to connect to leader {}", e);
            TopicRaftError::GRPC(e.to_string())
        })?;

        let data = serde_json::to_string(&msg).unwrap();

        let request = AppendEntriesRequest {
            data,
            raft_type: RaftType::Topic.into(),
        };

        client.append_entries(request).await.map_err(|e| {
            log::error!("Failed to send append entries request to leader: {}", e);
            TopicRaftError::GRPC(e.to_string())
        })?;

        Ok(())
    }
}

impl Default for TopicRaftActor {
    fn default() -> Self {
        let settings = crate::settings::Settings::default();
        Self {
            raft: OnceCell::new(),
            settings: Arc::new(settings),
            state: ActorState::Initializing,
            pending_messages: Vec::new(),
            topic_storage: OnceCell::new(),
        }
    }
}

impl SystemService for TopicRaftActor {
    fn service_started(&mut self, ctx: &mut Context<Self>) {
        let settings = self.settings.clone();
        let addr = ctx.address();

        ctx.spawn(
            async move {
                let raft_instance = Self::initialize_raft(settings).await;
                addr.do_send(InitializationComplete(raft_instance));
            }
            .into_actor(self),
        );
    }
}

impl Supervised for TopicRaftActor {}

impl Actor for TopicRaftActor {
    type Context = Context<Self>;
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
struct InitializationComplete(Result<(TopicRaft, Arc<RwLock<TopicStorage>>), TopicRaftError>);

impl Handler<InitializationComplete> for TopicRaftActor {
    type Result = ();

    fn handle(&mut self, msg: InitializationComplete, ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            Ok((raft_instance, topic_storage)) => {
                let _ = self.raft.set(Arc::new(raft_instance));
                let _ = self.topic_storage.set(topic_storage);
                self.state = ActorState::Running;
                log::info!("TopicRaftActor initialized successfully.");
                self.process_pending_messages(ctx);
            }
            Err(e) => {
                log::error!("Failed to initialize TopicRaftActor: {}", e);
                self.state = ActorState::Failed(e);
            }
        }
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "Result<(), TopicRaftError>")]
pub struct Subscribe {
    node_id: NodeId,
    tenant_id: String,
    client_identifier: String,
    topic: String,
    qos: u8,
}

impl Handler<Subscribe> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<(), TopicRaftError>>;

    fn handle(&mut self, msg: Subscribe, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                return Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                );
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                return Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = crate::raft::topic::types::Request::SubscribeTopic {
                                node_id: msg.node_id,
                                tenant_id: msg.tenant_id,
                                client_identifier: msg.client_identifier,
                                topic: msg.topic,
                                qos: msg.qos,
                            };
                            Self::handle_raft_write(raft_instance, command).await?;
                            Ok(())
                        } else {
                            Err(TopicRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                );
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                return Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                );
            }
            ActorState::Stopped => {
                return Box::pin(
                    async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                );
            }
        };
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "Result<(), TopicRaftError>")]
pub struct Unsubscribe {
    node_id: NodeId,
    tenant_id: String,
    client_identifier: String,
    topic: String,
}

impl Handler<Unsubscribe> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<(), TopicRaftError>>;

    fn handle(&mut self, msg: Unsubscribe, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                return Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                );
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                return Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = crate::raft::topic::types::Request::UnsubscribeTopic {
                                node_id: msg.node_id,
                                tenant_id: msg.tenant_id,
                                client_identifier: msg.client_identifier,
                                topic: msg.topic,
                            };
                            Self::handle_raft_write(raft_instance, command).await?;
                            Ok(())
                        } else {
                            Err(TopicRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                );
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                return Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                );
            }
            ActorState::Stopped => {
                return Box::pin(
                    async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                );
            }
        };
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "Result<(), TopicRaftError>")]
pub struct RegisterRetainPublishPacket {
    tenant_id: String,
    client_id: String,
    publish_packet: MqttPacketV3,
}

impl Handler<RegisterRetainPublishPacket> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<(), TopicRaftError>>;

    fn handle(&mut self, msg: RegisterRetainPublishPacket, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                return Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                );
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                return Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command =
                                crate::raft::topic::types::Request::RegisterRetainPublishPacket {
                                    tenant_id: msg.tenant_id,
                                    source_client_identifier: msg.client_id,
                                    publish_packet: msg.publish_packet,
                                };
                            Self::handle_raft_write(raft_instance, command).await?;
                            Ok(())
                        } else {
                            Err(TopicRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                );
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                return Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                );
            }
            ActorState::Stopped => {
                return Box::pin(
                    async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                );
            }
        }
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "Result<(), TopicRaftError>")]
pub struct CleanRetainPublishPacket {
    tenant_id: String,
    topic_filter: String,
}

impl Handler<CleanRetainPublishPacket> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<(), TopicRaftError>>;

    fn handle(&mut self, msg: CleanRetainPublishPacket, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                return Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                );
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                return Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command =
                                crate::raft::topic::types::Request::CleanRetainPublishPacket {
                                    tenant_id: msg.tenant_id,
                                    topic_filter: msg.topic_filter,
                                };
                            Self::handle_raft_write(raft_instance, command).await?;
                            Ok(())
                        } else {
                            Err(TopicRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                );
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                return Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                );
            }
            ActorState::Stopped => {
                return Box::pin(
                    async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                );
            }
        }
    }
}
