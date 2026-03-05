use std::{cell::OnceCell, collections::{BTreeMap, HashMap}, path::Path, sync::Arc};

use actix::dev::MessageResponse;
use actix::prelude::*;
use openraft::{
    error::{ClientWriteError, Fatal, InitializeError, RaftError},
    raft::ClientWriteResponse,
    Config, RaftMetrics,
};
use parking_lot::RwLock;
use yedmq_mqtt::MqttPacketV3;

use crate::protobuf::cluster_service_client::ClusterServiceClient;
use crate::{
    protobuf::{raft_service_client::RaftServiceClient, RaftType, WriteRequest},
    raft::{
        topic::{raft_network_impl::Network, store::new_storage, types::TopicRaft},
        Node, NodeId,
    },
    topic::{topic_storage::TopicStorage, TopicError},
};

#[derive(Debug, Clone)]
pub enum ActorState {
    Initializing,
    Running,
    Failed(Box<TopicRaftError>),
    Stopped,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum TopicRaftError {
    #[error("Invalid topic name: {topic}")]
    InvalidTopicName { topic: String },

    #[error("Not leader, current leader: {leader:?}")]
    NotLeader { leader: Option<Node> },

    #[error("Raft errror: {0}")]
    RaftClientWriteError(#[from] RaftError<NodeId, ClientWriteError<NodeId, Node>>),

    #[error("Raft append entries error: {0}")]
    RaftAppendEntriesError(#[from] RaftError<NodeId>),

    #[error("Raft install snapshot error: {0}")]
    RaftInstallSnapshotError(#[from] RaftError<NodeId, openraft::error::InstallSnapshotError>),

    #[error("Network error: {0}")]
    RaftNetworkError(#[from] openraft::error::NetworkError),

    #[error("Raft init cluster error: {0}")]
    RaftInitializationError(#[from] RaftError<NodeId, InitializeError<NodeId, Node>>),

    #[error("Raft fatal error: {0}")]
    RaftFatalError(#[from] Fatal<NodeId>),

    #[error("gRPC error: {0}")]
    GRPC(String),

    #[error("Actor not initialized")]
    NotInitialized,

    #[error("Actor not ready: {0}")]
    NotReady(String),

    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("No leader available")]
    NoLeaderAvailable,

    #[error("Topic error: {0}")]
    TopicError(#[from] TopicError),
}

pub struct TopicRaftActor {
    raft: OnceCell<Arc<TopicRaft>>,
    settings: Option<Arc<crate::settings::Settings>>,
    state: ActorState,
    pending_messages: Vec<Box<dyn std::any::Any + Send>>,
    topic_storage: OnceCell<Arc<RwLock<TopicStorage>>>,
}

impl TopicRaftActor {
    async fn initialize_raft(
        settings: Arc<crate::settings::Settings>,
    ) -> Result<(TopicRaft, Arc<RwLock<TopicStorage>>), TopicRaftError> {
        let raft_config = Config {
            cluster_name: "yedmq_topic_raft_cluster".to_string(),
            heartbeat_interval: settings.cluster.heartbeat_interval as u64,
            election_timeout_min: (settings.cluster.heartbeat_interval * 5) as u64,
            election_timeout_max: (settings.cluster.heartbeat_interval * 10) as u64,
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
        .await?;

        // init raft cluster nodes
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
        if settings.cluster.startup_mode == crate::settings::ClusterStartupMode::Bootstrap
            && !raft.is_initialized().await?
        {
            raft.initialize(cluster_nodes).await?;
        }
        //

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

    async fn try_local_linearizable_read(raft: &TopicRaft) -> Result<(), TopicRaftError> {
        raft.ensure_linearizable().await.map_err(|e| {
            log::error!("Failed to ensure linearizable read: {}", e);
            if let Some(leader) = e.forward_to_leader() {
                TopicRaftError::NotLeader {
                    leader: leader.leader_node.clone(),
                }
            } else {
                TopicRaftError::NotLeader { leader: None }
            }
        })?;
        Ok(())
    }

    async fn try_local_write(
        raft: &TopicRaft,
        command: crate::raft::topic::types::Request,
    ) -> Result<(), TopicRaftError> {
        raft.client_write(command).await.map_err(|e| {
            if let RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(
                e_inner,
            )) = e
            {
                TopicRaftError::NotLeader {
                    leader: e_inner.leader_node,
                }
            } else {
                log::warn!("failed to write command to raft: {}", e);
                TopicRaftError::RaftClientWriteError(e)
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
                log::debug!("Not leader, forwarding request to leader: {:?}", leader);
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
        let mut client = RaftServiceClient::connect(format!("http://{}", &leader_addr))
            .await
            .map_err(|e| {
                log::error!("Failed to connect to leader {}", e);
                TopicRaftError::GRPC(e.to_string())
            })?;

        let data = serde_json::to_string(&msg).unwrap();

        let request = WriteRequest {
            data,
            raft_type: RaftType::Topic.into(),
        };

        client.write(request).await.map_err(|e| {
            log::error!("Failed to send write request to leader: {}", e);
            TopicRaftError::GRPC(e.to_string())
        })?;

        Ok(())
    }
}

impl Default for TopicRaftActor {
    fn default() -> Self {
        Self {
            raft: OnceCell::new(),
            settings: None,
            state: ActorState::Initializing,
            pending_messages: Vec::new(),
            topic_storage: OnceCell::new(),
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Initialize {
    pub settings: Arc<crate::settings::Settings>,
}

impl Handler<Initialize> for TopicRaftActor {
    type Result = ();

    fn handle(&mut self, msg: Initialize, ctx: &mut Self::Context) -> Self::Result {
        self.settings = Some(msg.settings.clone());
        let settings = msg.settings.clone();
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

impl SystemService for TopicRaftActor {
    fn service_started(&mut self, _ctx: &mut Context<Self>) {}
}

impl Supervised for TopicRaftActor {}

impl Actor for TopicRaftActor {
    type Context = Context<Self>;
}

#[derive(Message, Clone)]
#[rtype(result = "GetTopicStorageResponse")]
pub struct GetTopicStorage;

pub struct GetTopicStorageResponse {
    pub topic_storage: Arc<RwLock<TopicStorage>>,
}

impl<A, M> MessageResponse<A, M> for GetTopicStorageResponse
where
    A: Actor,
    M: Message<Result = GetTopicStorageResponse>,
{
    fn handle(
        self,
        _ctx: &mut <A as Actor>::Context,
        tx: Option<actix::dev::OneshotSender<<M as Message>::Result>>,
    ) {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
    }
}

impl Handler<GetTopicStorage> for TopicRaftActor {
    type Result = GetTopicStorageResponse;

    fn handle(&mut self, _msg: GetTopicStorage, _ctx: &mut Self::Context) -> Self::Result {
        GetTopicStorageResponse {
            topic_storage: self.topic_storage.get().unwrap().clone(),
        }
    }
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
                self.state = ActorState::Failed(Box::new(e));
            }
        }
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "Result<(), TopicRaftError>")]
pub struct Subscribe {
    pub tenant_id: String,
    pub client_identifier: String,
    pub topic: String,
    pub qos: u8,
}

impl Handler<Subscribe> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<(), TopicRaftError>>;

    fn handle(&mut self, msg: Subscribe, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = crate::raft::topic::types::Request::SubscribeTopic {
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
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                    .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "Result<(), TopicRaftError>")]
pub struct Unsubscribe {
    pub tenant_id: String,
    pub client_identifier: String,
    pub topic: String,
}

impl Handler<Unsubscribe> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<(), TopicRaftError>>;

    fn handle(&mut self, msg: Unsubscribe, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = crate::raft::topic::types::Request::UnsubscribeTopic {
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
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                    .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "Result<(), TopicRaftError>")]
pub struct RegisterRetainPublishPacket {
    pub tenant_id: String,
    pub client_id: String,
    pub publish_packet: MqttPacketV3,
}

impl Handler<RegisterRetainPublishPacket> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<(), TopicRaftError>>;

    fn handle(&mut self, msg: RegisterRetainPublishPacket, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
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
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                    .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "Result<(), TopicRaftError>")]
pub struct CleanRetainPublishPacket {
    pub tenant_id: String,
    pub topic_filter: String,
}

impl Handler<CleanRetainPublishPacket> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<(), TopicRaftError>>;

    fn handle(&mut self, msg: CleanRetainPublishPacket, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
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
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                    .into_actor(self),
            ),
        }
    }
}

pub struct SubscriptionInfo {
    pub client_identifier: String,
    pub qos: u8,
}

pub struct GetSubscriptionsResponse {
    pub subscriptions: Vec<SubscriptionInfo>,
}

impl<A, M> MessageResponse<A, M> for GetSubscriptionsResponse
where
    A: Actor,
    M: Message<Result = GetSubscriptionsResponse>,
{
    fn handle(
        self,
        _ctx: &mut <A as Actor>::Context,
        tx: Option<actix::dev::OneshotSender<<M as Message>::Result>>,
    ) {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<GetSubscriptionsResponse, TopicRaftError>")]
pub struct GetSubscriptions {
    pub tenant_id: String,
    pub topic: String,
}

impl Handler<GetSubscriptions> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<GetSubscriptionsResponse, TopicRaftError>>;

    fn handle(&mut self, msg: GetSubscriptions, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let topic_storage = self.topic_storage.clone();
                Box::pin(
                    async move {
                        if raft.get().is_some() {
                            if let Some(topic_storage) = topic_storage.get() {
                                let storage = topic_storage.read();
                                let subscriptions = storage
                                    .get_subscriptions(msg.tenant_id, msg.topic)
                                    .map(|x| {
                                        let info = x
                                            .iter()
                                            .map(move |x| SubscriptionInfo {
                                                client_identifier: x.client_identifier.clone(),
                                                qos: x.qos,
                                            })
                                            .collect();
                                        GetSubscriptionsResponse {
                                            subscriptions: info,
                                        }
                                    })
                                    .map_err(|e| TopicRaftError::GRPC(e.to_string()))?;

                                Ok(subscriptions)
                            } else {
                                Err(TopicRaftError::NotInitialized)
                            }
                        } else {
                            Err(TopicRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                    .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<GetSubscriptionsResponse, TopicRaftError>")]
pub struct GetSubscriptionsEnsureLinearizable {
    pub tenant_id: String,
    pub topic: String,
}

impl Handler<GetSubscriptionsEnsureLinearizable> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<GetSubscriptionsResponse, TopicRaftError>>;

    fn handle(
        &mut self,
        msg: GetSubscriptionsEnsureLinearizable,
        _: &mut Self::Context,
    ) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let topic_storage = self.topic_storage.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            match Self::try_local_linearizable_read(raft_instance).await {
                                Ok(_) => {
                                    if let Some(topic_storage) = topic_storage.get() {
                                        let storage = topic_storage.read();
                                        let subscriptions = storage
                                            .get_subscriptions(msg.tenant_id, msg.topic)
                                            .map(|x| {
                                                let info = x
                                                    .iter()
                                                    .map(move |x| SubscriptionInfo {
                                                        client_identifier: x.client_identifier.clone(),
                                                        qos: x.qos,
                                                    })
                                                    .collect();
                                                GetSubscriptionsResponse {
                                                    subscriptions: info,
                                                }
                                            })
                                            .map_err(|e| TopicRaftError::GRPC(e.to_string()))?;

                                        Ok(subscriptions)
                                    } else {
                                        Err(TopicRaftError::NotInitialized)
                                    }
                                }
                                Err(TopicRaftError::NotLeader { leader }) => {
                                    log::error!("Failed to ensure linearizable read, current node is not leader");
                                    if let Some(leader) = leader {
                                        let mut client = ClusterServiceClient::connect(format!("http://{}", leader.rpc_addr.clone())).await.map_err(|e| TopicRaftError::GRPC(e.to_string()))?;
                                        client.get_subscribers_by_topic(crate::protobuf::GetSubscribersByTopicRequest {
                                            tenant_id: msg.tenant_id,
                                            topic: msg.topic,
                                        }).await
                                            .map_err(|e| TopicRaftError::GRPC(e.to_string()))
                                            .map(|response| {
                                                let subscriptions = response.into_inner().payload.into_iter()
                                                    .map(|x| SubscriptionInfo {
                                                        client_identifier: x.client_id,
                                                        qos: x.qos as u8,
                                                    })
                                                    .collect();
                                                GetSubscriptionsResponse { subscriptions }
                                            })
                                    } else {
                                        Err(TopicRaftError::NoLeaderAvailable)
                                    }
                                }
                                Err(e) => {
                                    log::error!("Failed to ensure linearizable read: {}", e);
                                    Err(e)
                                }
                            }
                        } else {
                            Err(TopicRaftError::NotInitialized)
                        }
                    }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                    .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<Vec<Arc<MqttPacketV3>>, TopicRaftError>")]
pub struct GetRetainPublishPacket {
    pub tenant_id: String,
    pub topic: String,
}

impl Handler<GetRetainPublishPacket> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<Vec<Arc<MqttPacketV3>>, TopicRaftError>>;

    fn handle(&mut self, msg: GetRetainPublishPacket, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let topic_storage = self.topic_storage.clone();
                Box::pin(
                    async move {
                        if raft.get().is_some() {
                            if let Some(topic_storage) = topic_storage.get() {
                                let storage = topic_storage.read();
                                let packets = storage
                                    .get_retain_publish_packet(msg.tenant_id, msg.topic)
                                    .map_err(|e| TopicRaftError::GRPC(e.to_string()))?;
                                Ok(packets)
                            } else {
                                Err(TopicRaftError::NotInitialized)
                            }
                        } else {
                            Err(TopicRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                    .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<Vec<Arc<MqttPacketV3>>, TopicRaftError>")]
pub struct GetRetainPublishPacketEnsureLinearizable {
    pub tenant_id: String,
    pub topic: String,
}

impl Handler<GetRetainPublishPacketEnsureLinearizable> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<Vec<Arc<MqttPacketV3>>, TopicRaftError>>;

    fn handle(
        &mut self,
        msg: GetRetainPublishPacketEnsureLinearizable,
        _: &mut Self::Context,
    ) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                Box::pin(
                    async { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let topic_storage = self.topic_storage.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            match Self::try_local_linearizable_read(raft_instance).await {
                                Ok(_) => {
                                    if let Some(topic_storage) = topic_storage.get() {
                                        let storage = topic_storage.read();
                                        let packets = storage
                                            .get_retain_publish_packet(msg.tenant_id, msg.topic)
                                            .map_err(|e| TopicRaftError::GRPC(e.to_string()))?;
                                        Ok(packets)
                                    } else {
                                        Err(TopicRaftError::NotInitialized)
                                    }
                                }
                                Err(TopicRaftError::NotLeader { leader }) => {
                                    log::error!("Failed to ensure linearizable read, current node is not leader");
                                    if let Some(leader) = leader {
                                        let mut client = ClusterServiceClient::connect(format!("http://{}", leader.rpc_addr.clone())).await
                                            .map_err(|e| TopicRaftError::GRPC(e.to_string()))?;
                                        let response = client.get_retain_publish_message(crate::protobuf::GetRetainPublishMessageRequest {
                                            tenant_id: msg.tenant_id,
                                            topic: msg.topic,
                                        }).await
                                            .map_err(|e| TopicRaftError::GRPC(e.to_string()))?;

                                        let inner = response.into_inner();
                                        if inner.success {
                                            if let Some(payload) = inner.payload {
                                                // Deserialize payload to Vec<Arc<MqttPacketV3>>
                                                let packets: Vec<Arc<MqttPacketV3>> = serde_json::from_str(&payload)
                                                    .map_err(|e| TopicRaftError::GRPC(format!("Failed to deserialize payload: {}", e)))?;
                                                Ok(packets)
                                            } else {
                                                Ok(vec![])
                                            }
                                        } else {
                                            Err(TopicRaftError::GRPC(format!("Failed to get retain publish message: {:?}", inner.error)))
                                        }
                                    } else {
                                        Err(TopicRaftError::NoLeaderAvailable)
                                    }
                                }
                                Err(e) => {
                                    log::error!("Failed to ensure linear read: {}", e);
                                    Err(e)
                                }
                            }
                        } else {
                            Err(TopicRaftError::NotInitialized)
                        }
                    }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(TopicRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async { Err(TopicRaftError::NotReady("Actor is stopped".to_string())) }
                    .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::AppendEntriesResponse<NodeId>, TopicRaftError>")]
pub struct AppendEntriesRequestMessage {
    pub payload: openraft::raft::AppendEntriesRequest<super::types::TypeConfig>,
}

impl Handler<AppendEntriesRequestMessage> for TopicRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<openraft::raft::AppendEntriesResponse<NodeId>, TopicRaftError>,
    >;

    fn handle(&mut self, msg: AppendEntriesRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.append_entries(msg.payload).await?;
                            Ok(res)
                        } else {
                            Err(TopicRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::InstallSnapshotResponse<NodeId>, TopicRaftError>")]
pub struct InstallSnapshotRequestMessage {
    pub payload: openraft::raft::InstallSnapshotRequest<super::types::TypeConfig>,
}

impl Handler<InstallSnapshotRequestMessage> for TopicRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<openraft::raft::InstallSnapshotResponse<NodeId>, TopicRaftError>,
    >;

    fn handle(
        &mut self,
        msg: InstallSnapshotRequestMessage,
        _: &mut Self::Context,
    ) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.install_snapshot(msg.payload).await?;
                            Ok(res)
                        } else {
                            Err(TopicRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::VoteResponse<NodeId>, TopicRaftError>")]
pub struct VoteRequestMessage {
    pub payload: openraft::raft::VoteRequest<NodeId>,
}

impl Handler<VoteRequestMessage> for TopicRaftActor {
    type Result =
        ResponseActFuture<Self, Result<openraft::raft::VoteResponse<NodeId>, TopicRaftError>>;

    fn handle(&mut self, msg: VoteRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.vote(msg.payload).await?;
                            Ok(res)
                        } else {
                            Err(TopicRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

pub struct RetainMessage {
    pub topic: String,
    pub client_id: String,
    pub qos: u8,
}

pub struct GetRetainMessageListWithPaginationResponse {
    pub total: u64,
    pub data: Vec<RetainMessage>,
}

#[derive(Message)]
#[rtype(result = "Result<GetRetainMessageListWithPaginationResponse, TopicRaftError>")]
pub struct GetRetainMessageListWithPagination {
    pub tenant_id: String,
    pub offset: u64,
    pub limit: u64,
}

impl Handler<GetRetainMessageListWithPagination> for TopicRaftActor {
    type Result =
        ResponseActFuture<Self, Result<GetRetainMessageListWithPaginationResponse, TopicRaftError>>;

    fn handle(
        &mut self,
        msg: GetRetainMessageListWithPagination,
        _: &mut Self::Context,
    ) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let topic_storage = self.topic_storage.clone();
                Box::pin(
                    async move {
                        if raft.get().is_some() {
                            if let Some(topic_storage) = topic_storage.get() {
                                let storage = topic_storage.read();
                                let res = storage.get_retain_message_list_with_pagination(
                                    &msg.tenant_id,
                                    msg.offset,
                                    msg.limit,
                                );
                                match res {
                                    Ok(res) => Ok(GetRetainMessageListWithPaginationResponse {
                                        total: res.0,
                                        data: res
                                            .1
                                            .iter()
                                            .map(|(topic, client_id, qos)| RetainMessage {
                                                topic: topic.clone(),
                                                client_id: client_id.clone(),
                                                qos: *qos,
                                            })
                                            .collect(),
                                    }),
                                    Err(e) => {
                                        let topic_err = e.downcast_ref::<TopicError>().unwrap();
                                        Err(TopicRaftError::TopicError(topic_err.clone()))
                                    }
                                }
                            } else {
                                Err(TopicRaftError::NotReady("Initializing".to_string()))
                            }
                        } else {
                            Err(TopicRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

pub struct TopicInfo {
    pub topic: String,
    pub qos: u8,
    pub client_id: String,
}

pub struct GetTopicListWithPaginationResponse {
    pub total: u64,
    pub data: Vec<TopicInfo>,
}

#[derive(Message)]
#[rtype(result = "Result<GetTopicListWithPaginationResponse, TopicRaftError>")]
pub struct GetTopicListWithPagination {
    pub tenant_id: String,
    pub offset: u64,
    pub limit: u64,
}

impl Handler<GetTopicListWithPagination> for TopicRaftActor {
    type Result =
        ResponseActFuture<Self, Result<GetTopicListWithPaginationResponse, TopicRaftError>>;

    fn handle(&mut self, msg: GetTopicListWithPagination, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let topic_storage = self.topic_storage.clone();
                Box::pin(
                    async move {
                        if raft.get().is_some() {
                            if let Some(topic_storage) = topic_storage.get() {
                                let storage = topic_storage.read();
                                let res = storage.get_topic_list_with_pagination(
                                    &msg.tenant_id,
                                    msg.offset,
                                    msg.limit,
                                );
                                match res {
                                    Ok(res) => Ok(GetTopicListWithPaginationResponse {
                                        total: res.0,
                                        data: res
                                            .1
                                            .iter()
                                            .map(|(client_id, topic, qos)| TopicInfo {
                                                topic: topic.clone(),
                                                client_id: client_id.clone(),
                                                qos: *qos,
                                            })
                                            .collect(),
                                    }),
                                    Err(e) => {
                                        let topic_err = e.downcast_ref::<TopicError>().unwrap();
                                        Err(TopicRaftError::TopicError(topic_err.clone()))
                                    }
                                }
                            } else {
                                Err(TopicRaftError::NotReady("Initializing".to_string()))
                            }
                        } else {
                            Err(TopicRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), TopicRaftError>")]
pub struct InitRaftClusterMessage {}

impl Handler<InitRaftClusterMessage> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<(), TopicRaftError>>;

    fn handle(&mut self, _msg: InitRaftClusterMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                if let Some(raft_instance) = self.raft.get() {
                    let mut cluster_nodes = BTreeMap::new();
                    for item in self
                        .settings
                        .as_ref()
                        .expect("settings should not be none")
                        .cluster
                        .nodes
                        .iter()
                    {
                        cluster_nodes.insert(
                            item.id,
                            Node {
                                rpc_addr: item.rpc_address.to_string(),
                                api_addr: item.api_address.to_string(),
                            },
                        );
                    }
                    let raft = raft_instance.clone();
                    Box::pin(
                        async move {
                            raft.initialize(cluster_nodes).await?;
                            Ok(())
                        }
                        .into_actor(self),
                    )
                } else {
                    Box::pin(
                        async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                            .into_actor(self),
                    )
                }
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<super::types::TypeConfig>, TopicRaftError>")]
pub struct AddLearnerMessage {
    pub node_id: NodeId,
    pub node: Node,
}

impl Handler<AddLearnerMessage> for TopicRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<ClientWriteResponse<super::types::TypeConfig>, TopicRaftError>,
    >;

    fn handle(&mut self, msg: AddLearnerMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance
                                .add_learner(msg.node_id, msg.node, true)
                                .await?;
                            Ok(res)
                        } else {
                            Err(TopicRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<super::types::TypeConfig>, TopicRaftError>")]
pub struct ChangeMembershipMessage {
    pub members: Vec<NodeId>,
}

impl Handler<ChangeMembershipMessage> for TopicRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<ClientWriteResponse<super::types::TypeConfig>, TopicRaftError>,
    >;

    fn handle(&mut self, msg: ChangeMembershipMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.change_membership(msg.members, true).await?;
                            Ok(res)
                        } else {
                            Err(TopicRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<Node>, TopicRaftError>")]
pub struct GetLeader {}

impl Handler<GetLeader> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<Node>, TopicRaftError>>;

    fn handle(&mut self, _msg: GetLeader, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let current_leader_node_id = raft_instance.current_leader().await;
                            if let Some(current_leader_node_id) = current_leader_node_id {
                                let metrics_ref = raft_instance.metrics();
                                let metrics = metrics_ref.borrow();
                                let mut nodes_iter = metrics.membership_config.nodes();
                                let node =
                                    nodes_iter.find(|node| *node.0 == current_leader_node_id);
                                if let Some(node) = node {
                                    Ok(Some(node.1.clone()))
                                } else {
                                    Ok(None)
                                }
                            } else {
                                Ok(None)
                            }
                        } else {
                            Err(TopicRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<super::types::TypeConfig>, TopicRaftError>")]
pub struct DirectWriteToRaft {
    pub command: super::types::Request,
}

impl Handler<DirectWriteToRaft> for TopicRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<ClientWriteResponse<super::types::TypeConfig>, TopicRaftError>,
    >;

    fn handle(&mut self, msg: DirectWriteToRaft, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.client_write(msg.command).await?;
                            Ok(res)
                        } else {
                            Err(TopicRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<RaftMetrics<NodeId,Node>, TopicRaftError>")]
pub struct GetRaftMetrics {}

impl Handler<GetRaftMetrics> for TopicRaftActor {
    type Result = ResponseActFuture<Self, Result<RaftMetrics<NodeId, Node>, TopicRaftError>>;

    fn handle(&mut self, _msg: GetRaftMetrics, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move { Err(TopicRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let metrics_ref = raft_instance.metrics();
                            let metrics = metrics_ref.borrow();
                            Ok(metrics.clone())
                        } else {
                            Err(TopicRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(TopicRaftError::NotReady("Stopped".to_string())) }
                    .into_actor(self),
            ),
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<HashMap<NodeId, Node>, TopicRaftError>")]
pub struct GetClusterNodes;

impl Handler<GetClusterNodes> for TopicRaftActor {
    type Result = Result<HashMap<NodeId, Node>, TopicRaftError>;

    fn handle(&mut self, _msg: GetClusterNodes, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("TopicRaftActor is initializing, message will be queued.");
                Ok(HashMap::new())
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                if let Some(raft_instance) = raft.get() {
                    let metrics_ref = raft_instance.metrics();
                    let metrics = metrics_ref.borrow();
                    let nodes = metrics
                        .membership_config
                        .membership()
                        .nodes()
                        .map(|node| (*node.0, node.1.clone()))
                        .collect();
                    Ok(nodes)
                } else {
                    Ok(HashMap::new())
                }
            }
            ActorState::Failed(e) => Err(TopicRaftError::ServiceUnavailable(e.to_string())),
            ActorState::Stopped => Err(TopicRaftError::NotReady("Actor is stopped".to_string())),
        }
    }
}
