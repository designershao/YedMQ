use std::{cell::OnceCell, path::Path, sync::Arc};

use actix::{Actor, AsyncContext, Context, Handler, Message, ResponseActFuture, Supervised, SystemService, WrapFuture};
use openraft::{error::{ClientWriteError, RaftError}, raft::ClientWriteResponse, Config};
use tokio::sync::RwLock;
use yedmq_mqtt::MqttPacketV3;

use crate::{protobuf::{ raft_service_client::RaftServiceClient, AppendEntriesRequest, RaftType}, raft::{session_state::{raft_network_impl::Network, store::new_storage, types::{SessionStateRequest, SessionStateTypeConfig}, SessionStateRaft}, Node, NodeId}, session::session_state_storage::{SessionState, SessionStateStorage}, settings::Session};


#[derive(Debug, Clone)]
pub enum ActorState {
    Initializing,
    Running,
    Failed(SessionStateRaftError),
    Stopped,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum SessionStateRaftError {
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

    #[error("Unexpected response type: {0}")]
    UnexpectedResponseType(String),
}

pub struct SessionStateRaftActor {
    raft: OnceCell<Arc<SessionStateRaft>>,
    settings: Arc<crate::settings::Settings>,
    state: ActorState,
    pending_messages: Vec<Box<dyn std::any::Any + Send>>,
    session_state_storage: OnceCell<Arc<RwLock<SessionStateStorage>>>
}


impl SessionStateRaftActor {
    async fn initialize_raft(
        settings: Arc<crate::settings::Settings>,
    ) -> Result<(SessionStateRaft, Arc<RwLock<SessionStateStorage>>), SessionStateRaftError> {
        let raft_config = Config {
            cluster_name: "yedmq_topic_cluster".to_string(),
            ..Default::default()
        };

        let dir = Path::new(&settings.cluster.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let session_state_storage = Arc::new(RwLock::new(SessionStateStorage::new()));

        let (log_store, state_machine_store) = new_storage(&dir, session_state_storage.clone()).await;

        let network = Network {};

        let raft = openraft::Raft::new(
            settings.cluster.node_id,
            config.clone(),
            network,
            log_store,
            state_machine_store,
        )
        .await
        .map_err(|e| SessionStateRaftError::RaftInitializationError(e.to_string()))?;
        Ok((raft, session_state_storage))
    }

    async fn try_local_linearizable_read(
        raft: &SessionStateRaft,
    ) -> Result<(), SessionStateRaftError> {
        raft.ensure_linearizable().await.map_err(|e| {
            log::error!("Failed to ensure linearizable read: {}", e);
            if let Some(leader) = e.forward_to_leader() {
                SessionStateRaftError::NotLeader { leader: leader.leader_node.clone() }
            } else {
                SessionStateRaftError::NotLeader { leader: None }
            }
        })?;
        Ok(())
    }

    async fn try_local_write(
        raft: &SessionStateRaft,
        command: crate::raft::session_state::types::SessionStateRequest,
    ) -> Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError> {
        let res = raft.client_write(command).await.map_err(|e| {
            log::error!("Failed to write command to raft: {}", e);
            if let RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(
                e_inner,
            )) = e
            {
                SessionStateRaftError::NotLeader {
                    leader: e_inner.leader_node,
                }
            } else {
                SessionStateRaftError::Raft(e)
            }
        })?;
        Ok(res)
    }

    async fn handle_raft_write(
        raft: &SessionStateRaft,
        request: crate::raft::session_state::types::SessionStateRequest,
    ) -> Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError> {
        match Self::try_local_write(raft, request.clone()).await {
            Ok(r) => Ok(r),
            Err(SessionStateRaftError::NotLeader { leader }) => {
                log::warn!("Not leader, forwarding request to leader: {:?}", leader);
                if let Some(leader_node) = leader {
                    Self::forward_to_leader(leader_node.rpc_addr, request).await
                } else {
                    Err(SessionStateRaftError::NoLeaderAvailable)
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
        msg: crate::raft::session_state::types::SessionStateRequest,
    ) -> Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError> {
        Self::send_to_remote_actor(current_leader_rpc_addr, msg).await
    }

    async fn send_to_remote_actor(
        leader_addr: String,
        msg: crate::raft::session_state::types::SessionStateRequest,
    ) -> Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError> {
        let mut client = RaftServiceClient::connect(leader_addr).await.map_err(|e| {
            log::error!("Failed to connect to leader {}", e);
            SessionStateRaftError::GRPC(e.to_string())
        })?;

        let data = serde_json::to_string(&msg).unwrap();

        let request = AppendEntriesRequest {
            data,
            raft_type: RaftType::Topic.into(),
        };

        let res= client.append_entries(request).await.map_err(|e| {
            log::error!("Failed to send append entries request to leader: {}", e);
            SessionStateRaftError::GRPC(e.to_string())
        })?;

        let inner_res = res.into_inner();
        if inner_res.success {
            let res = inner_res.data;
            Ok(serde_json::from_str(&res).unwrap())
        } else {
            Err(SessionStateRaftError::GRPC(inner_res.error.unwrap().message))
        }
    }

    fn process_pending_messages(&mut self, ctx: &mut Context<Self>) {
        for msg in self.pending_messages.drain(..) {
        }
    }

}

impl Default for SessionStateRaftActor {
    fn default() -> Self {
        let settings = crate::settings::Settings::default();
        Self {
            raft: OnceCell::new(),
            settings: Arc::new(settings),
            state: ActorState::Initializing,
            pending_messages: Vec::new(),
            session_state_storage: OnceCell::new(),
        }
    }
}

impl SystemService for SessionStateRaftActor {
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

impl Supervised for SessionStateRaftActor {}

impl Actor for SessionStateRaftActor {
    type Context = Context<Self>;
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
struct InitializationComplete(Result<(SessionStateRaft, Arc<RwLock<SessionStateStorage>>), SessionStateRaftError>);

impl Handler<InitializationComplete> for SessionStateRaftActor {
    type Result = ();

    fn handle(&mut self, msg: InitializationComplete, ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            Ok((raft_instance, session_state_storage)) => {
                let _ = self.raft.set(Arc::new(raft_instance));
                let _ = self.session_state_storage.set(session_state_storage);
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

#[derive(Message)]
#[rtype(result="Result<(), SessionStateRaftError>")]
pub struct StoreOfflineMessage {
    pub tenant_id: String,
    pub client_id: String,
    pub packets: MqttPacketV3
}

impl Handler<StoreOfflineMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: StoreOfflineMessage, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                return Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self));
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                return Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        let command = SessionStateRequest::AppendToPendingQueue { 
                            tenant_id: msg.tenant_id, 
                            client_id: msg.client_id, 
                            packet: msg.packets 
                        };
                        Self::handle_raft_write(raft_instance, command).await?;
                        Ok(())
                    } else {
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                );
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                return Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                );
            }
            ActorState::Stopped => {
                return Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                );
            }            
        }
    }
}


#[derive(Message)]
#[rtype(result="Result<Option<MqttPacketV3>, SessionStateRaftError>")]
pub struct PopOfflineMessage {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<PopOfflineMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<MqttPacketV3>, SessionStateRaftError>>;

    fn handle(&mut self, msg: PopOfflineMessage, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                return Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self));
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                return Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        let command = SessionStateRequest::PopFromPendingQueue { 
                            tenant_id: msg.tenant_id, 
                            client_id: msg.client_id 
                        };
                        let res = Self::handle_raft_write(raft_instance, command).await?;
                        match res.data {
                            super::types::SessionStateResponse::PopFromPendingQueueResult(mqtt_packet_v3) => {
                                Ok(mqtt_packet_v3)
                            },
                            _ => {
                                Err(SessionStateRaftError::UnexpectedResponseType(
                                    "pop from pending queue error, unexpected response".to_string(),
                                ))
                            }
                        }
                    } else {
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                );
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                return Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                );
            }
            ActorState::Stopped => {
                return Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                );
            }            
        }
    }
}