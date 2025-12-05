use std::{cell::OnceCell, collections::BTreeMap, path::Path, sync::Arc};

use actix::{Actor, AsyncContext, Context, Handler, Message, ResponseActFuture, Supervised, SystemService, WrapFuture};
use log::info;
use openraft::{error::{ClientWriteError, Fatal, InitializeError, RaftError}, raft::ClientWriteResponse, Config, RaftMetrics};
use tokio::sync::RwLock;
use crate::{globals, protobuf::{WriteRequest, cluster_service_client::ClusterServiceClient}, session::session_actor_map_storage::SessionActorMapEntry};

use crate::{protobuf::{raft_service_client::RaftServiceClient, RaftType}, raft::{session_actor_map::{raft_network_impl::Network, store::new_storage, types::SessionActorMapTypeConfig, SessionActorMapRaft}, Node, NodeId}, session::session_actor_map_storage::{SessionActorMapStorage, SessionVersion}};


#[derive(Debug, Clone)]
pub enum ActorState {
    Initializing,
    Running,
    Failed(SessionActorMapRaftError),
    Stopped,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum SessionActorMapRaftError {
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

    #[error("Unexpected response type: {0}")]
    UnexpectedResponseType(String),

    #[error("Session version rejected, current: {current_version}, existing: {existing_version}")]
    SessionVersionRejected {
        current_version: SessionVersion,
        existing_version: SessionVersion,
    },
}

pub struct SessionActorMapRaftActor {
    raft: OnceCell<Arc<SessionActorMapRaft>>,
    settings: Arc<crate::settings::Settings>,
    state: ActorState,
    pending_messages: Vec<Box<dyn std::any::Any + Send>>,
    session_actor_map_storage: OnceCell<Arc<RwLock<SessionActorMapStorage>>>,
}


impl SessionActorMapRaftActor {
    async fn initialize_raft(
        settings: Arc<crate::settings::Settings>,
    ) -> Result<(SessionActorMapRaft, Arc<RwLock<SessionActorMapStorage>>), SessionActorMapRaftError> {
        let raft_config = Config {
            cluster_name: "yedmq_session_actor_map_raft_cluster".to_string(),
            ..Default::default()
        };

        let dir = Path::new(&settings.cluster.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let session_clock = globals::get_session_clock();

        let session_actor_map_storage = Arc::new(RwLock::new(SessionActorMapStorage::new()));
        let (log_store, state_machine_store) = new_storage(
            &dir, 
            session_actor_map_storage.clone(),
            settings.cluster.node_id,
            session_clock.clone(),
            settings.cluster.session_ttl,
        ).await;

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
        if !raft.is_initialized().await? {
            raft.initialize(cluster_nodes).await?;
        }
        //

        Ok((raft, session_actor_map_storage))
    }

    async fn try_local_linearizable_read(
        raft: &SessionActorMapRaft,
    ) -> Result<(), SessionActorMapRaftError> {
        raft.ensure_linearizable().await.map_err(|e| {
            log::error!("Failed to ensure linearizable read: {}", e);
            if let Some(leader) = e.forward_to_leader() {
                SessionActorMapRaftError::NotLeader { leader: leader.leader_node.clone() }
            } else {
                SessionActorMapRaftError::NotLeader { leader: None }
            }
        })?;
        Ok(())
    }


    async fn try_local_write(
        raft: &SessionActorMapRaft,
        command: crate::raft::session_actor_map::types::SessionActorMapRequest,
    ) -> Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError> {
        let res = raft.client_write(command).await.map_err(|e| {
            log::error!("Failed to write command to raft: {}", e);
            if let RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(
                e_inner,
            )) = e
            {
                SessionActorMapRaftError::NotLeader {
                    leader: e_inner.leader_node,
                }
            } else {
                SessionActorMapRaftError::RaftClientWriteError(e)
            }
        })?;
        Ok(res)
    }

    async fn handle_raft_write(
        raft: &SessionActorMapRaft,
        request: crate::raft::session_actor_map::types::SessionActorMapRequest,
    ) -> Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError> {
        match Self::try_local_write(raft, request.clone()).await {
            Ok(r) => {
                match r.data {
                    super::types::SessionActorMapResponse::None => Ok(r),
                    super::types::SessionActorMapResponse::Rejected { current_version, existing_version } => {
                        Err(SessionActorMapRaftError::SessionVersionRejected {
                            current_version,
                            existing_version,
                        })
                    },
                }
            },
            Err(SessionActorMapRaftError::NotLeader { leader }) => {
                log::warn!("Not leader, forwarding request to leader: {:?}", leader);
                if let Some(leader_node) = leader {
                    Self::forward_to_leader(leader_node.rpc_addr, request).await
                } else {
                    Err(SessionActorMapRaftError::NoLeaderAvailable)
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
        msg: crate::raft::session_actor_map::types::SessionActorMapRequest,
    ) -> Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError> {
        Self::send_to_remote_actor(current_leader_rpc_addr, msg).await
    }

    async fn send_to_remote_actor(
        leader_addr: String,
        msg: crate::raft::session_actor_map::types::SessionActorMapRequest,
    ) -> Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError> {
        info!("Forwarding request to leader at {}", leader_addr);
        let mut client = RaftServiceClient::connect(format!("http://{}", &leader_addr)).await.map_err(|e| {
            log::error!("Failed to connect to leader {}", e);
            SessionActorMapRaftError::GRPC(e.to_string())
        })?;

        let data = serde_json::to_string(&msg).unwrap();

        let request = WriteRequest {
            data,
            raft_type: RaftType::SessionActorMap.into(),
        };

        let res= client.write(request).await.map_err(|e| {
            log::error!("Failed to send write request to leader: {}", e);
            SessionActorMapRaftError::GRPC(e.to_string())
        })?;

        let inner_res = res.into_inner();
        if inner_res.success {
            let res = inner_res.data;
            Ok(serde_json::from_str(&res).unwrap())
        } else {
            Err(SessionActorMapRaftError::GRPC(inner_res.error.unwrap().message))
        }
    }

    fn process_pending_messages(&mut self, ctx: &mut Context<Self>) {
        for msg in self.pending_messages.drain(..) {
            if let Some(msg) = msg.downcast_ref::<RenewSession>() {
                ctx.notify(msg.clone());
            } else if let Some(msg) = msg.downcast_ref::<RegisterSessionActorMap>() {
                ctx.notify(msg.clone());
            } else if let Some(msg) = msg.downcast_ref::<UnregisterSessionActorMap>() {
                ctx.notify(msg.clone());
            } else {
                log::warn!("Unknown message type in pending messages: {:?}", msg);
            }
        }
    }

}

impl Default for SessionActorMapRaftActor {
    fn default() -> Self {
        let settings = crate::settings::Settings::new().unwrap();
        Self {
            raft: OnceCell::new(),
            settings: Arc::new(settings),
            state: ActorState::Initializing,
            pending_messages: Vec::new(),
            session_actor_map_storage: OnceCell::new(),
        }
    }
}

impl SystemService for SessionActorMapRaftActor {
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

impl Supervised for SessionActorMapRaftActor {}

impl Actor for SessionActorMapRaftActor {
    type Context = Context<Self>;
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
struct InitializationComplete(Result<(SessionActorMapRaft, Arc<RwLock<SessionActorMapStorage>>), SessionActorMapRaftError>);

impl Handler<InitializationComplete> for SessionActorMapRaftActor {
    type Result = ();

    fn handle(&mut self, msg: InitializationComplete, ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            Ok((raft_instance, session_actor_map_storage)) => {
                let _ = self.raft.set(Arc::new(raft_instance));
                let _ = self.session_actor_map_storage.set(session_actor_map_storage);
                self.state = ActorState::Running;
                log::info!("SessionActorMapActor initialized successfully.");
                self.process_pending_messages(ctx);
            }
            Err(e) => {
                log::error!("Failed to initialize SessionActorMapActor: {}", e);
                self.state = ActorState::Failed(e);
            }
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::AppendEntriesResponse<NodeId>, SessionActorMapRaftError>")]
pub struct AppendEntriesRequestMessage {
    pub payload: openraft::raft::AppendEntriesRequest<super::types::SessionActorMapTypeConfig>
}

impl Handler<AppendEntriesRequestMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<openraft::raft::AppendEntriesResponse<NodeId>, SessionActorMapRaftError>>;

    fn handle(&mut self, msg: AppendEntriesRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.append_entries(msg.payload).await?;
                            Ok(res)
                        } else {
                            Err(SessionActorMapRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionActorMapRaftError>")]
pub struct RenewSession {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<RenewSession> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionActorMapRaftError>>;

    fn handle(&mut self, msg: RenewSession, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        let command = super::types::SessionActorMapRequest::SessionLeaseRenewRequest {
                            sessions: vec![
                                super::types::RenewSession {
                                    tenant_id: msg.tenant_id.clone(),
                                    session_id: msg.client_id.clone(),
                                }
                            ]
                        };
                        Self::handle_raft_write(raft_instance, command).await?;
                        Ok(())
                    } else {
                        Err(SessionActorMapRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionActorMapRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionActorMapRaftError>")]
pub struct RegisterSessionActorMap {
    pub tenant_id: String,
    pub client_id: String,
    pub node_id: NodeId,
    pub version: SessionVersion,
}

impl Handler<RegisterSessionActorMap> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionActorMapRaftError>>;

    fn handle(&mut self, msg: RegisterSessionActorMap, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        let command = super::types::SessionActorMapRequest::RegisterSession { 
                            tenant_id: msg.tenant_id.clone(),
                            session_id: msg.client_id.clone(),
                            node_id: msg.node_id,
                            version: msg.version,
                        };
                        Self::handle_raft_write(raft_instance, command).await?;
                        Ok(())
                    } else {
                        Err(SessionActorMapRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionActorMapRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}


#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionActorMapRaftError>")]
pub struct UnregisterSessionActorMap {
    pub tenant_id: String,
    pub client_id: String,
    pub version: SessionVersion,
}

impl Handler<UnregisterSessionActorMap> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionActorMapRaftError>>;

    fn handle(&mut self, msg: UnregisterSessionActorMap, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        let command = super::types::SessionActorMapRequest::UnregisterSession { 
                            tenant_id: msg.tenant_id.clone(),
                            session_id: msg.client_id.clone(),
                            session_version: msg.version,
                        };
                        Self::handle_raft_write(raft_instance, command).await?;
                        Ok(())
                    } else {
                        Err(SessionActorMapRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionActorMapRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}


#[derive(Message, Clone)]
#[rtype(result = "Result<Option<SessionActorMapEntry>, SessionActorMapRaftError>")]
pub struct GetSessionActorMap {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<GetSessionActorMap> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<SessionActorMapEntry>, SessionActorMapRaftError>>;

    fn handle(&mut self, msg: GetSessionActorMap, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_actor_map_storage = self.session_actor_map_storage.clone();
                Box::pin(
                async move {
                    if raft.get().is_some() {
                        if let Some(session_actor_map_storage) = session_actor_map_storage.get() {
                            let storage = session_actor_map_storage.read().await;
                            let entry = storage.get_session_actor_map(&msg.tenant_id, &msg.client_id);
                            Ok(entry)
                        } else {
                            Err(SessionActorMapRaftError::NotInitialized)
                        }
                    } else {
                        Err(SessionActorMapRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionActorMapRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}


#[derive(Message, Clone)]
#[rtype(result = "Result<Option<SessionActorMapEntry>, SessionActorMapRaftError>")]
pub struct GetSessionActorMapLinearizable {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<GetSessionActorMapLinearizable> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<SessionActorMapEntry>, SessionActorMapRaftError>>;

    fn handle(&mut self, msg: GetSessionActorMapLinearizable, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_actor_map_storage = self.session_actor_map_storage.clone();
                Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        match Self::try_local_linearizable_read(raft_instance).await {
                            Ok(_) => {
                                if let Some(session_actor_map_storage) = session_actor_map_storage.get() {
                                    let storage = session_actor_map_storage.read().await;
                                    let entry = storage.get_session_actor_map(&msg.tenant_id, &msg.client_id);
                                    Ok(entry)
                                } else {
                                    Err(SessionActorMapRaftError::NotInitialized)
                                }
                            }
                            Err(SessionActorMapRaftError::NotLeader { leader }) => {
                                log::warn!("Not leader, forwarding request to leader: {:?}", leader);
                                if let Some(leader_node) = leader {
                                    let mut client = ClusterServiceClient::connect(format!("http://{}", leader_node.rpc_addr)).await.map_err(|e| {
                                        log::error!("Failed to connect to leader: {}", e);
                                        SessionActorMapRaftError::GRPC(e.to_string())
                                    })?;
                                    client.get_session_actor_map(
                                        crate::protobuf::GetSessionActorMapRequest {
                                            tenant_id: msg.tenant_id.clone(),
                                            client_id: msg.client_id.clone(),
                                        }
                                    ).await.map_err(|e| {
                                        log::error!("Failed to get session actor map from leader: {}", e);
                                        SessionActorMapRaftError::GRPC(e.to_string())
                                    }).and_then(|res| {
                                        let inner_res = res.into_inner();
                                        if inner_res.success {
                                            if inner_res.payload.is_none() {
                                                Ok(None)
                                            } else {
                                                let entry = serde_json::from_str(&inner_res.payload.unwrap()).unwrap();
                                                Ok(Some(entry))
                                            }
                                        } else {
                                            Err(SessionActorMapRaftError::GRPC(inner_res.error.unwrap().message))
                                        }
                                    })
                                } else {
                                    Err(SessionActorMapRaftError::NoLeaderAvailable)
                                }
                            }
                            Err(e) => Err(e),
                        }
                    } else {
                        Err(SessionActorMapRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();                
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionActorMapRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::InstallSnapshotResponse<NodeId>, SessionActorMapRaftError>")]
pub struct InstallSnapshotRequestMessage {
    pub payload: openraft::raft::InstallSnapshotRequest<super::types::SessionActorMapTypeConfig>
}

impl Handler<InstallSnapshotRequestMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<openraft::raft::InstallSnapshotResponse<NodeId>, SessionActorMapRaftError>>; 

    fn handle(&mut self, msg: InstallSnapshotRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.install_snapshot(msg.payload).await?;
                            Ok(res)
                        } else {
                            Err(SessionActorMapRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}


#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::VoteResponse<NodeId>, SessionActorMapRaftError>")]
pub struct VoteRequestMessage {
    pub payload: openraft::raft::VoteRequest<NodeId>
}

impl Handler<VoteRequestMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<openraft::raft::VoteResponse<NodeId>, SessionActorMapRaftError>>;

    fn handle(&mut self, msg: VoteRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.vote(msg.payload).await?;
                            Ok(res)
                        } else {
                            Err(SessionActorMapRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}


#[derive(Message)]
#[rtype(result = "Result<(), SessionActorMapRaftError>")]
pub struct InitRaftClusterMessage {}


impl Handler<InitRaftClusterMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionActorMapRaftError>>;

    fn handle(&mut self, _msg: InitRaftClusterMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                if let Some(raft_instance) = self.raft.get(){
                    let mut cluster_nodes = BTreeMap::new();
                    for item in self.settings.cluster.nodes.iter() {
                        cluster_nodes.insert(
                            item.id,
                            Node {
                                rpc_addr: item.rpc_address.to_string(),
                                api_addr: item.api_address.to_string(),
                            },
                        );
                    }
                    let raft = raft_instance.clone();
                    Box::pin(async move {
                        raft.initialize(cluster_nodes).await?;
                        Ok(())
                    }.into_actor(self))
                } else {
                    Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
                }
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>")]
pub struct AddLearnerMessage {
    pub node_id: NodeId,
    pub node: Node,
}

impl Handler<AddLearnerMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>>;

    fn handle(&mut self, msg: AddLearnerMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.add_learner(msg.node_id, msg.node, true).await?;
                            Ok(res)
                        } else {
                            Err(SessionActorMapRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>")]
pub struct ChangeMembershipMessage {
    pub members: Vec<NodeId>
}

impl Handler<ChangeMembershipMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>>;

    fn handle(&mut self, msg: ChangeMembershipMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.change_membership(msg.members, true).await?;
                            Ok(res)
                        } else {
                            Err(SessionActorMapRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<Node>, SessionActorMapRaftError>")]

pub struct GetLeader{}

impl Handler<GetLeader> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<Node>, SessionActorMapRaftError>>;

    fn handle(&mut self, _msg: GetLeader, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let current_leader_node_id = raft_instance.current_leader().await;
                            if let Some(current_leader_node_id) =  current_leader_node_id {
                                let metrics_ref = raft_instance.metrics();
                                let metrics = metrics_ref.borrow();
                                let mut nodes_iter = metrics.membership_config.nodes();
                                let node = nodes_iter.find(|node| *node.0 == current_leader_node_id);
                                if let Some(node) = node {
                                    Ok(Some(node.1.clone()))
                                } else {
                                    Ok(None)
                                }
                            } else {
                                Ok(None)
                            }
                        } else {
                            Err(SessionActorMapRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>")]
pub struct DirectWriteToRaft {
    pub command: super::types::SessionActorMapRequest,
}

impl Handler<DirectWriteToRaft> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>>;
    
    fn handle(&mut self, msg: DirectWriteToRaft, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        let res = Self::handle_raft_write(raft_instance, msg.command).await?;
                        Ok(res)
                    } else {
                        Err(SessionActorMapRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionActorMapRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}




#[derive(Message)]
#[rtype(result = "Result<RaftMetrics<NodeId,Node>, SessionActorMapRaftError>")]
pub struct GetRaftMetrics{}

impl Handler<GetRaftMetrics> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<RaftMetrics<NodeId,Node>, SessionActorMapRaftError>>;

    fn handle(&mut self, _msg: GetRaftMetrics, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionActorMapRaftActor is initializing, message will be queued.");
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            Ok(raft_instance.metrics().borrow().clone())
                        } else {
                            Err(SessionActorMapRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }       
    }
}

