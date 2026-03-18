use std::{
    cell::OnceCell,
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
};

use crate::{
    protobuf::{WriteRequest, cluster_service_client::ClusterServiceClient},
    session::session_actor_map_storage::SessionActorMapEntry,
};
use actix::dev::MessageResponse;
use actix::{
    Actor, AsyncContext, Context, Handler, Message, ResponseActFuture, Supervised, SystemService,
    WrapFuture,
};
use openraft::{
    Config, RaftMetrics,
    error::{ClientWriteError, Fatal, InitializeError, RaftError},
    raft::ClientWriteResponse,
};
use parking_lot::RwLock;

use crate::{
    protobuf::{RaftType, raft_service_client::RaftServiceClient},
    raft::{
        Node, NodeId,
        session_actor_map::{
            SessionActorMapRaft, raft_network_impl::Network, store::new_storage,
            types::SessionActorMapTypeConfig,
        },
    },
    session::session_actor_map_storage::{SessionActorMapStorage, SessionClock, SessionVersion},
};

#[derive(Debug, Clone)]
pub enum ActorState {
    Initializing,
    Running,
    Failed(Box<SessionActorMapRaftError>),
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

    #[error("Tenant not found: {tenant_id}")]
    TenantNotFound { tenant_id: String },
}

pub struct SessionActorMapRaftActor {
    raft: OnceCell<Arc<SessionActorMapRaft>>,
    settings: Option<Arc<crate::settings::Settings>>,
    state: ActorState,
    pending_messages: Vec<Box<dyn std::any::Any + Send>>,
    session_actor_map_storage: OnceCell<Arc<RwLock<SessionActorMapStorage>>>,
    session_clock: Option<Arc<SessionClock>>,
}

impl SessionActorMapRaftActor {
    async fn initialize_raft(
        settings: Arc<crate::settings::Settings>,
        session_clock: Arc<SessionClock>,
    ) -> Result<(SessionActorMapRaft, Arc<RwLock<SessionActorMapStorage>>), SessionActorMapRaftError>
    {
        let raft_config = Config {
            cluster_name: "yedmq_session_actor_map_raft_cluster".to_string(),
            heartbeat_interval: settings.cluster.heartbeat_interval as u64,
            election_timeout_min: (settings.cluster.heartbeat_interval * 5) as u64,
            election_timeout_max: (settings.cluster.heartbeat_interval * 10) as u64,
            ..Default::default()
        };

        let dir = Path::new(&settings.cluster.store_dir);

        let config = Arc::new(raft_config.validate().map_err(|e| {
            SessionActorMapRaftError::ServiceUnavailable(format!("invalid raft config: {e}"))
        })?);

        let session_actor_map_storage = Arc::new(RwLock::new(SessionActorMapStorage::new()));
        let (log_store, state_machine_store) = new_storage(
            &dir,
            session_actor_map_storage.clone(),
            settings.cluster.node_id,
            session_clock.clone(),
            settings.cluster.session_ttl,
        )
        .await
        .map_err(|e| SessionActorMapRaftError::ServiceUnavailable(e.to_string()))?;

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

        Ok((raft, session_actor_map_storage))
    }

    async fn try_local_linearizable_read(
        raft: &SessionActorMapRaft,
    ) -> Result<(), SessionActorMapRaftError> {
        raft.ensure_linearizable().await.map_err(|e| {
            log::error!("Failed to ensure linearizable read: {}", e);
            if let Some(leader) = e.forward_to_leader() {
                SessionActorMapRaftError::NotLeader {
                    leader: leader.leader_node.clone(),
                }
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
            if let RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(
                e_inner,
            )) = e
            {
                SessionActorMapRaftError::NotLeader {
                    leader: e_inner.leader_node,
                }
            } else {
                log::warn!("failed to write command to raft: {}", e);
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
            Ok(r) => match r.data {
                super::types::SessionActorMapResponse::None => Ok(r),
                super::types::SessionActorMapResponse::Rejected {
                    current_version,
                    existing_version,
                } => Err(SessionActorMapRaftError::SessionVersionRejected {
                    current_version,
                    existing_version,
                }),
            },
            Err(SessionActorMapRaftError::NotLeader { leader }) => {
                log::debug!("Not leader, forwarding request to leader: {:?}", leader);
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
        let mut client = RaftServiceClient::connect(format!("http://{}", &leader_addr))
            .await
            .map_err(|e| {
                log::error!("Failed to connect to leader {}", e);
                SessionActorMapRaftError::GRPC(e.to_string())
            })?;

        let data = serde_json::to_string(&msg).map_err(|e| {
            log::error!("Failed to serialize raft write request: {}", e);
            SessionActorMapRaftError::GRPC(format!("serialize raft request failed: {e}"))
        })?;

        let request = WriteRequest {
            data,
            raft_type: RaftType::SessionActorMap.into(),
        };

        let res = client.write(request).await.map_err(|e| {
            log::error!("Failed to send write request to leader: {}", e);
            SessionActorMapRaftError::GRPC(e.to_string())
        })?;

        let inner_res = res.into_inner();
        if inner_res.success {
            let res = inner_res.data;
            serde_json::from_str(&res).map_err(|e| {
                log::error!("Failed to deserialize remote raft write response: {}", e);
                SessionActorMapRaftError::UnexpectedResponseType(format!(
                    "invalid remote raft write response: {e}"
                ))
            })
        } else {
            Err(SessionActorMapRaftError::GRPC(
                inner_res
                    .error
                    .map(|err| err.message)
                    .unwrap_or_else(|| "remote raft write failed without error detail".to_string()),
            ))
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
        Self {
            raft: OnceCell::new(),
            settings: None,
            state: ActorState::Initializing,
            pending_messages: Vec::new(),
            session_actor_map_storage: OnceCell::new(),
            session_clock: None,
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Initialize {
    pub settings: Arc<crate::settings::Settings>,
    pub session_clock: Arc<SessionClock>,
}

impl Handler<Initialize> for SessionActorMapRaftActor {
    type Result = ();

    fn handle(&mut self, msg: Initialize, ctx: &mut Self::Context) -> Self::Result {
        let settings = msg.settings;
        let session_clock = msg.session_clock;
        self.settings = Some(settings.clone());
        self.session_clock = Some(session_clock.clone());

        let addr = ctx.address();
        ctx.spawn(
            async move {
                let raft_instance = Self::initialize_raft(settings, session_clock).await;
                addr.do_send(InitializationComplete(raft_instance));
            }
            .into_actor(self),
        );
    }
}

impl SystemService for SessionActorMapRaftActor {
    fn service_started(&mut self, _ctx: &mut Context<Self>) {}
}

impl Supervised for SessionActorMapRaftActor {}

impl Actor for SessionActorMapRaftActor {
    type Context = Context<Self>;
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
struct InitializationComplete(
    Result<(SessionActorMapRaft, Arc<RwLock<SessionActorMapStorage>>), SessionActorMapRaftError>,
);

impl Handler<InitializationComplete> for SessionActorMapRaftActor {
    type Result = ();

    fn handle(&mut self, msg: InitializationComplete, ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            Ok((raft_instance, session_actor_map_storage)) => {
                let _ = self.raft.set(Arc::new(raft_instance));
                let _ = self
                    .session_actor_map_storage
                    .set(session_actor_map_storage);
                self.state = ActorState::Running;
                log::info!("SessionActorMapActor initialized successfully.");
                self.process_pending_messages(ctx);
            }
            Err(e) => {
                log::error!("Failed to initialize SessionActorMapActor: {}", e);
                self.state = ActorState::Failed(Box::new(e));
            }
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::AppendEntriesResponse<NodeId>, SessionActorMapRaftError>")]
pub struct AppendEntriesRequestMessage {
    pub payload: openraft::raft::AppendEntriesRequest<super::types::SessionActorMapTypeConfig>,
}

impl Handler<AppendEntriesRequestMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<openraft::raft::AppendEntriesResponse<NodeId>, SessionActorMapRaftError>,
    >;

    fn handle(&mut self, msg: AppendEntriesRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
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
                            Err(SessionActorMapRaftError::NotReady(
                                "Initializing".to_string(),
                            ))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }
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
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
                    .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command =
                                super::types::SessionActorMapRequest::SessionLeaseRenewRequest {
                                    sessions: vec![super::types::RenewSession {
                                        tenant_id: msg.tenant_id.clone(),
                                        session_id: msg.client_id.clone(),
                                    }],
                                };
                            Self::handle_raft_write(raft_instance, command).await?;
                            Ok(())
                        } else {
                            Err(SessionActorMapRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionActorMapRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

pub struct GetSessionActorMapStorageResponse {
    pub session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
}
impl<A, M> MessageResponse<A, M> for GetSessionActorMapStorageResponse
where
    A: Actor,
    M: Message<Result = Result<GetSessionActorMapStorageResponse, SessionActorMapRaftError>>,
{
    fn handle(
        self,
        _ctx: &mut <A as Actor>::Context,
        tx: Option<actix::dev::OneshotSender<<M as Message>::Result>>,
    ) {
        if let Some(tx) = tx {
            let _ = tx.send(Ok(self));
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<GetSessionActorMapStorageResponse, SessionActorMapRaftError>")]
pub struct GetSessionActorMapStorage;

impl Handler<GetSessionActorMapStorage> for SessionActorMapRaftActor {
    type Result = Result<GetSessionActorMapStorageResponse, SessionActorMapRaftError>;

    fn handle(
        &mut self,
        _msg: GetSessionActorMapStorage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let session_actor_map_storage = self.session_actor_map_storage.get().cloned().ok_or(
            SessionActorMapRaftError::NotReady("Initializing".to_string()),
        )?;

        Ok(GetSessionActorMapStorageResponse {
            session_actor_map_storage,
        })
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
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
                    .into_actor(self),
                )
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
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionActorMapRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
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
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
                    .into_actor(self),
                )
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
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionActorMapRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
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
    type Result =
        ResponseActFuture<Self, Result<Option<SessionActorMapEntry>, SessionActorMapRaftError>>;

    fn handle(&mut self, msg: GetSessionActorMap, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
                    .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_actor_map_storage = self.session_actor_map_storage.clone();
                Box::pin(
                    async move {
                        if raft.get().is_some() {
                            if let Some(session_actor_map_storage) = session_actor_map_storage.get()
                            {
                                let storage = session_actor_map_storage.read();
                                let entry =
                                    storage.get_session_actor_map(&msg.tenant_id, &msg.client_id);
                                Ok(entry)
                            } else {
                                Err(SessionActorMapRaftError::NotInitialized)
                            }
                        } else {
                            Err(SessionActorMapRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionActorMapRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
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
    type Result =
        ResponseActFuture<Self, Result<Option<SessionActorMapEntry>, SessionActorMapRaftError>>;

    fn handle(
        &mut self,
        msg: GetSessionActorMapLinearizable,
        _: &mut Self::Context,
    ) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
                    .into_actor(self),
                )
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
                                    let storage = session_actor_map_storage.read();
                                    let entry = storage.get_session_actor_map(&msg.tenant_id, &msg.client_id);
                                    Ok(entry)
                                } else {
                                    Err(SessionActorMapRaftError::NotInitialized)
                                }
                            }
                            Err(SessionActorMapRaftError::NotLeader { leader }) => {
                                log::debug!("Not leader, forwarding request to leader: {:?}", leader);
                                if let Some(leader_node) = leader {
                                    let mut client = ClusterServiceClient::connect(format!("http://{}", leader_node.rpc_addr)).await.map_err(|e| {
                                        log::error!("Failed to connect to leader: {}", e);
                                        SessionActorMapRaftError::GRPC(e.to_string())
                                    })?;
                                    let response = client
                                        .get_session_actor_map(crate::protobuf::GetSessionActorMapRequest {
                                            tenant_id: msg.tenant_id.clone(),
                                            client_id: msg.client_id.clone(),
                                        })
                                        .await
                                        .map_err(|e| {
                                            log::error!("Failed to get session actor map from leader: {}", e);
                                            SessionActorMapRaftError::GRPC(e.to_string())
                                        })?;
                                    let inner_res = response.into_inner();
                                    if inner_res.success {
                                        if let Some(payload) = inner_res.payload {
                                            let entry = serde_json::from_str(&payload).map_err(|e| {
                                                SessionActorMapRaftError::UnexpectedResponseType(
                                                    format!("invalid session actor map payload: {e}"),
                                                )
                                            })?;
                                            Ok(Some(entry))
                                        } else {
                                            Ok(None)
                                        }
                                    } else {
                                        Err(SessionActorMapRaftError::GRPC(
                                            inner_res
                                                .error
                                                .map(|err| err.message)
                                                .unwrap_or_else(|| {
                                                    "get session actor map failed without error detail".to_string()
                                                }),
                                        ))
                                    }
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
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionActorMapRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone)]
#[rtype(
    result = "Result<openraft::raft::InstallSnapshotResponse<NodeId>, SessionActorMapRaftError>"
)]
pub struct InstallSnapshotRequestMessage {
    pub payload: openraft::raft::InstallSnapshotRequest<super::types::SessionActorMapTypeConfig>,
}

impl Handler<InstallSnapshotRequestMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<openraft::raft::InstallSnapshotResponse<NodeId>, SessionActorMapRaftError>,
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
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
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
                            Err(SessionActorMapRaftError::NotReady(
                                "Initializing".to_string(),
                            ))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<openraft::raft::VoteResponse<NodeId>, SessionActorMapRaftError>")]
pub struct VoteRequestMessage {
    pub payload: openraft::raft::VoteRequest<NodeId>,
}

impl Handler<VoteRequestMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<openraft::raft::VoteResponse<NodeId>, SessionActorMapRaftError>,
    >;

    fn handle(&mut self, msg: VoteRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
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
                            Err(SessionActorMapRaftError::NotReady(
                                "Initializing".to_string(),
                            ))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<(), SessionActorMapRaftError>")]
pub struct InitRaftClusterMessage {}

impl Handler<InitRaftClusterMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionActorMapRaftError>>;

    fn handle(&mut self, _msg: InitRaftClusterMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
                    .into_actor(self),
                )
            }
            ActorState::Running => {
                if let Some(raft_instance) = self.raft.get() {
                    let Some(settings) = self.settings.as_ref() else {
                        return Box::pin(
                            async move { Err(SessionActorMapRaftError::NotInitialized) }
                                .into_actor(self),
                        );
                    };
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
                        async move {
                            Err(SessionActorMapRaftError::NotReady(
                                "Initializing".to_string(),
                            ))
                        }
                        .into_actor(self),
                    )
                }
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(
    result = "Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>"
)]
pub struct AddLearnerMessage {
    pub node_id: NodeId,
    pub node: Node,
}

impl Handler<AddLearnerMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>,
    >;

    fn handle(&mut self, msg: AddLearnerMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
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
                            Err(SessionActorMapRaftError::NotReady(
                                "Initializing".to_string(),
                            ))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(
    result = "Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>"
)]
pub struct ChangeMembershipMessage {
    pub members: Vec<NodeId>,
}

impl Handler<ChangeMembershipMessage> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>,
    >;

    fn handle(&mut self, msg: ChangeMembershipMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
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
                            Err(SessionActorMapRaftError::NotReady(
                                "Initializing".to_string(),
                            ))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<Option<Node>, SessionActorMapRaftError>")]
pub struct GetLeader {}

impl Handler<GetLeader> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<Node>, SessionActorMapRaftError>>;

    fn handle(&mut self, _msg: GetLeader, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
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
                            Err(SessionActorMapRaftError::NotReady(
                                "Initializing".to_string(),
                            ))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(
    result = "Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>"
)]
pub struct DirectWriteToRaft {
    pub command: super::types::SessionActorMapRequest,
}

impl Handler<DirectWriteToRaft> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<ClientWriteResponse<SessionActorMapTypeConfig>, SessionActorMapRaftError>,
    >;

    fn handle(&mut self, msg: DirectWriteToRaft, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
                    .into_actor(self),
                )
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
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionActorMapRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<RaftMetrics<NodeId,Node>, SessionActorMapRaftError>")]
pub struct GetRaftMetrics {}

impl Handler<GetRaftMetrics> for SessionActorMapRaftActor {
    type Result =
        ResponseActFuture<Self, Result<RaftMetrics<NodeId, Node>, SessionActorMapRaftError>>;

    fn handle(&mut self, _msg: GetRaftMetrics, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionActorMapRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
                    .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            Ok(raft_instance.metrics().borrow().clone())
                        } else {
                            Err(SessionActorMapRaftError::NotReady(
                                "Initializing".to_string(),
                            ))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionActorMapRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<GetClientListWithPaginationResponse, SessionActorMapRaftError>")]
pub struct GetClientListWithPagination {
    pub tenant_id: String,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetClientListWithPaginationResponse {
    pub client_list: Vec<(String, NodeId)>,
    pub total: usize,
}

impl Handler<GetClientListWithPagination> for SessionActorMapRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<GetClientListWithPaginationResponse, SessionActorMapRaftError>,
    >;

    fn handle(&mut self, msg: GetClientListWithPagination, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move {
                        Err(SessionActorMapRaftError::NotReady(
                            "Initializing".to_string(),
                        ))
                    }
                    .into_actor(self),
                )
            }
            ActorState::Running => {
                let session_actor_map_storage = self.session_actor_map_storage.clone();
                Box::pin(
                    async move {
                        if let Some(session_actor_map_storage) = session_actor_map_storage.get() {
                            let storage = session_actor_map_storage.read();
                            let entries = storage.get_client_id_list_with_pagination(
                                &msg.tenant_id,
                                msg.offset,
                                msg.limit,
                            );

                            if let Some(entries) = entries {
                                Ok(GetClientListWithPaginationResponse {
                                    client_list: entries.client_list,
                                    total: entries.total,
                                })
                            } else {
                                Err(SessionActorMapRaftError::TenantNotFound {
                                    tenant_id: msg.tenant_id.clone(),
                                })
                            }
                        } else {
                            Err(SessionActorMapRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionActorMapRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<HashMap<NodeId, Node>, SessionActorMapRaftError>")]
pub struct GetClusterNodes;

impl Handler<GetClusterNodes> for SessionActorMapRaftActor {
    type Result = Result<HashMap<NodeId, Node>, SessionActorMapRaftError>;

    fn handle(&mut self, _msg: GetClusterNodes, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionActorMapRaftActor is initializing, message will be queued.");
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
            ActorState::Failed(e) => {
                Err(SessionActorMapRaftError::ServiceUnavailable(e.to_string()))
            }
            ActorState::Stopped => Err(SessionActorMapRaftError::NotReady(
                "Actor is stopped".to_string(),
            )),
        }
    }
}
