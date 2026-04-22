use std::{
    cell::OnceCell,
    collections::BTreeMap,
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use actix::{
    Actor, AsyncContext, Context, Handler, Message, ResponseActFuture, Supervised, SystemService,
    WrapFuture,
};
use openraft::{
    error::{ClientWriteError, Fatal, InitializeError, RaftError},
    raft::ClientWriteResponse,
    Config, RaftMetrics, StorageError,
};
use tokio::sync::RwLock;
use yedmq_mqtt::MqttPacketV3;

use crate::{
    inflight::InflightError,
    protobuf::{
        cluster_service_client::ClusterServiceClient, raft_service_client::RaftServiceClient,
        RaftType, WriteRequest,
    },
    raft::{
        session_state::{
            raft_network_impl::Network,
            store::new_storage,
            types::{SessionStateRequest, SessionStateResponse, SessionStateTypeConfig},
            SessionStateRaft,
        },
        GRPCBusinessError, Node, NodeId,
    },
    rpc::grpc_status,
    session::session_state_storage::{SessionState, SessionStateStorage, SessionStateStorageError},
};

#[derive(Debug, Clone)]
pub enum ActorState {
    Initializing,
    Running,
    Failed(Box<SessionStateRaftError>),
    Stopped,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum SessionStateRaftError {
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

    #[error("Raft storage error: {0}")]
    RaftStorageError(#[from] StorageError<NodeId>),

    #[error("gRPC error: {0}")]
    GRPC(#[from] tonic::Status),

    #[error("gRPC connect error: {0}")]
    GRPCConnect(String),

    #[error("gRPC business error: {0}")]
    GRPCBusiness(GRPCBusinessError),

    #[error("Serialization error: {0}")]
    Serialize(String),

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

    #[error("Session state not existed: {0}")]
    SessionStateNotExisted(String),

    #[error("Packet identifier has existed: {0}")]
    InflightError(#[from] InflightError),
}

pub struct SessionStateRaftActor {
    raft: OnceCell<Arc<SessionStateRaft>>,
    settings: Option<Arc<crate::settings::Settings>>,
    payload_store: OnceCell<Arc<dyn crate::raft::payload::PayloadStore>>,
    state: ActorState,
    pending_messages: Vec<Box<dyn std::any::Any + Send>>,
    session_state_storage: OnceCell<Arc<RwLock<SessionStateStorage>>>,
    is_ready: Arc<std::sync::atomic::AtomicBool>,
}

impl Default for SessionStateRaftActor {
    fn default() -> Self {
        Self {
            raft: OnceCell::new(),
            settings: None,
            payload_store: OnceCell::new(),
            state: ActorState::Initializing,
            pending_messages: Vec::new(),
            session_state_storage: OnceCell::new(),
            is_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Initialize {
    pub settings: Arc<crate::settings::Settings>,
    pub payload_store: Arc<dyn crate::raft::payload::PayloadStore>,
}

impl Handler<Initialize> for SessionStateRaftActor {
    type Result = ();

    fn handle(&mut self, msg: Initialize, ctx: &mut Self::Context) -> Self::Result {
        self.settings = Some(msg.settings.clone());
        let _ = self.payload_store.set(msg.payload_store.clone());
        let settings = msg.settings.clone();
        let payload_store = msg.payload_store.clone();
        let addr = ctx.address();

        ctx.spawn(
            async move {
                let raft_instance = Self::initialize_raft(settings, payload_store).await;
                addr.do_send(InitializationComplete(raft_instance));
            }
            .into_actor(self),
        );
    }
}

impl SystemService for SessionStateRaftActor {
    fn service_started(&mut self, _ctx: &mut Context<Self>) {}
}

impl Supervised for SessionStateRaftActor {}

impl SessionStateRaftActor {
    fn leader_node_from_status(parsed: &grpc_status::ParsedStatus) -> Option<Node> {
        parsed.leader_addr.clone().map(|rpc_addr| Node {
            node_id: parsed.leader_node_id,
            rpc_addr,
            api_addr: String::new(),
        })
    }

    fn map_remote_business_error(
        grpc_code: tonic::Code,
        detail: crate::protobuf::ErrorDetail,
    ) -> SessionStateRaftError {
        match detail.code() {
            crate::protobuf::ErrorCode::SessionStateNotFound => {
                SessionStateRaftError::SessionStateNotExisted(detail.message)
            }
            crate::protobuf::ErrorCode::PacketIdentifierAlreadyExists => {
                SessionStateRaftError::InflightError(InflightError::PacketIdentifierHasExisted)
            }
            _ => SessionStateRaftError::GRPCBusiness(GRPCBusinessError::new(grpc_code, detail)),
        }
    }

    fn map_remote_status(status: tonic::Status) -> SessionStateRaftError {
        let parsed = grpc_status::decode_status(&status);

        if parsed.error_kind.as_deref() == Some(grpc_status::ERROR_KIND_LEADER_REDIRECT) {
            return SessionStateRaftError::NotLeader {
                leader: Self::leader_node_from_status(&parsed),
            };
        }

        if parsed.error_kind.as_deref() == Some(grpc_status::ERROR_KIND_BUSINESS) {
            if let Some(detail) = parsed.detail {
                return Self::map_remote_business_error(status.code(), detail);
            }
        }

        if parsed.error_kind.as_deref() == Some(grpc_status::ERROR_KIND_NO_LEADER) {
            return SessionStateRaftError::NoLeaderAvailable;
        }

        if parsed.error_kind.as_deref() == Some(grpc_status::ERROR_KIND_NOT_READY) {
            return SessionStateRaftError::NotReady(status.message().to_string());
        }

        match status.code() {
            tonic::Code::Unavailable => {
                SessionStateRaftError::ServiceUnavailable(status.message().to_string())
            }
            _ => SessionStateRaftError::GRPC(status),
        }
    }

    async fn initialize_raft(
        settings: Arc<crate::settings::Settings>,
        payload_store: Arc<dyn crate::raft::payload::PayloadStore>,
    ) -> Result<
        (
            SessionStateRaft,
            Arc<RwLock<SessionStateStorage>>,
            Arc<std::sync::atomic::AtomicBool>,
        ),
        SessionStateRaftError,
    > {
        let raft_config = Config {
            cluster_name: "yedmq_session_state_raft_cluster".to_string(),
            heartbeat_interval: settings.cluster.heartbeat_interval as u64,
            election_timeout_min: (settings.cluster.heartbeat_interval * 5) as u64,
            election_timeout_max: (settings.cluster.heartbeat_interval * 10) as u64,
            ..Default::default()
        };

        let dir = Path::new(&settings.cluster.store_dir);

        let config = Arc::new(raft_config.validate().map_err(|e| {
            SessionStateRaftError::ServiceUnavailable(format!("invalid raft config: {}", e))
        })?);

        let session_state_storage = Arc::new(RwLock::new(SessionStateStorage::new()));

        let (log_store, state_machine_store, is_ready) = new_storage(
            &dir,
            session_state_storage.clone(),
            payload_store.clone(),
            settings.clone(),
        )
        .await?;

        let network = Network::new(payload_store);

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
                    node_id: Some(item.id),
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

        Ok((raft, session_state_storage, is_ready))
    }

    async fn try_local_linearizable_read(
        raft: &SessionStateRaft,
    ) -> Result<(), SessionStateRaftError> {
        match raft.ensure_linearizable().await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(leader) = e.forward_to_leader() {
                    Err(SessionStateRaftError::NotLeader {
                        leader: leader.leader_node.clone(),
                    })
                } else {
                    log::error!("Failed to ensure linearizable read: {}", e);
                    Err(SessionStateRaftError::NotLeader { leader: None })
                }
            }
        }
    }

    async fn try_local_write(
        raft: &SessionStateRaft,
        command: crate::raft::session_state::types::SessionStateRequest,
    ) -> Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError> {
        let res = raft.client_write(command).await.map_err(|e| {
            if let RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(
                e_inner,
            )) = e
            {
                SessionStateRaftError::NotLeader {
                    leader: e_inner.leader_node,
                }
            } else {
                log::warn!("failed to write command to raft: {}", e);
                SessionStateRaftError::RaftClientWriteError(e)
            }
        })?;
        Ok(res)
    }

    async fn handle_raft_write(
        raft: &SessionStateRaft,
        request: crate::raft::session_state::types::SessionStateRequest,
        payload_store: Option<Arc<dyn crate::raft::payload::PayloadStore>>,
    ) -> Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError> {
        match Self::try_local_write(raft, request.clone()).await {
            Ok(r) => match r.data {
                SessionStateResponse::InflightRegisterRxPacketResponse(Err(
                    SessionStateStorageError::InflightError(e),
                )) => Err(SessionStateRaftError::InflightError(e)),
                SessionStateResponse::InflightRegisterTxPacketResponse(Err(
                    SessionStateStorageError::SessionStateNotExisted { client_id },
                )) => Err(SessionStateRaftError::SessionStateNotExisted(format!(
                    "Session state not existed for client_id: {}",
                    client_id
                ))),
                _ => Ok(r),
            },
            Err(SessionStateRaftError::NotLeader { leader }) => {
                log::debug!("Not leader, forwarding request to leader: {:?}", leader);
                if let Some(leader_node) = leader {
                    if let Some(store) = payload_store {
                        let key = match &request {
                            SessionStateRequest::InflightRegisterRxPacket {
                                packet_key, ..
                            } => Some(packet_key),
                            SessionStateRequest::InflightRegisterTxPacket {
                                packet_key, ..
                            } => Some(packet_key),
                            SessionStateRequest::AppendToPendingQueue { packet_key, .. } => {
                                Some(packet_key)
                            }
                            _ => None,
                        };

                        if let Some(k) = key {
                            log::info!(
                                "Forwarding: pushing payload key {} to leader {}",
                                k,
                                leader_node.rpc_addr
                            );
                            let client = crate::raft::payload::PayloadClient::new(store);
                            let metrics_rx = raft.metrics();
                            let term = metrics_rx.borrow().vote.leader_id.term;
                            if let Err(e) = client
                                .replicate(&leader_node.rpc_addr, k.clone(), term)
                                .await
                            {
                                log::error!(
                                    "CRITICAL: Failed to push payload {} to leader {} before forwarding: {}",
                                    k,
                                    leader_node.rpc_addr,
                                    e
                                );
                                return Err(SessionStateRaftError::ServiceUnavailable(format!(
                                    "push payload error {}",
                                    e
                                )));
                            }
                            log::info!("Forwarding: payload key {} pushed successfully", k);
                        }
                    }
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
        let mut client = crate::rpc::grpc_client::lazy_channel(&leader_addr)
            .map(RaftServiceClient::new)
            .map_err(|e| SessionStateRaftError::GRPCConnect(e.to_string()))?;

        let data = serde_json::to_string(&msg)
            .map_err(|e| SessionStateRaftError::Serialize(e.to_string()))?;

        let request = WriteRequest {
            data,
            raft_type: RaftType::SessionState.into(),
        };

        let res = client
            .write(request)
            .await
            .map_err(Self::map_remote_status)?;

        let inner_res = res.into_inner();
        serde_json::from_str(&inner_res.data)
            .map_err(|e| SessionStateRaftError::Serialize(e.to_string()))
    }

    fn process_pending_messages(&mut self, ctx: &mut Context<Self>) {
        for msg in self.pending_messages.drain(..) {
            if let Some(msg) = msg.downcast_ref::<StoreOfflineMessage>() {
                ctx.notify(msg.clone());
            } else if let Some(msg) = msg.downcast_ref::<PopOfflineMessage>() {
                ctx.notify(msg.clone());
            } else if let Some(msg) = msg.downcast_ref::<CreateSessionState>() {
                ctx.notify(msg.clone());
            } else if let Some(msg) = msg.downcast_ref::<RegisterInflightRxPacket>() {
                ctx.notify(msg.clone());
            } else if let Some(msg) = msg.downcast_ref::<RegisterInflightTxPacket>() {
                ctx.notify(msg.clone());
            } else if let Some(msg) = msg.downcast_ref::<AdvanceInflightState>() {
                ctx.notify(msg.clone());
            } else if let Some(msg) = msg.downcast_ref::<DeleteSessionState>() {
                ctx.notify(msg.clone());
            } else if let Some(msg) = msg.downcast_ref::<SubscribeTopic>() {
                ctx.notify(msg.clone());
            } else if let Some(msg) = msg.downcast_ref::<UnsubscribeTopic>() {
                ctx.notify(msg.clone());
            } else {
                log::warn!("Unknown pending message type: {:?}", msg);
            }
        }
    }
}

impl Actor for SessionStateRaftActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(1), |_, ctx| {
            ctx.address().do_send(PerformIntegrityCheck {});
        });
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct PerformIntegrityCheck {}

impl Handler<PerformIntegrityCheck> for SessionStateRaftActor {
    type Result = ();

    fn handle(&mut self, _msg: PerformIntegrityCheck, ctx: &mut Self::Context) -> Self::Result {
        if !matches!(self.state, ActorState::Running) {
            return;
        }

        let raft = self.raft.get().cloned();
        let storage = self.session_state_storage.get().cloned();
        let payload_store = self.payload_store.get().cloned();
        let is_ready_flag = self.is_ready.clone();

        if let (Some(raft), Some(storage), Some(payload_store)) = (raft, storage, payload_store) {
            ctx.spawn(
                async move {
                    let active_keys = {
                        let s = storage.read().await;
                        s.ref_counts.keys().cloned().collect::<Vec<String>>()
                    };

                    let mut missing = Vec::new();
                    for key in active_keys {
                        if !payload_store.contains(&key).await.unwrap_or(false) {
                            missing.push(key);
                        }
                    }

                    if missing.is_empty() {
                        if !is_ready_flag.load(Ordering::SeqCst) {
                            log::info!(
                                "Integrity check passed: all payloads present. Node is now READY."
                            );
                            is_ready_flag.store(true, Ordering::SeqCst);
                        }
                    } else {
                        if is_ready_flag.load(Ordering::SeqCst) {
                            log::warn!(
                                "Integrity check failed: {} payloads missing. Node is NOT READY.",
                                missing.len()
                            );
                            is_ready_flag.store(false, Ordering::SeqCst);
                        }

                        // Try to sync missing payloads from leader
                        if let Some(leader_id) = raft.current_leader().await {
                            let metrics_rx = raft.metrics();
                            let metrics = metrics_rx.borrow();
                            let leader_node = metrics
                                .membership_config
                                .nodes()
                                .find(|(id, _)| **id == leader_id)
                                .map(|(_, n)| n);
                            if let Some(node) = leader_node {
                                log::info!(
                                    "Syncing {} missing payloads from leader node {}",
                                    missing.len(),
                                    leader_id
                                );
                                let client =
                                    crate::raft::payload::PayloadClient::new(payload_store);
                                let manifest = missing.into_iter().map(|k| (k, 0, 0)).collect();
                                if let Err(e) = client.bulk_sync(&node.rpc_addr, manifest).await {
                                    log::warn!(
                                        "Failed to sync missing payloads from leader: {}",
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
                .into_actor(self),
            );
        }
    }
}

type InitializationResult = Result<
    (
        SessionStateRaft,
        Arc<RwLock<SessionStateStorage>>,
        Arc<std::sync::atomic::AtomicBool>,
    ),
    SessionStateRaftError,
>;

#[derive(Message, Clone)]
#[rtype(result = "()")]
struct InitializationComplete(InitializationResult);

impl Handler<InitializationComplete> for SessionStateRaftActor {
    type Result = ();

    fn handle(&mut self, msg: InitializationComplete, ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            Ok((raft_instance, session_state_storage, is_ready)) => {
                let _ = self.raft.set(Arc::new(raft_instance));
                let _ = self.session_state_storage.set(session_state_storage);
                self.is_ready = is_ready;
                self.state = ActorState::Running;
                log::info!("SessionStateRaftActor initialized successfully.");
                self.process_pending_messages(ctx);
            }
            Err(e) => {
                log::error!("Failed to initialize SessionStateRaftActor: {}", e);
                self.state = ActorState::Failed(Box::new(e));
            }
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct StoreOfflineMessage {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_key: String,
}

impl Handler<StoreOfflineMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: StoreOfflineMessage, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::AppendToPendingQueue {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                                packet_key: msg.packet_key,
                            };
                            Self::handle_raft_write(raft_instance, command, payload_store).await?;
                            Ok(())
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(Option<String>, Option<Vec<u8>>), SessionStateRaftError>")]
pub struct PopOfflineMessage {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<PopOfflineMessage> for SessionStateRaftActor {
    type Result =
        ResponseActFuture<Self, Result<(Option<String>, Option<Vec<u8>>), SessionStateRaftError>>;

    fn handle(&mut self, msg: PopOfflineMessage, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::PopFromPendingQueue {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                            };
                            let res =
                                Self::handle_raft_write(raft_instance, command, payload_store)
                                    .await?;
                            match res.data {
                                super::types::SessionStateResponse::PopFromPendingQueueResult(
                                    packet_key,
                                    payload,
                                ) => Ok((packet_key, payload)),
                                _ => Err(SessionStateRaftError::UnexpectedResponseType(
                                    "pop from pending queue error, unexpected response".to_string(),
                                )),
                            }
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<SessionState>, SessionStateRaftError>")]
pub struct GetSessionState {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<GetSessionState> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<SessionState>, SessionStateRaftError>>;

    fn handle(&mut self, msg: GetSessionState, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_state_storage = self.session_state_storage.clone();
                Box::pin(
                    async move {
                        if raft.get().is_some() {
                            if let Some(session_state_storage) = session_state_storage.get() {
                                let session_state_storage = session_state_storage.read().await;
                                let session_state = session_state_storage
                                    .get_session_state(&msg.tenant_id, &msg.client_id)
                                    .await;
                                if let Some(session_state) = session_state {
                                    Ok(Some(session_state.read().await.clone()))
                                } else {
                                    Ok(None)
                                }
                            } else {
                                Err(SessionStateRaftError::NotInitialized)
                            }
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<SessionState>, SessionStateRaftError>")]
pub struct GetSessionStateEnsureLinearizable {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<GetSessionStateEnsureLinearizable> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<SessionState>, SessionStateRaftError>>;

    fn handle(
        &mut self,
        msg: GetSessionStateEnsureLinearizable,
        _: &mut Context<Self>,
    ) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_state_storage = self.session_state_storage.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            match Self::try_local_linearizable_read(raft_instance).await {
                                Ok(_) => {
                                    if let Some(session_state_storage) = session_state_storage.get()
                                    {
                                        let session_state_storage =
                                            session_state_storage.read().await;
                                        let session_state = session_state_storage
                                            .get_session_state(&msg.tenant_id, &msg.client_id)
                                            .await;
                                        if let Some(session_state) = session_state {
                                            Ok(Some(session_state.read().await.clone()))
                                        } else {
                                            Ok(None)
                                        }
                                    } else {
                                        Err(SessionStateRaftError::NotInitialized)
                                    }
                                }
                                Err(SessionStateRaftError::NotLeader { leader }) => {
                                    log::debug!(
                                        "Not leader, forwarding request to leader: {:?}",
                                        leader
                                    );
                                    if let Some(leader_node) = leader {
                                        let mut client = crate::rpc::grpc_client::lazy_channel(
                                            &leader_node.rpc_addr,
                                        )
                                        .map(ClusterServiceClient::new)
                                        .map_err(|e| {
                                            SessionStateRaftError::GRPCConnect(e.to_string())
                                        })?;
                                        let response = client
                                            .get_session_state(
                                                crate::protobuf::GetSessionStateRequest {
                                                    tenant_id: msg.tenant_id,
                                                    client_id: msg.client_id,
                                                },
                                            )
                                            .await
                                            .map_err(Self::map_remote_status)?;
                                        let inner = response.into_inner();
                                        if let Some(payload) = inner.payload {
                                            match serde_json::from_str(&payload) {
                                                Ok(Some(session_state)) => Ok(Some(session_state)),
                                                Ok(None) => Ok(None),
                                                Err(e) => {
                                                    log::error!(
                                                        "Failed to deserialize session state: {}",
                                                        e
                                                    );
                                                    Ok(None)
                                                }
                                            }
                                        } else {
                                            Ok(None)
                                        }
                                    } else {
                                        Err(SessionStateRaftError::NoLeaderAvailable)
                                    }
                                }
                                Err(e) => Err(e),
                            }
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct CreateSessionState {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<CreateSessionState> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: CreateSessionState, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                let inflight_duration = match self.settings.as_ref() {
                    Some(settings) => settings.mqtt.inflight_retry_interval_secs,
                    None => {
                        return Box::pin(
                            async move { Err(SessionStateRaftError::NotInitialized) }
                                .into_actor(self),
                        );
                    }
                };
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::CreateSessionState {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                                inflight_duration_secs: inflight_duration,
                            };
                            Self::handle_raft_write(raft_instance, command, payload_store).await?;
                            Ok(())
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct DeleteSessionState {
    pub tenant_id: String,
    pub client_id: String,
    pub expected_disconnected_at: Option<u64>,
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct UpdateSessionConnectionState {
    pub tenant_id: String,
    pub client_id: String,
    pub disconnected_at: Option<u64>,
}

#[derive(Message, Clone)]
#[rtype(result = "Result<Vec<(String, String, u64)>, SessionStateRaftError>")]
pub struct ScanExpiredSessions {
    pub now: u64,
    pub ttl: u64,
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct SubscribeTopic {
    pub tenant_id: String,
    pub client_id: String,
    pub topic: String,
    pub qos: u8,
}

impl Handler<SubscribeTopic> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: SubscribeTopic, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::SubscribeTopic {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                                topic: msg.topic,
                                qos: msg.qos,
                            };
                            Self::handle_raft_write(raft_instance, command, payload_store).await?;
                            Ok(())
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct UnsubscribeTopic {
    pub tenant_id: String,
    pub client_id: String,
    pub topic: String,
}

impl Handler<UnsubscribeTopic> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: UnsubscribeTopic, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::UnsubscribeTopic {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                                topic: msg.topic,
                            };
                            Self::handle_raft_write(raft_instance, command, payload_store).await?;
                            Ok(())
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

impl Handler<UpdateSessionConnectionState> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: UpdateSessionConnectionState, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::UpdateSessionConnectionState {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                                disconnected_at: msg.disconnected_at,
                            };
                            Self::handle_raft_write(raft_instance, command, payload_store).await?;
                            Ok(())
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

impl Handler<ScanExpiredSessions> for SessionStateRaftActor {
    type Result =
        ResponseActFuture<Self, Result<Vec<(String, String, u64)>, SessionStateRaftError>>;

    fn handle(&mut self, msg: ScanExpiredSessions, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                // For read operations like scan, we might not want to queue them or maybe we do.
                // Assuming we can queue them for now.
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_state_storage = self.session_state_storage.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            // Ensure linearizability for scan
                            match Self::try_local_linearizable_read(raft_instance).await {
                                Ok(_) => {
                                    if let Some(storage_arc) = session_state_storage.get() {
                                        let storage = storage_arc.read().await;
                                        let expired =
                                            storage.scan_expired_sessions(msg.now, msg.ttl).await;
                                        Ok(expired)
                                    } else {
                                        Err(SessionStateRaftError::NotInitialized)
                                    }
                                }
                                Err(SessionStateRaftError::NotLeader { leader }) => {
                                    // For now, if not leader, we just return empty or error.
                                    // The requirement is that ONLY LEADER performs the scan.
                                    // So if I am not leader, I shouldn't even be called?
                                    // But if called, I should probably return error so caller knows.
                                    // Or forward? Forwarding scan is tricky as it returns big data.
                                    // But wait, the SessionManager checks is_leader() before calling this.
                                    // So if we are here, we SHOULD be leader.
                                    // If linearizable read fails (split brain), we return error.
                                    Err(SessionStateRaftError::NotLeader { leader })
                                }
                                Err(e) => Err(e),
                            }
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

impl Handler<DeleteSessionState> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: DeleteSessionState, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::DeleteSessionState {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                                expected_disconnected_at: msg.expected_disconnected_at,
                            };
                            Self::handle_raft_write(raft_instance, command, payload_store).await?;
                            Ok(())
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct RegisterInflightRxPacket {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u16,
    pub qos: u8,
    pub packet_key: String,
}

impl Handler<RegisterInflightRxPacket> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: RegisterInflightRxPacket, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::InflightRegisterRxPacket {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                                packet_id: msg.packet_id,
                                qos: msg.qos,
                                packet_key: msg.packet_key,
                            };
                            Self::handle_raft_write(raft_instance, command, payload_store).await?;
                            Ok(())
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct RegisterInflightTxPacket {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u16,
    pub qos: u8,
    pub packet_key: String,
}

impl Handler<RegisterInflightTxPacket> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: RegisterInflightTxPacket, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::InflightRegisterTxPacket {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                                packet_id: msg.packet_id,
                                qos: msg.qos,
                                packet_key: msg.packet_key,
                            };
                            Self::handle_raft_write(raft_instance, command, payload_store).await?;
                            Ok(())
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

// Advance to the next state of inflight packet
#[derive(Message, Clone)]
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct AdvanceInflightState {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u16,
}

impl Handler<AdvanceInflightState> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: AdvanceInflightState, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::InflightNextState {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                                packet_identifier: msg.packet_id,
                            };
                            Self::handle_raft_write(raft_instance, command, payload_store).await?;
                            Ok(())
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<MqttPacketV3>, SessionStateRaftError>")]
pub struct GetCurrentInflightPacket {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u16,
}

impl Handler<GetCurrentInflightPacket> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<MqttPacketV3>, SessionStateRaftError>>;

    fn handle(&mut self, msg: GetCurrentInflightPacket, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_state_storage = self.session_state_storage.clone();
                let payload_store = self.payload_store.clone();
                Box::pin(
                    async move {
                        if raft.get().is_some() {
                            if let Some(session_state_storage) = session_state_storage.get() {
                                let session_state_storage = session_state_storage.read().await;
                                let state = session_state_storage
                                    .inflight_get_packet_state(
                                        msg.tenant_id.clone(),
                                        msg.client_id.clone(),
                                        msg.packet_id,
                                    )
                                    .await;

                                let key = session_state_storage
                                    .inflight_get_current_packet_key(
                                        msg.tenant_id,
                                        msg.client_id,
                                        msg.packet_id,
                                    )
                                    .await;

                                match state {
                                    Some(crate::inflight::InflightState::WaitPubcomp) => {
                                        let packet = MqttPacketV3::Pubrel(
                                            yedmq_mqtt::v3::pubrel::PubRelPacket::new(
                                                msg.packet_id,
                                            ),
                                        );
                                        Ok(Some(packet))
                                    }
                                    Some(crate::inflight::InflightState::WaitPubrel) => {
                                        let packet = MqttPacketV3::Pubrec(
                                            yedmq_mqtt::v3::pubrec::PubRecPacket::new(
                                                msg.packet_id,
                                            ),
                                        );
                                        Ok(Some(packet))
                                    }
                                    Some(crate::inflight::InflightState::WaitPubrec)
                                    | Some(crate::inflight::InflightState::WaitPuback) => {
                                        if let Some(key) = key {
                                            let store = payload_store
                                                .get()
                                                .ok_or(SessionStateRaftError::NotInitialized)?;
                                            match store.get(&key).await {
                                                Ok(Some(data)) => {
                                                    match serde_json::from_slice::<MqttPacketV3>(
                                                        &data,
                                                    ) {
                                                        Ok(mut packet) => {
                                                            packet.set_dup(1);
                                                            Ok(Some(packet))
                                                        }
                                                        Err(e) => {
                                                            log::error!(
                                                                "Failed to deserialize packet: {}",
                                                                e
                                                            );
                                                            Ok(None)
                                                        }
                                                    }
                                                }
                                                Ok(None) => {
                                                    log::warn!("Payload missing for key: {}", key);
                                                    Ok(None)
                                                }
                                                Err(e) => {
                                                    log::error!("Store error: {}", e);
                                                    Err(SessionStateRaftError::ServiceUnavailable(
                                                        e.to_string(),
                                                    ))
                                                }
                                            }
                                        } else {
                                            Ok(None)
                                        }
                                    }
                                    _ => Ok(None),
                                }
                            } else {
                                Err(SessionStateRaftError::NotInitialized)
                            }
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<MqttPacketV3>, SessionStateRaftError>")]
pub struct GetCurrentInflightPacketLinearizable {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u16,
}

impl Handler<GetCurrentInflightPacketLinearizable> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<MqttPacketV3>, SessionStateRaftError>>;

    fn handle(
        &mut self,
        msg: GetCurrentInflightPacketLinearizable,
        _: &mut Context<Self>,
    ) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_state_storage = self.session_state_storage.clone();
                let payload_store = self.payload_store.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            match Self::try_local_linearizable_read(raft_instance).await {
                                Ok(_) => {
                                    if let Some(session_state_storage) = session_state_storage.get() {
                                        let session_state_storage = session_state_storage.read().await;
                                        let state = session_state_storage
                                            .inflight_get_packet_state(msg.tenant_id.clone(), msg.client_id.clone(), msg.packet_id).await;
                                        let key = session_state_storage
                                            .inflight_get_current_packet_key(msg.tenant_id, msg.client_id, msg.packet_id).await;

                                        match state {
                                            Some(crate::inflight::InflightState::WaitPubcomp) => {
                                                let packet = MqttPacketV3::Pubrel(yedmq_mqtt::v3::pubrel::PubRelPacket::new(msg.packet_id));
                                                Ok(Some(packet))
                                            }
                                            Some(crate::inflight::InflightState::WaitPubrel) => {
                                                let packet = MqttPacketV3::Pubrec(yedmq_mqtt::v3::pubrec::PubRecPacket::new(msg.packet_id));
                                                Ok(Some(packet))
                                            }
                                            Some(crate::inflight::InflightState::WaitPubrec)
                                            | Some(crate::inflight::InflightState::WaitPuback) => {
                                                if let Some(key) = key {
                                                    let store = payload_store.get().ok_or(SessionStateRaftError::NotInitialized)?;
                                                    match store.get(&key).await {
                                                        Ok(Some(data)) => {
                                                            match serde_json::from_slice::<MqttPacketV3>(&data) {
                                                                Ok(mut packet) => {
                                                                    packet.set_dup(1);
                                                                    Ok(Some(packet))
                                                                }
                                                                Err(e) => {
                                                                    log::warn!("Failed to deserialize packet: {}", e);
                                                                    Ok(None)
                                                                }
                                                            }
                                                        }
                                                        _ => Ok(None)
                                                    }
                                                } else {
                                                    Ok(None)
                                                }
                                            }
                                            _ => Ok(None),
                                        }
                                    } else {
                                        Err(SessionStateRaftError::NotInitialized)
                                    }
                                }
                                Err(SessionStateRaftError::NotLeader { leader }) => {
                                    log::debug!("Not leader, forwarding request to leader: {:?}", leader);
                                    if let Some(leader_node) = leader {
                                        let mut client = crate::rpc::grpc_client::lazy_channel(
                                            &leader_node.rpc_addr,
                                        )
                                        .map(ClusterServiceClient::new)
                                        .map_err(|e| {
                                            SessionStateRaftError::GRPCConnect(e.to_string())
                                        })?;
                                        let response = client.get_current_inflight_packet(
                                            crate::protobuf::GetCurrentInflightPacketRequest {
                                                tenant_id: msg.tenant_id,
                                                client_id: msg.client_id,
                                                packet_id: u32::from(msg.packet_id),
                                            }
                                        ).await.map_err(Self::map_remote_status)?;
                                        let inner = response.into_inner();
                                        if let Some(packet) = inner.packet {
                                            let packet = serde_json::from_str(&packet).map_err(|e| {
                                                SessionStateRaftError::Serialize(format!(
                                                    "deserialize current inflight packet failed: {}",
                                                    e
                                                ))
                                            })?;
                                            Ok(Some(packet))
                                        } else {
                                            Ok(None)
                                        }
                                    } else {
                                        Err(SessionStateRaftError::NoLeaderAvailable)
                                    }
                                }
                                Err(e) => {
                                    Err(e)
                                }
                            }
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<MqttPacketV3>, SessionStateRaftError>")]
pub struct GetNextInflightPacket {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u16,
}

impl Handler<GetNextInflightPacket> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<MqttPacketV3>, SessionStateRaftError>>;

    fn handle(&mut self, msg: GetNextInflightPacket, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_state_storage = self.session_state_storage.clone();
                let payload_store = self.payload_store.clone();
                Box::pin(
                    async move {
                        if raft.get().is_some() {
                            if let Some(session_state_storage) = session_state_storage.get() {
                                let session_state_storage = session_state_storage.read().await;
                                let state = session_state_storage
                                    .inflight_get_packet_state(
                                        msg.tenant_id.clone(),
                                        msg.client_id.clone(),
                                        msg.packet_id,
                                    )
                                    .await;
                                let key = session_state_storage
                                    .inflight_get_next_state_packet_key(
                                        msg.tenant_id,
                                        msg.client_id,
                                        msg.packet_id,
                                    )
                                    .await;

                                match state {
                                    Some(crate::inflight::InflightState::WaitPubcomp) => {
                                        let packet = MqttPacketV3::Pubrel(
                                            yedmq_mqtt::v3::pubrel::PubRelPacket::new(
                                                msg.packet_id,
                                            ),
                                        );
                                        Ok(Some(packet))
                                    }
                                    Some(crate::inflight::InflightState::WaitPubrel) => {
                                        let packet = MqttPacketV3::Pubrec(
                                            yedmq_mqtt::v3::pubrec::PubRecPacket::new(
                                                msg.packet_id,
                                            ),
                                        );
                                        Ok(Some(packet))
                                    }
                                    Some(crate::inflight::InflightState::WaitPubrec)
                                    | Some(crate::inflight::InflightState::WaitPuback) => {
                                        if let Some(key) = key {
                                            let store = payload_store
                                                .get()
                                                .ok_or(SessionStateRaftError::NotInitialized)?;
                                            match store.get(&key).await {
                                                Ok(Some(data)) => {
                                                    match serde_json::from_slice::<MqttPacketV3>(
                                                        &data,
                                                    ) {
                                                        Ok(mut packet) => {
                                                            packet.set_dup(1);
                                                            Ok(Some(packet))
                                                        }
                                                        Err(e) => {
                                                            log::error!(
                                                                "Failed to deserialize packet: {}",
                                                                e
                                                            );
                                                            Ok(None)
                                                        }
                                                    }
                                                }
                                                Ok(None) => {
                                                    log::warn!("Payload missing for key: {}", key);
                                                    Ok(None)
                                                }
                                                Err(e) => {
                                                    log::error!("Store error: {}", e);
                                                    Err(SessionStateRaftError::ServiceUnavailable(
                                                        e.to_string(),
                                                    ))
                                                }
                                            }
                                        } else {
                                            Ok(None)
                                        }
                                    }
                                    _ => Ok(None),
                                }
                            } else {
                                Err(SessionStateRaftError::NotInitialized)
                            }
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<Option<MqttPacketV3>, SessionStateRaftError>")]
pub struct GetNextInflightPacketLinearizable {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u16,
}

impl Handler<GetNextInflightPacketLinearizable> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<MqttPacketV3>, SessionStateRaftError>>;

    fn handle(
        &mut self,
        msg: GetNextInflightPacketLinearizable,
        _: &mut Context<Self>,
    ) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_state_storage = self.session_state_storage.clone();
                let payload_store = self.payload_store.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            match Self::try_local_linearizable_read(raft_instance).await {
                                Ok(_) => {
                                    if let Some(session_state_storage) = session_state_storage.get() {
                                        let session_state_storage = session_state_storage.read().await;
                                        let state = session_state_storage
                                            .inflight_get_packet_state(msg.tenant_id.clone(), msg.client_id.clone(), msg.packet_id).await;
                                        let key = session_state_storage
                                            .inflight_get_next_state_packet_key(msg.tenant_id, msg.client_id, msg.packet_id).await;

                                        match state {
                                            Some(crate::inflight::InflightState::WaitPubcomp) => {
                                                let packet = MqttPacketV3::Pubrel(yedmq_mqtt::v3::pubrel::PubRelPacket::new(msg.packet_id));
                                                Ok(Some(packet))
                                            }
                                            Some(crate::inflight::InflightState::WaitPubrel) => {
                                                let packet = MqttPacketV3::Pubrec(yedmq_mqtt::v3::pubrec::PubRecPacket::new(msg.packet_id));
                                                Ok(Some(packet))
                                            }
                                            Some(crate::inflight::InflightState::WaitPubrec)
                                            | Some(crate::inflight::InflightState::WaitPuback) => {
                                                if let Some(key) = key {
                                                    let store = payload_store.get().ok_or(SessionStateRaftError::NotInitialized)?;
                                                    match store.get(&key).await {
                                                        Ok(Some(data)) => {
                                                            match serde_json::from_slice::<MqttPacketV3>(&data) {
                                                                Ok(mut packet) => {
                                                                    packet.set_dup(1);
                                                                    Ok(Some(packet))
                                                                }
                                                                Err(e) => {
                                                                    log::warn!("Failed to deserialize packet: {}", e);
                                                                    Ok(None)
                                                                }
                                                            }
                                                        }
                                                        _ => Ok(None)
                                                    }
                                                } else {
                                                    Ok(None)
                                                }
                                            }
                                            _ => Ok(None),
                                        }
                                    } else {
                                        Err(SessionStateRaftError::NotInitialized)
                                    }
                                }
                                Err(SessionStateRaftError::NotLeader { leader: leader_node }) => {
                                    if let Some(leader_node) = leader_node {
                                        let mut client = crate::rpc::grpc_client::lazy_channel(
                                            &leader_node.rpc_addr,
                                        )
                                        .map(ClusterServiceClient::new)
                                        .map_err(|e| {
                                            SessionStateRaftError::GRPCConnect(e.to_string())
                                        })?;

                                        let request = crate::protobuf::GetNextInflightPacketRequest {
                                            tenant_id: msg.tenant_id.clone(),
                                            client_id: msg.client_id.clone(),
                                            packet_id: u32::from(msg.packet_id),
                                        };

                                        let response = client
                                            .get_next_inflight_packet(request)
                                            .await
                                            .map_err(Self::map_remote_status)?;
                                        let inner = response.into_inner();
                                        if let Some(packet) = inner.packet {
                                            let packet = serde_json::from_str(&packet).map_err(|e| {
                                                log::error!("Failed to deserialize next inflight packet: {}", e);
                                                SessionStateRaftError::Serialize(format!(
                                                    "deserialize next inflight packet failed: {}",
                                                    e
                                                ))
                                            })?;
                                            Ok(Some(packet))
                                        } else {
                                            Ok(None)
                                        }
                                    } else {
                                        Err(SessionStateRaftError::NoLeaderAvailable)
                                    }
                                }
                                Err(_e) => {
                                    Err(SessionStateRaftError::NotLeader { leader: None })
                                }
                            }
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct InflightCleanFinishedItems {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<InflightCleanFinishedItems> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, msg: InflightCleanFinishedItems, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Ok(()) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let command = SessionStateRequest::InflightCleanFinishItems {
                                tenant_id: msg.tenant_id,
                                client_id: msg.client_id,
                            };
                            Self::handle_raft_write(raft_instance, command, payload_store).await?;
                            Ok(())
                        } else {
                            log::error!("Raft instance not initialized");
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Failed(e) => {
                let e = (**e).clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async {
                    Err(SessionStateRaftError::NotReady(
                        "Actor is stopped".to_string(),
                    ))
                }
                .into_actor(self),
            ),
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::AppendEntriesResponse<NodeId>, SessionStateRaftError>")]
pub struct AppendEntriesRequestMessage {
    pub payload: openraft::raft::AppendEntriesRequest<super::types::SessionStateTypeConfig>,
}

impl Handler<AppendEntriesRequestMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<openraft::raft::AppendEntriesResponse<NodeId>, SessionStateRaftError>,
    >;

    fn handle(&mut self, msg: AppendEntriesRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
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
                            Err(SessionStateRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<openraft::raft::InstallSnapshotResponse<NodeId>, SessionStateRaftError>")]
pub struct InstallSnapshotRequestMessage {
    pub payload: openraft::raft::InstallSnapshotRequest<super::types::SessionStateTypeConfig>,
}

impl Handler<InstallSnapshotRequestMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<openraft::raft::InstallSnapshotResponse<NodeId>, SessionStateRaftError>,
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
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
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
                            Err(SessionStateRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<openraft::raft::VoteResponse<NodeId>, SessionStateRaftError>")]
pub struct VoteRequestMessage {
    pub payload: openraft::raft::VoteRequest<NodeId>,
}

impl Handler<VoteRequestMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<openraft::raft::VoteResponse<NodeId>, SessionStateRaftError>,
    >;

    fn handle(&mut self, msg: VoteRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let is_ready = self.is_ready.load(std::sync::atomic::Ordering::SeqCst);
                Box::pin(
                    async move {
                        if !is_ready {
                            log::warn!(
                                "Rejecting vote request: node is not ready (missing payloads)"
                            );
                            return Err(SessionStateRaftError::NotReady(
                                "Missing payloads for snapshot".to_string(),
                            ));
                        }

                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.vote(msg.payload).await?;
                            Ok(res)
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<(), SessionStateRaftError>")]
pub struct InitRaftClusterMessage {}

impl Handler<InitRaftClusterMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;

    fn handle(&mut self, _msg: InitRaftClusterMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                if let Some(raft_instance) = self.raft.get() {
                    let mut cluster_nodes = BTreeMap::new();
                    let settings = match self.settings.as_ref() {
                        Some(settings) => settings,
                        None => {
                            return Box::pin(
                                async move { Err(SessionStateRaftError::NotInitialized) }
                                    .into_actor(self),
                            );
                        }
                    };
                    for item in settings.cluster.nodes.iter() {
                        cluster_nodes.insert(
                            item.id,
                            Node {
                                node_id: Some(item.id),
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
                    Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
                }
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError>")]
pub struct AddLearnerMessage {
    pub node_id: NodeId,
    pub node: Node,
}

impl Handler<AddLearnerMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError>,
    >;

    fn handle(&mut self, msg: AddLearnerMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
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
                            Err(SessionStateRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError>")]
pub struct ChangeMembershipMessage {
    pub members: Vec<NodeId>,
}

impl Handler<ChangeMembershipMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError>,
    >;

    fn handle(&mut self, msg: ChangeMembershipMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
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
                            Err(SessionStateRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<Option<Node>, SessionStateRaftError>")]
pub struct GetLeader {}

impl Handler<GetLeader> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<Node>, SessionStateRaftError>>;

    fn handle(&mut self, _msg: GetLeader, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
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
                            Err(SessionStateRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError>")]
pub struct DirectWriteToRaft {
    pub command: SessionStateRequest,
}

impl Handler<DirectWriteToRaft> for SessionStateRaftActor {
    type Result = ResponseActFuture<
        Self,
        Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError>,
    >;

    fn handle(&mut self, msg: DirectWriteToRaft, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res =
                                Self::handle_raft_write(raft_instance, msg.command, payload_store)
                                    .await?;
                            Ok(res)
                        } else {
                            Err(SessionStateRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<RaftMetrics<NodeId,Node>, SessionStateRaftError>")]
pub struct GetRaftMetrics {}

#[derive(Message)]
#[rtype(result = "bool")]
pub struct GetPayloadReady {}

impl Handler<GetPayloadReady> for SessionStateRaftActor {
    type Result = bool;

    fn handle(&mut self, _msg: GetPayloadReady, _: &mut Self::Context) -> Self::Result {
        matches!(self.state, ActorState::Running) && self.is_ready.load(Ordering::SeqCst)
    }
}

impl Handler<GetRaftMetrics> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<RaftMetrics<NodeId, Node>, SessionStateRaftError>>;

    fn handle(&mut self, _msg: GetRaftMetrics, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(
                    async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }
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
                            Err(SessionStateRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => Box::pin(
                async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }
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
#[rtype(result = "Result<Vec<NodeId>, SessionStateRaftError>")]
pub struct GetClusterNodes;

impl Handler<GetClusterNodes> for SessionStateRaftActor {
    type Result = Result<Vec<NodeId>, SessionStateRaftError>;

    fn handle(&mut self, _msg: GetClusterNodes, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Ok(Vec::new())
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
                        .map(|node| *node.0)
                        .collect();
                    Ok(nodes)
                } else {
                    Ok(Vec::new())
                }
            }
            ActorState::Failed(e) => Err(SessionStateRaftError::ServiceUnavailable(e.to_string())),
            ActorState::Stopped => Err(SessionStateRaftError::NotReady(
                "Actor is stopped".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_remote_status_reads_leader_redirect_metadata() {
        let status = grpc_status::leader_redirect_status("redirect", "10.0.0.3:9080", Some(3));

        match SessionStateRaftActor::map_remote_status(status) {
            SessionStateRaftError::NotLeader {
                leader: Some(leader),
            } => {
                assert_eq!(leader.node_id, Some(3));
                assert_eq!(leader.rpc_addr, "10.0.0.3:9080");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn map_remote_status_distinguishes_no_leader_and_not_ready() {
        assert!(matches!(
            SessionStateRaftActor::map_remote_status(grpc_status::no_leader_status("no leader")),
            SessionStateRaftError::NoLeaderAvailable
        ));

        assert!(matches!(
            SessionStateRaftActor::map_remote_status(grpc_status::not_ready_status("not ready")),
            SessionStateRaftError::NotReady(message) if message == "not ready"
        ));
    }

    #[test]
    fn map_remote_status_preserves_business_grpc_code() {
        let status = grpc_status::business_status(
            tonic::Code::AlreadyExists,
            grpc_status::business_detail(
                crate::protobuf::ErrorCode::InvalidArgument,
                "duplicate",
                "session_state_raft",
            ),
        );

        match SessionStateRaftActor::map_remote_status(status) {
            SessionStateRaftError::GRPCBusiness(err) => {
                assert_eq!(err.grpc_code(), tonic::Code::AlreadyExists);
                assert_eq!(err.code(), crate::protobuf::ErrorCode::InvalidArgument);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
