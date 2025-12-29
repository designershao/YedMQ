use std::{cell::OnceCell, collections::BTreeMap, path::Path, sync::{Arc, atomic::Ordering}, time::Duration};

use actix::{Actor, AsyncContext, Context, Handler, Message, ResponseActFuture, Supervised, SystemService, WrapFuture};
use openraft::{error::{ClientWriteError, Fatal, InitializeError, RaftError}, raft::ClientWriteResponse, Config, RaftMetrics};
use tokio::sync::RwLock;
use yedmq_mqtt::MqttPacketV3;

use crate::{inflight::InflightError, protobuf::{ cluster_service_client::ClusterServiceClient, raft_service_client::RaftServiceClient, RaftType, WriteRequest}, raft::{session_state::{raft_network_impl::Network, store::new_storage, types::{SessionStateRequest, SessionStateResponse, SessionStateTypeConfig}, SessionStateRaft}, Node, NodeId}, session::session_state_storage::{SessionState, SessionStateStorage, SessionStateStorageError}};


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

    #[error("Session state not existed: {0}")]
    SessionStateNotExisted(String),

    #[error("Packet identifier has existed: {0}")]
    InflightError(#[from] InflightError)
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
    fn service_started(&mut self, _ctx: &mut Context<Self>) {
    }
}

impl Supervised for SessionStateRaftActor {}


impl SessionStateRaftActor {
    async fn initialize_raft(
        settings: Arc<crate::settings::Settings>,
        payload_store: Arc<dyn crate::raft::payload::PayloadStore>,
    ) -> Result<(SessionStateRaft, Arc<RwLock<SessionStateStorage>>, Arc<std::sync::atomic::AtomicBool>), SessionStateRaftError> {
        let raft_config = Config {
            cluster_name: "yedmq_session_state_raft_cluster".to_string(),
            ..Default::default()
        };

        let dir = Path::new(&settings.cluster.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let session_state_storage = Arc::new(RwLock::new(SessionStateStorage::new()));

        let (log_store, state_machine_store, is_ready) = new_storage(&dir, session_state_storage.clone(), payload_store.clone(), settings.clone()).await;

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
                    rpc_addr: item.rpc_address.to_string(),
                    api_addr: item.api_address.to_string(),
                },
            );
        }
        if !raft.is_initialized().await? {
            raft.initialize(cluster_nodes).await?;
        }
        //

        Ok((raft, session_state_storage, is_ready))
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
            log::warn!("failed to write command to raft: {}", e);
            if let RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(
                e_inner,
            )) = e
            {
                SessionStateRaftError::NotLeader {
                    leader: e_inner.leader_node,
                }
            } else {
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
            Ok(r) => {
                match r.data {
                    SessionStateResponse::InflightRegisterRxPacketResponse(Err(SessionStateStorageError::InflightError(e))) => {
                        Err(SessionStateRaftError::InflightError(e))
                    }
                    SessionStateResponse::InflightRegisterTxPacketResponse(Err(SessionStateStorageError::SessionStateNotExisted { client_id })) => {
                        Err(SessionStateRaftError::SessionStateNotExisted(format!("Session state not existed for client_id: {}", client_id)))
                    }
                    _ => {
                        Ok(r)
                    }
                }
            },
            Err(SessionStateRaftError::NotLeader { leader }) => {
                log::info!("Not leader, forwarding request to leader: {:?}", leader);
                if let Some(leader_node) = leader {
                    if let Some(store) = payload_store {
                        let key = match &request {
                            SessionStateRequest::InflightRegisterRxPacket { packet_key, .. } => Some(packet_key),
                            SessionStateRequest::InflightRegisterTxPacket { packet_key, .. } => Some(packet_key),
                            SessionStateRequest::AppendToPendingQueue { packet_key, .. } => Some(packet_key),
                            _ => None,
                        };

                        if let Some(k) = key {
                            let client = crate::raft::payload::PayloadClient::new(store);
                            let metrics_rx = raft.metrics();
                            let term = metrics_rx.borrow().vote.leader_id.term;
                            if let Err(e) = client.replicate(&leader_node.rpc_addr, k.clone(), term).await {
                                log::error!("Failed to push payload to leader before forwarding: {}", e);
                                return Err(SessionStateRaftError::GRPC(e.to_string()));
                            }
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
        let mut client = RaftServiceClient::connect(format!("http://{}", &leader_addr)).await.map_err(|e| {
            log::error!("Failed to connect to leader {}", e);
            SessionStateRaftError::GRPC(e.to_string())
        })?;

        let data = serde_json::to_string(&msg).unwrap();

        let request = WriteRequest {
            data,
            raft_type: RaftType::SessionState.into(),
        };

        let res= client.write(request).await.map_err(|e| {
            log::error!("Failed to send write request to leader: {}", e);
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
            } else {
                log::warn!("Unknown pending message type: {:?}", msg);
            }
        }
    }

}

impl Actor for SessionStateRaftActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(5), |_, ctx| {
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
            ctx.spawn(async move {
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
                        log::info!("Integrity check passed: all payloads present. Node is now READY.");
                        is_ready_flag.store(true, Ordering::SeqCst);
                    }
                } else {
                    if is_ready_flag.load(Ordering::SeqCst) {
                        log::warn!("Integrity check failed: {} payloads missing. Node is NOT READY.", missing.len());
                        is_ready_flag.store(false, Ordering::SeqCst);
                    }

                    // Try to sync missing payloads from leader
                    if let Some(leader_id) = raft.current_leader().await {
                        let metrics_rx = raft.metrics();
                        let metrics = metrics_rx.borrow();
                        let leader_node = metrics.membership_config.nodes().find(|(id, _)| **id == leader_id).map(|(_, n)| n);
                        if let Some(node) = leader_node {
                            log::info!("Syncing {} missing payloads from leader node {}", missing.len(), leader_id);
                            let client = crate::raft::payload::PayloadClient::new(payload_store);
                            let manifest = missing.into_iter().map(|k| (k, 0, 0)).collect();
                            if let Err(e) = client.bulk_sync(&node.rpc_addr, manifest).await {
                                log::warn!("Failed to sync missing payloads from leader: {}", e);
                            }
                        }
                    }
                }
            }.into_actor(self));
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
struct InitializationComplete(Result<(SessionStateRaft, Arc<RwLock<SessionStateStorage>>, Arc<std::sync::atomic::AtomicBool>), SessionStateRaftError>);

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
                self.state = ActorState::Failed(e);
            }
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result="Result<(), SessionStateRaftError>")]
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
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
                            packet_key: msg.packet_key
                        };
                        Self::handle_raft_write(raft_instance, command, payload_store).await?;
                        Ok(())
                    } else {
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}


#[derive(Message, Clone)]
#[rtype(result="Result<Option<String>, SessionStateRaftError>")]
pub struct PopOfflineMessage {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<PopOfflineMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<String>, SessionStateRaftError>>;

    fn handle(&mut self, msg: PopOfflineMessage, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        let command = SessionStateRequest::PopFromPendingQueue { 
                            tenant_id: msg.tenant_id, 
                            client_id: msg.client_id 
                        };
                        let res = Self::handle_raft_write(raft_instance, command, payload_store).await?;
                        match res.data {
                            super::types::SessionStateResponse::PopFromPendingQueueResult(packet_key) => {
                                Ok(packet_key)
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
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

#[derive(Message)]
#[rtype(result="Result<Option<SessionState>, SessionStateRaftError>")]
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
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

#[derive(Message)]
#[rtype(result="Result<Option<SessionState>, SessionStateRaftError>")]
pub struct GetSessionStateEnsureLinearizable {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<GetSessionStateEnsureLinearizable> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<SessionState>, SessionStateRaftError>>;
    
    fn handle(&mut self, msg: GetSessionStateEnsureLinearizable, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let session_state_storage = self.session_state_storage.clone();
                Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        match Self::try_local_linearizable_read(raft_instance).await {
                            Ok(_) => {
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
                            },
                            Err(SessionStateRaftError::NotLeader { leader }) => {
                                log::info!("Not leader, forwarding request to leader: {:?}", leader);
                                if let Some(leader_node) = leader {
                                    let mut client = ClusterServiceClient::connect(format!("http://{}", leader_node.rpc_addr.clone())).await.map_err(|e| {
                                        log::error!("Failed to connect to leader {}", e);
                                        SessionStateRaftError::GRPC(e.to_string())
                                    })?;
                                    client.get_session_state(crate::protobuf::GetSessionStateRequest {
                                        tenant_id: msg.tenant_id,
                                        client_id: msg.client_id,
                                    }).await.map_err(|e| {
                                        log::error!("Failed to get session state from leader: {}", e);
                                        SessionStateRaftError::GRPC(e.to_string())
                                    }).and_then(|res| {
                                        let inner = res.into_inner();
                                        if inner.success {
                                            if let Some(payload) = inner.payload {
                                                println!("Got session state from leader: {}", payload);
                                                match serde_json::from_str(&payload) {
                                                    Ok(Some(session_state)) => Ok(Some(session_state)),
                                                    Ok(None) => Ok(None),
                                                    Err(e) => {
                                                        log::error!("Failed to deserialize session state: {}", e);
                                                        Ok(None)
                                                    }
                                                }
                                            } else {
                                                Ok(None)
                                            }
                                        } else {
                                            Err(SessionStateRaftError::GRPC(inner.error.unwrap().message))
                                        }
                                    })
                                } else {
                                    Err(SessionStateRaftError::NoLeaderAvailable)
                                }
                            },
                            Err(e) => {
                                log::error!("Failed to ensure linearizable read: {}", e);
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
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result="Result<(), SessionStateRaftError>")]
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
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                let inflight_duration = self.settings.as_ref().expect("settings should not be none").mqtt.inflight_retry_interval_secs;
                Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        let command = SessionStateRequest::CreateSessionState { 
                            tenant_id: msg.tenant_id, 
                            client_id: msg.client_id, 
                            inflight_duration_secs: inflight_duration 
                        };
                        Self::handle_raft_write(raft_instance, command, payload_store).await?;
                        Ok(())
                    } else {
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result="Result<(), SessionStateRaftError>")]
pub struct DeleteSessionState {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<DeleteSessionState> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;
    
    fn handle(&mut self, msg: DeleteSessionState, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                async move {
                    if let Some(raft_instance) = raft.get() {
                        let command = SessionStateRequest::DeleteSessionState { 
                            tenant_id: msg.tenant_id, 
                            client_id: msg.client_id 
                        };
                        Self::handle_raft_write(raft_instance, command, payload_store).await?;
                        Ok(())
                    } else {
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result="Result<(), SessionStateRaftError>")]
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
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
                            packet_key: msg.packet_key
                        };
                        Self::handle_raft_write(raft_instance, command, payload_store).await?;
                        Ok(())
                    } else {
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}



#[derive(Message, Clone)]
#[rtype(result="Result<(), SessionStateRaftError>")]
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
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
                            packet_key: msg.packet_key
                        };
                        Self::handle_raft_write(raft_instance, command, payload_store).await?;
                        Ok(())
                    } else {
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

// Advance to the next state of inflight packet
#[derive(Message, Clone)]
#[rtype(result="Result<(), SessionStateRaftError>")]
pub struct AdvanceInflightState {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u64,
}

impl Handler<AdvanceInflightState> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<(), SessionStateRaftError>>;
    
    fn handle(&mut self, msg: AdvanceInflightState, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
                            packet_identifier: msg.packet_id
                        };
                        Self::handle_raft_write(raft_instance, command, payload_store).await?;
                        Ok(())
                    } else {
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

#[derive(Message)]
#[rtype(result="Result<Option<MqttPacketV3>, SessionStateRaftError>")]
pub struct GetCurrentInflightPacket {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u64,
}

impl Handler<GetCurrentInflightPacket> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<MqttPacketV3>, SessionStateRaftError>>;
    
    fn handle(&mut self, msg: GetCurrentInflightPacket, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
                            let packet_id_u16 = msg.packet_id.try_into().unwrap();
                            let state = session_state_storage
                                .inflight_get_packet_state(msg.tenant_id.clone(), msg.client_id.clone(), packet_id_u16).await;
                            
                            let key = session_state_storage
                                .inflight_get_current_packet_key(msg.tenant_id, msg.client_id, packet_id_u16).await;
                            
                            match state {
                                Some(crate::inflight::InflightState::WaitPubcomp) => {
                                    let packet = MqttPacketV3::Pubrel(yedmq_mqtt::v3::pubrel::PubRelPacket::new(packet_id_u16));
                                    Ok(Some(packet))
                                },
                                Some(crate::inflight::InflightState::WaitPubrel) => {
                                    let packet = MqttPacketV3::Pubrec(yedmq_mqtt::v3::pubrec::PubRecPacket::new(packet_id_u16));
                                    Ok(Some(packet))
                                },
                                _ => {
                                    if let Some(key) = key {
                                        let store = payload_store.get().ok_or(SessionStateRaftError::NotInitialized)?;
                                        match store.get(&key).await {
                                            Ok(Some(data)) => {
                                                match serde_json::from_slice::<MqttPacketV3>(&data) {
                                                    Ok(mut packet) => {
                                                        packet.set_dup(1);
                                                        Ok(Some(packet))
                                                    },
                                                    Err(e) => {
                                                        log::error!("Failed to deserialize packet: {}", e);
                                                        Ok(None)
                                                    }
                                                }
                                            },
                                            Ok(None) => {
                                                log::warn!("Payload missing for key: {}", key);
                                                Ok(None)
                                            },
                                            Err(e) => {
                                                log::error!("Store error: {}", e);
                                                Err(SessionStateRaftError::ServiceUnavailable(e.to_string()))
                                            }
                                        }
                                    } else {
                                        Ok(None)
                                    }
                                }
                            }
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    } else {
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }
        }
    }
}

#[derive(Message)]
#[rtype(result="Result<Option<MqttPacketV3>, SessionStateRaftError>")]
pub struct GetCurrentInflightPacketLinearizable {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u64,
}

impl Handler<GetCurrentInflightPacketLinearizable> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<MqttPacketV3>, SessionStateRaftError>>;
    
    fn handle(&mut self, msg: GetCurrentInflightPacketLinearizable, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
                                    let packet_id_u16 = msg.packet_id.try_into().unwrap();
                                    let state = session_state_storage
                                        .inflight_get_packet_state(msg.tenant_id.clone(), msg.client_id.clone(), packet_id_u16).await;
                                    let key = session_state_storage
                                        .inflight_get_current_packet_key(msg.tenant_id, msg.client_id, packet_id_u16).await;
                                    
                                    match state {
                                        Some(crate::inflight::InflightState::WaitPubcomp) => {
                                            let packet = MqttPacketV3::Pubrel(yedmq_mqtt::v3::pubrel::PubRelPacket::new(packet_id_u16));
                                            Ok(Some(packet))
                                        },
                                        Some(crate::inflight::InflightState::WaitPubrel) => {
                                            let packet = MqttPacketV3::Pubrec(yedmq_mqtt::v3::pubrec::PubRecPacket::new(packet_id_u16));
                                            Ok(Some(packet))
                                        },
                                        _ => {
                                            if let Some(key) = key {
                                                let store = payload_store.get().ok_or(SessionStateRaftError::NotInitialized)?;
                                                match store.get(&key).await {
                                                    Ok(Some(data)) => {
                                                        match serde_json::from_slice::<MqttPacketV3>(&data) {
                                                            Ok(mut packet) => {
                                                                packet.set_dup(1);
                                                                Ok(Some(packet))
                                                            },
                                                            Err(e) => Ok(None)
                                                        }
                                                    },
                                                    _ => Ok(None)
                                                }
                                            } else {
                                                Ok(None)
                                            }
                                        }
                                    }
                                } else {
                                    Err(SessionStateRaftError::NotInitialized)
                                }
                            },
                            Err(SessionStateRaftError::NotLeader { leader }) => {
                                log::info!("Not leader, forwarding request to leader: {:?}", leader);
                                if let Some(leader_node) = leader {
                                    let mut client = ClusterServiceClient::connect(format!("http://{}", leader_node.rpc_addr)).await.map_err(|e| {
                                        log::error!("Failed to connect to leader {}", e);
                                        SessionStateRaftError::GRPC(e.to_string())
                                    })?;
                                    let response = client.get_current_inflight_packet(
                                        crate::protobuf::GetCurrentInflightPacketRequest {
                                            tenant_id: msg.tenant_id,
                                            client_id: msg.client_id,
                                            packet_id: msg.packet_id.try_into().unwrap(),
                                        }
                                    ).await.map_err(|e| {
                                        log::error!("Failed to get current inflight packet from leader: {}", e);
                                        SessionStateRaftError::GRPC(e.to_string())
                                    })?;
                                    let inner = response.into_inner();
                                    if inner.success {
                                        if let Some(packet) = inner.packet {
                                            let packet = serde_json::from_str(&packet).unwrap();
                                            Ok(Some(packet))
                                        } else {
                                            Ok(None)
                                        }
                                    } else {
                                        Err(SessionStateRaftError::GRPC(inner.error.unwrap().message))
                                    }
                                } else {
                                    Err(SessionStateRaftError::NoLeaderAvailable)
                                }
                            }
                            Err(e) => {
                                log::error!("Failed to ensure linearizable read: {}", e);
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
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

#[derive(Message)]
#[rtype(result="Result<Option<MqttPacketV3>, SessionStateRaftError>")]
pub struct GetNextInflightPacket{
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u64,
}

impl Handler<GetNextInflightPacket> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<MqttPacketV3>, SessionStateRaftError>>;
    
    fn handle(&mut self, msg: GetNextInflightPacket, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
                            let packet_id_u16 = msg.packet_id.try_into().unwrap();
                            let state = session_state_storage
                                .inflight_get_packet_state(msg.tenant_id.clone(), msg.client_id.clone(), packet_id_u16).await;
                            let key = session_state_storage
                                .inflight_get_next_state_packet_key(msg.tenant_id, msg.client_id, packet_id_u16).await;
                            
                            match state {
                                Some(crate::inflight::InflightState::WaitPubcomp) => {
                                    let packet = MqttPacketV3::Pubrel(yedmq_mqtt::v3::pubrel::PubRelPacket::new(packet_id_u16));
                                    Ok(Some(packet))
                                },
                                Some(crate::inflight::InflightState::WaitPubrel) => {
                                    let packet = MqttPacketV3::Pubrec(yedmq_mqtt::v3::pubrec::PubRecPacket::new(packet_id_u16));
                                    Ok(Some(packet))
                                },
                                _ => {
                                    if let Some(key) = key {
                                        let store = payload_store.get().ok_or(SessionStateRaftError::NotInitialized)?;
                                        match store.get(&key).await {
                                            Ok(Some(data)) => {
                                                match serde_json::from_slice::<MqttPacketV3>(&data) {
                                                    Ok(mut packet) => {
                                                        packet.set_dup(1);
                                                        Ok(Some(packet))
                                                    },
                                                    Err(e) => {
                                                        log::error!("Failed to deserialize packet: {}", e);
                                                        Ok(None)
                                                    }
                                                }
                                            },
                                            Ok(None) => {
                                                log::warn!("Payload missing for key: {}", key);
                                                Ok(None)
                                            },
                                            Err(e) => {
                                                log::error!("Store error: {}", e);
                                                Err(SessionStateRaftError::ServiceUnavailable(e.to_string()))
                                            }
                                        }
                                    } else {
                                        Ok(None)
                                    }
                                }
                            }
                        } else {
                            Err(SessionStateRaftError::NotInitialized)
                        }
                    } else {
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }
        }
    }
}

#[derive(Message)]
#[rtype(result="Result<Option<MqttPacketV3>, SessionStateRaftError>")]
pub struct GetNextInflightPacketLinearizable {
    pub tenant_id: String,
    pub client_id: String,
    pub packet_id: u64,
}

impl Handler<GetNextInflightPacketLinearizable> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<MqttPacketV3>, SessionStateRaftError>>;
    
    fn handle(&mut self, msg: GetNextInflightPacketLinearizable, _: &mut Context<Self>) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
                                    let packet_id_u16 = msg.packet_id.try_into().unwrap();
                                    let state = session_state_storage
                                        .inflight_get_packet_state(msg.tenant_id.clone(), msg.client_id.clone(), packet_id_u16).await;
                                    let key = session_state_storage
                                        .inflight_get_next_state_packet_key(msg.tenant_id, msg.client_id, packet_id_u16).await;
                                    
                                    match state {
                                        Some(crate::inflight::InflightState::WaitPubcomp) => {
                                            let packet = MqttPacketV3::Pubrel(yedmq_mqtt::v3::pubrel::PubRelPacket::new(packet_id_u16));
                                            Ok(Some(packet))
                                        },
                                        Some(crate::inflight::InflightState::WaitPubrel) => {
                                            let packet = MqttPacketV3::Pubrec(yedmq_mqtt::v3::pubrec::PubRecPacket::new(packet_id_u16));
                                            Ok(Some(packet))
                                        },
                                        _ => {
                                            if let Some(key) = key {
                                                let store = payload_store.get().ok_or(SessionStateRaftError::NotInitialized)?;
                                                match store.get(&key).await {
                                                    Ok(Some(data)) => {
                                                        match serde_json::from_slice::<MqttPacketV3>(&data) {
                                                            Ok(mut packet) => {
                                                                packet.set_dup(1);
                                                                Ok(Some(packet))
                                                            },
                                                            Err(e) => Ok(None)
                                                        }
                                                    },
                                                    _ => Ok(None)
                                                }
                                            } else {
                                                Ok(None)
                                            }
                                        }
                                    }
                                } else {
                                    Err(SessionStateRaftError::NotInitialized)
                                }
                            },
                            Err(SessionStateRaftError::NotLeader { leader: leader_node }) => {
                                if let Some(leader_node) = leader_node {
                                    let mut client = ClusterServiceClient::connect(format!("http://{}", leader_node.rpc_addr)).await.map_err(|e| {
                                        log::error!("Failed to connect to leader {}", e);
                                        SessionStateRaftError::GRPC(e.to_string())
                                    })?;

                                    let request = crate::protobuf::GetNextInflightPacketRequest {
                                        tenant_id: msg.tenant_id.clone(),
                                        client_id: msg.client_id.clone(),
                                        packet_id: msg.packet_id,
                                    };

                                    let response = client.get_next_inflight_packet(request).await.map_err(|e| {
                                        log::error!("Failed to get next inflight packet: {}", e);
                                        SessionStateRaftError::GRPC(e.to_string())
                                    })?;
                                    let inner = response.into_inner();
                                    if inner.success {
                                        if let Some(packet) = inner.packet {
                                            let packet = serde_json::from_str(&packet).unwrap();
                                            Ok(Some(packet))
                                        } else {
                                            Ok(None)
                                        }
                                    } else {
                                        Err(SessionStateRaftError::GRPC(inner.error.unwrap().message))
                                    }
                                } else {
                                    Err(SessionStateRaftError::NoLeaderAvailable)
                                }
                            }
                            Err(e) => {
                                log::error!("Failed to ensure linearizable read: {}", e);
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
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }
        }
    }
}

#[derive(Message)]
#[rtype(result="Result<(), SessionStateRaftError>")]
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
                            client_id: msg.client_id 
                        };
                        Self::handle_raft_write(raft_instance, command, payload_store).await?;
                        Ok(())
                    } else {
                        log::error!("Raft instance not initialized");
                        Err(SessionStateRaftError::NotInitialized)
                    }
                }.into_actor(self)
                )
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(
                    async move { Err(SessionStateRaftError::ServiceUnavailable(e.to_string())) }
                        .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(
                    async { Err(SessionStateRaftError::NotReady("Actor is stopped".to_string())) }
                        .into_actor(self),
                )
            }            
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::AppendEntriesResponse<NodeId>, SessionStateRaftError>")]
pub struct AppendEntriesRequestMessage {
    pub payload: openraft::raft::AppendEntriesRequest<super::types::SessionStateTypeConfig>
}

impl Handler<AppendEntriesRequestMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<openraft::raft::AppendEntriesResponse<NodeId>, SessionStateRaftError>>;

    fn handle(&mut self, msg: AppendEntriesRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::InstallSnapshotResponse<NodeId>, SessionStateRaftError>")]
pub struct InstallSnapshotRequestMessage {
    pub payload: openraft::raft::InstallSnapshotRequest<super::types::SessionStateTypeConfig>
}

impl Handler<InstallSnapshotRequestMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<openraft::raft::InstallSnapshotResponse<NodeId>, SessionStateRaftError>>; 

    fn handle(&mut self, msg: InstallSnapshotRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}


#[derive(Message, Clone)]
#[rtype(result = "Result<openraft::raft::VoteResponse<NodeId>, SessionStateRaftError>")]
pub struct VoteRequestMessage {
    pub payload: openraft::raft::VoteRequest<NodeId>
}

impl Handler<VoteRequestMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<openraft::raft::VoteResponse<NodeId>, SessionStateRaftError>>;

    fn handle(&mut self, msg: VoteRequestMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let is_ready = self.is_ready.load(std::sync::atomic::Ordering::SeqCst);
                Box::pin(
                    async move {
                        if !is_ready {
                            log::warn!("Rejecting vote request: node is not ready (missing payloads)");
                            return Err(SessionStateRaftError::NotReady("Missing payloads for snapshot".to_string()));
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
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
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
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                if let Some(raft_instance) = self.raft.get(){
                    let mut cluster_nodes = BTreeMap::new();
                    for item in self.settings.as_ref().expect("settings should not be none").cluster.nodes.iter() {
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
                    Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
                }
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
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
    type Result = ResponseActFuture<Self, Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError>>;

    fn handle(&mut self, msg: AddLearnerMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = raft_instance.add_learner(msg.node_id, msg.node, true).await?;
                            Ok(res)
                        } else {
                            Err(SessionStateRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError>")]
pub struct ChangeMembershipMessage {
    pub members: Vec<NodeId>
}

impl Handler<ChangeMembershipMessage> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError>>;

    fn handle(&mut self, msg: ChangeMembershipMessage, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}


#[derive(Message)]
#[rtype(result = "Result<Option<Node>, SessionStateRaftError>")]

pub struct GetLeader{}

impl Handler<GetLeader> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<Option<Node>, SessionStateRaftError>>;

    fn handle(&mut self, _msg: GetLeader, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
                            Err(SessionStateRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
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
    type Result = ResponseActFuture<Self, Result<ClientWriteResponse<SessionStateTypeConfig>, SessionStateRaftError>>;

    fn handle(&mut self, msg: DirectWriteToRaft, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                self.pending_messages.push(Box::new(msg));
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
            }
            ActorState::Running => {
                let raft = self.raft.clone();
                let payload_store = self.payload_store.get().cloned();
                Box::pin(
                    async move {
                        if let Some(raft_instance) = raft.get() {
                            let res = Self::handle_raft_write(raft_instance, msg.command, payload_store).await?;
                            Ok(res)
                        } else {
                            Err(SessionStateRaftError::NotReady("Initializing".to_string()))
                        }
                    }
                    .into_actor(self),
                )
            }
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}




#[derive(Message)]
#[rtype(result = "Result<RaftMetrics<NodeId,Node>, SessionStateRaftError>")]
pub struct GetRaftMetrics {}

impl Handler<GetRaftMetrics> for SessionStateRaftActor {
    type Result = ResponseActFuture<Self, Result<RaftMetrics<NodeId,Node>, SessionStateRaftError>>;

    fn handle(&mut self, _msg: GetRaftMetrics, _: &mut Self::Context) -> Self::Result {
        match &self.state {
            ActorState::Initializing => {
                log::warn!("SessionStateRaftActor is initializing, message will be queued.");
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Initializing".to_string())) }.into_actor(self))
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
            ActorState::Stopped => {
                Box::pin(async move { Err(SessionStateRaftError::NotReady("Stopped".to_string())) }.into_actor(self))
            }
            ActorState::Failed(e) => {
                let e = e.clone();
                Box::pin(async move { Err(e) }.into_actor(self))
            }
        }
    }
}

