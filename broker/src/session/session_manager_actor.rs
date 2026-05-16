use std::{sync::Arc, time::Duration};

use crate::{
    node_resolver::NodeResolver,
    protobuf::ForceStopSessionActorRequest,
    raft::{session_actor_map::types::RenewSession, NodeId},
    session::{
        session_actor::{SessionActor, SessionActorConfig},
        session_actor_map_service::SessionActorMapService,
        session_registry::{SessionActorRecipientWrapper, SessionRegistry},
        session_state_service::SessionStateService,
    },
    settings::Settings,
};
use actix::{
    dev::MessageResponse, Actor, Addr, AsyncContext, Context, Handler, Message, Recipient,
    ResponseFuture, Supervised, SystemService, WrapFuture,
};
use dashmap::DashMap;
use log::{debug, error, info, warn};
use thiserror::Error;
use tokio::sync::{mpsc::Sender, RwLock};
use yedmq_mqtt::packet::Packet;
use yedmq_plugin_host::plugin_manager::PluginManager;

use super::{
    session_actor::{SessionActorMessage, SessionInfo},
    session_actor_map_storage::{SessionClock, SessionVersion},
    session_state_storage::SessionState,
    WillMessage,
};
use crate::connection::ConnectionActorMessage;
use crate::raft::topic::topic_raft_actor::TopicRaftActor;
use crate::router_actor::RouterActor;
use crate::topic::topic_service::TopicService;

#[derive(Error, Debug)]
pub enum SessionManagerError {
    #[error("tenant {0} not found")]
    TenantNotExisted(String),

    #[error("tenant {0} has existed")]
    TenantHasExisted(String),

    #[error("session {0} not found")]
    SessionNotExisted(String),

    #[error("session {0} has existed")]
    SessionHasExisted(String),

    #[error("newer session has existed")]
    NewerSessionExisted,

    #[error("session actor map error {0}")]
    SessionActorMapError(
        Box<crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError>,
    ),

    #[error("session state raft error {0}")]
    SessionStateRaftError(
        Box<crate::raft::session_state::session_state_raft_actor::SessionStateRaftError>,
    ),

    #[error("send message error {0}")]
    SendMessageError(#[from] actix::MailboxError),

    #[error("node not found {0}")]
    NodeNotFound(String),

    #[error("session manager is not initialized: {0}")]
    NotInitialized(&'static str),

    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

pub enum SessionLifecycleMessage {
    SessionStarted,

    SessionActivate,

    SessionDeactivate,

    SessionStopped {
        tenant_id: String,
        client_id: String,
        version: SessionVersion,
    },
}

impl From<crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError>
    for SessionManagerError
{
    fn from(
        value: crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError,
    ) -> Self {
        Self::SessionActorMapError(Box::new(value))
    }
}

impl From<crate::raft::session_state::session_state_raft_actor::SessionStateRaftError>
    for SessionManagerError
{
    fn from(
        value: crate::raft::session_state::session_state_raft_actor::SessionStateRaftError,
    ) -> Self {
        Self::SessionStateRaftError(Box::new(value))
    }
}

impl SystemService for SessionManagerActor {
    fn service_started(&mut self, _: &mut Context<Self>) {
        info!("SessionManagerActor started");
    }
}

impl Supervised for SessionManagerActor {}

use crate::metric::Metric;
use crate::raft::payload::PayloadStore;
use crate::timer_actor::TimerActor;

#[derive(Default)]
pub struct SessionManagerActor {
    sessions: SessionRegistry,

    plugin_manager: Option<Arc<PluginManager>>,

    session_lifecycle_tx: Option<Sender<SessionLifecycleMessage>>,

    settings: Option<Arc<Settings>>,
    node_resolver: Option<Arc<NodeResolver>>,

    session_clock: Option<Arc<SessionClock>>,

    current_node_id: NodeId,

    router_actors: Option<Vec<Addr<RouterActor>>>,

    arbiter_pool: Option<Arc<crate::arbiter_pool::ArbiterPool>>,

    payload_store: Option<Arc<dyn PayloadStore>>,

    timer_actor: Option<Addr<TimerActor>>,

    metric: Option<Arc<Metric>>,
}

fn require_initialized<T: Clone>(
    value: &Option<T>,
    name: &'static str,
) -> Result<T, SessionManagerError> {
    value
        .as_ref()
        .cloned()
        .ok_or(SessionManagerError::NotInitialized(name))
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Initialize {
    pub settings: Arc<Settings>,
    pub node_resolver: Arc<NodeResolver>,
    pub plugin_manager: Arc<PluginManager>,
    pub session_clock: Arc<SessionClock>,
    pub session_registry: SessionRegistry,
    pub timer_actor: Addr<TimerActor>,
    pub payload_store: Arc<dyn PayloadStore>,
    pub metric: Arc<Metric>,
    pub arbiter_pool: Arc<crate::arbiter_pool::ArbiterPool>,
}

impl Handler<Initialize> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, msg: Initialize, _ctx: &mut Self::Context) -> Self::Result {
        self.settings = Some(msg.settings.clone());
        self.node_resolver = Some(msg.node_resolver);
        self.plugin_manager = Some(msg.plugin_manager);
        self.session_clock = Some(msg.session_clock);
        self.current_node_id = msg.settings.cluster.node_id;
        self.sessions = msg.session_registry;
        self.payload_store = Some(msg.payload_store);
        self.timer_actor = Some(msg.timer_actor);
        self.metric = Some(msg.metric);
        self.arbiter_pool = Some(msg.arbiter_pool);
    }
}

impl Actor for SessionManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let (session_lifecycle_tx, mut session_lifecycle_rx) = tokio::sync::mpsc::channel(10);
        self.session_lifecycle_tx = Some(session_lifecycle_tx);
        let self_addr = ctx.address();
        let session_actor_map_service = SessionActorMapService::from_registry();
        let future = async move {
            while let Some(msg) = session_lifecycle_rx.recv().await {
                match msg {
                    SessionLifecycleMessage::SessionStarted => {}
                    SessionLifecycleMessage::SessionActivate => {}
                    SessionLifecycleMessage::SessionDeactivate => {}
                    SessionLifecycleMessage::SessionStopped {
                        tenant_id,
                        client_id,
                        version,
                    } => {
                        info!(
                            "Received session lifecycle message SessionStopped session {} stopped",
                            client_id
                        );
                        if let Err(err) = session_actor_map_service
                            .unregister_session_actor_map(
                                tenant_id.clone(),
                                client_id.clone(),
                                version.clone(),
                            )
                            .await
                        {
                            warn!("failed to unregister session actor map: {}", err);
                        }
                        // Because the session should renew the session lease before it stops,
                        // so if we send unregister error, we just log it and continue to remove session from local map and
                        // let the raft state machine to clean up the session actor map later.
                        match self_addr
                            .send(RemoveSessionMessage {
                                tenant_id,
                                client_id,
                            })
                            .await
                        {
                            Ok(Ok(_)) => {}
                            Ok(Err(e)) => {
                                if let SessionManagerError::TenantNotExisted(tenant_id) = e {
                                    warn!("tenant {} not existed when removing session from local map, maybe tenant has been removed", tenant_id);
                                } else {
                                    error!("failed to remove session from local map: {}", e);
                                }
                            }
                            Err(err) => {
                                error!("failed to send RemoveSessionMessage to self: {:?}", err);
                            }
                        }
                    }
                }
            }
        };
        ctx.spawn(future.into_actor(self));

        // Renew session lease every 10 seconds

        ctx.run_interval(Duration::from_secs(10), |_, ctx| {
            ctx.address().do_send(RenewSessionLease {});
        });

        // Check expired sessions every 30 seconds

        ctx.run_interval(Duration::from_secs(30), |_, ctx| {
            ctx.address().do_send(CheckExpiredSessions {});
        });
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("session manager stopped");
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct SetRouterActors {
    pub router_actors: Vec<Addr<RouterActor>>,
}

impl Handler<SetRouterActors> for SessionManagerActor {
    type Result = ();
    fn handle(&mut self, msg: SetRouterActors, _ctx: &mut Self::Context) -> Self::Result {
        self.router_actors = Some(msg.router_actors);
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RenewSessionLease {}

#[derive(Message)]
#[rtype(result = "()")]
pub struct CheckExpiredSessions {}

impl Handler<CheckExpiredSessions> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, _msg: CheckExpiredSessions, ctx: &mut Self::Context) -> Self::Result {
        let session_state_service = SessionStateService::from_registry();
        let session_actor_map_service = SessionActorMapService::from_registry();
        let Some(settings) = self.settings.as_ref().cloned() else {
            warn!("skip CheckExpiredSessions: settings not initialized");
            return;
        };
        let Some(node_resolver) = self.node_resolver.as_ref().cloned() else {
            warn!("skip CheckExpiredSessions: node_resolver not initialized");
            return;
        };
        let topic_raft_actor_addr = TopicRaftActor::from_registry();
        let ttl = settings.cluster.session_ttl;
        let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(err) => {
                warn!(
                    "skip CheckExpiredSessions: system time is before UNIX_EPOCH: {}",
                    err
                );
                return;
            }
        };

        ctx.spawn(
            async move {
                let expired_res = session_state_service.scan_expired_sessions(now, ttl).await;

                match expired_res {
                    Ok(expired_list) => {
                        for (tenant_id, client_id, disconnected_at) in expired_list {
                            info!("cleaning up expired persistent session: {}/{} (disconnected at {})", tenant_id, client_id, disconnected_at);

                            let map_entry_res = session_actor_map_service
                                .get_session_actor_map(tenant_id.clone(), client_id.clone())
                                .await;

                            if let Ok(Some(entry)) = map_entry_res {
                                if let Err(e) = call_force_disconnect(
                                    entry.node_id,
                                    node_resolver.clone(),
                                    topic_raft_actor_addr.clone(),
                                    tenant_id.clone(),
                                    client_id.clone(),
                                ).await {
                                    warn!("failed to force disconnect expired session {}/{} on node {}: {}", tenant_id, client_id, entry.node_id, e);
                                }
                            }

                            let _ = session_state_service
                                .delete_session_state(
                                    tenant_id.clone(),
                                    client_id.clone(),
                                    Some(disconnected_at),
                                )
                                .await;
                        }
                    }
                    Err(crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NotLeader { .. }) => {
                    }
                    Err(e) => {
                        error!("failed to scan expired sessions: {}", e);
                    }
                }
            }
            .into_actor(self),
        );
    }
}

impl Handler<RenewSessionLease> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, _msg: RenewSessionLease, ctx: &mut Self::Context) -> Self::Result {
        let sessions = self.sessions.get_inner().clone();
        ctx.spawn(
            async move {
                for entry in sessions.iter() {
                    let tenant_sessions = entry.value();
                    let tenant_id = entry.key();

                    debug!("renew session lease for tenant {}", tenant_id);
                    let sessions_keys: Vec<RenewSession> = tenant_sessions
                        .iter()
                        .map(|entry| RenewSession {
                            tenant_id: tenant_id.clone(),
                            session_id: entry.key().clone(),
                        })
                        .collect();

                    let session_actor_map_service = SessionActorMapService::from_registry();
                    for session in &sessions_keys {
                        debug!(
                            "renew session lease for tenant {} session {}",
                            tenant_id, session.session_id
                        );
                        let res = session_actor_map_service
                            .renew_session(session.tenant_id.clone(), session.session_id.clone())
                            .await;
                        if let Err(err) = res {
                            error!("failed to renew session lease: {}", err);
                        } else {
                            debug!("renew session lease for tenant {} succeed", tenant_id);
                        }
                    }
                }
            }
            .into_actor(self),
        );
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), SessionManagerError>")]
pub struct RemoveExpiredSession {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<RemoveExpiredSession> for SessionManagerActor {
    type Result = ResponseFuture<Result<(), SessionManagerError>>;

    fn handle(&mut self, msg: RemoveExpiredSession, _ctx: &mut Self::Context) -> Self::Result {
        info!("remove expired session {}", msg.client_id);
        let sessions = self.sessions.get_inner().clone();
        Box::pin(async move {
            let tenant_sessions = sessions.get(msg.tenant_id.as_str());
            match tenant_sessions {
                None => Err(SessionManagerError::TenantNotExisted(msg.tenant_id)),
                Some(tenant_sessions) => {
                    let session = tenant_sessions.get(&msg.client_id);
                    if let Some(session) = session {
                        session
                            .session_actor_message_recipient
                            .do_send(SessionActorMessage::ForceStop);
                        info!(
                            "force stop session {} remove from local node session map",
                            msg.client_id
                        );
                        tenant_sessions.remove(&msg.client_id);
                        info!("remove expired session {} succeed", msg.client_id);
                        Ok(())
                    } else {
                        warn!("session {} not found in tenant {}, maybe this node has rebooted or the session has been removed", msg.client_id, msg.tenant_id);
                        Err(SessionManagerError::SessionNotExisted(msg.client_id))
                    }
                }
            }
        })
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RemoveDuplicateSessionsByClock {
    pub tenant_id: String,
    pub client_id: String,
    pub session_version: SessionVersion,
}

impl Handler<RemoveDuplicateSessionsByClock> for SessionManagerActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RemoveDuplicateSessionsByClock,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        info!("remove duplicate session {}", msg.client_id);
        let sessions = self.sessions.get_inner().clone();
        let self_addr = ctx.address();
        ctx.spawn(
            async move {
                let should_stop = if let Some(tenant_sessions) = sessions.get(&msg.tenant_id) {
                    if let Some(session) = tenant_sessions.get(&msg.client_id) {
                        msg.session_version.is_newer_than(&session.session_version)
                    } else {
                        false
                    }
                } else {
                    warn!("tenant {} not found", msg.tenant_id);
                    false
                };

                if should_stop {
                    match self_addr
                        .send(ForceStop {
                            tenant_id: msg.tenant_id.clone(),
                            client_id: msg.client_id.clone(),
                        })
                        .await
                    {
                        Ok(res) => {
                            if let Err(e) = res {
                                error!(
                                    "force stop duplicate session {} failed: {}",
                                    msg.client_id, e
                                );
                            } else {
                                info!("remove duplicate session {} succeed", msg.client_id);
                            }
                        }
                        Err(e) => {
                            error!(
                            "send ForceStop message to self for duplicate session {} failed: {}",
                            msg.client_id, e
                        );
                        }
                    }
                }
            }
            .into_actor(self),
        );
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct SendMessageToSession {
    pub tenant_id: String,
    pub client_id: String,
    pub packet: Packet,
}

impl Handler<SendMessageToSession> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, msg: SendMessageToSession, ctx: &mut Self::Context) -> Self::Result {
        let sessions = self.sessions.get_inner().clone();
        ctx.spawn(
            async move {
                if let Some(tenant_session) = sessions.get(&msg.tenant_id) {
                    if let Some(session) = tenant_session.get(&msg.client_id) {
                        session
                            .session_actor_message_recipient
                            .do_send(SessionActorMessage::OutboundMessage(msg.packet));
                    }
                } else {
                    warn!("tenant {} not found", msg.tenant_id);
                }
            }
            .into_actor(self),
        );
    }
}

#[derive(Message)]
#[rtype(result = "Result<SessionInfo, SessionManagerError>")]
pub struct GetSessionInfo {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<GetSessionInfo> for SessionManagerActor {
    type Result = ResponseFuture<Result<SessionInfo, SessionManagerError>>;
    fn handle(&mut self, msg: GetSessionInfo, _ctx: &mut Self::Context) -> Self::Result {
        let sessions = self.sessions.get_inner().clone();
        Box::pin(async move {
            match sessions.get(msg.tenant_id.as_str()) {
                None => Err(SessionManagerError::TenantNotExisted(msg.tenant_id)),
                Some(tenant_sessions) => {
                    if let Some(session) = tenant_sessions.get(&msg.client_id) {
                        let session_info_recipient = session.get_session_info_recipient.clone();
                        let session_info = session_info_recipient
                            .send(crate::session::session_actor::GetSessionInfo {})
                            .await?;
                        Ok(session_info)
                    } else {
                        Err(SessionManagerError::SessionNotExisted(msg.client_id))
                    }
                }
            }
        })
    }
}

#[derive(Message)]
#[rtype(result = "Result<(u64,Vec<SessionInfo>), SessionManagerError>")]
pub struct GetSessionInfoListWithPagination {
    pub tenant_id: String,
    pub offset_param: u64,
    pub limit_param: u64,
}

impl Handler<GetSessionInfoListWithPagination> for SessionManagerActor {
    type Result = ResponseFuture<Result<(u64, Vec<SessionInfo>), SessionManagerError>>;
    fn handle(
        &mut self,
        msg: GetSessionInfoListWithPagination,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let sessions = self.sessions.get_inner().clone();
        Box::pin(async move {
            match sessions.get(msg.tenant_id.as_str()) {
                None => Err(SessionManagerError::TenantNotExisted(msg.tenant_id)),
                Some(tenant_sessions) => {
                    let mut session_infos = Vec::new();
                    let total_len = tenant_sessions.len();
                    let tenant_sessions_iter = tenant_sessions.iter();
                    let iter = tenant_sessions_iter
                        .skip(msg.offset_param as usize)
                        .take(msg.limit_param as usize);
                    for session in iter {
                        let session_info_recipient = session.get_session_info_recipient.clone();
                        let session_info = session_info_recipient
                            .send(crate::session::session_actor::GetSessionInfo {})
                            .await?;
                        session_infos.push(session_info);
                    }
                    Ok((total_len as u64, session_infos))
                }
            }
        })
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), SessionManagerError>")]
pub struct ForceStopWithSessionService {
    pub tenant_id: String,
    pub client_id: String,
    pub session_version: SessionVersion,
}

impl Handler<ForceStopWithSessionService> for SessionManagerActor {
    type Result = ResponseFuture<Result<(), SessionManagerError>>;

    fn handle(
        &mut self,
        msg: ForceStopWithSessionService,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let sessions = self.sessions.get_inner().clone();
        Box::pin(async move {
            match sessions.get(&msg.tenant_id) {
                None => Err(SessionManagerError::TenantNotExisted(msg.tenant_id)),
                Some(tenant_sessions) => {
                    if let Some(session) = tenant_sessions.get(&msg.client_id) {
                        if msg.session_version.is_newer_than(&session.session_version) {
                            let recipient = session.session_actor_message_recipient.clone();
                            drop(session);
                            let res = recipient.send(SessionActorMessage::ForceStop).await;
                            if let Err(e) = res {
                                error!("force stop session {} failed: {}", msg.client_id, e);
                            }
                        }
                    }
                    Ok(())
                }
            }
        })
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), SessionManagerError>")]
pub struct ForceStop {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<ForceStop> for SessionManagerActor {
    type Result = ResponseFuture<Result<(), SessionManagerError>>;

    fn handle(&mut self, msg: ForceStop, _ctx: &mut Self::Context) -> Self::Result {
        info!("force stop session {}", msg.client_id);
        let sessions = self.sessions.get_inner().clone();

        Box::pin(async move {
            match sessions.get(&msg.tenant_id) {
                None => Err(SessionManagerError::TenantNotExisted(msg.tenant_id)),
                Some(tenant_sessions) => {
                    if let Some(session) = tenant_sessions.get(&msg.client_id) {
                        let recipient = session.session_actor_message_recipient.clone();
                        drop(session);
                        if let Err(e) = recipient.send(SessionActorMessage::ForceStop).await {
                            warn!(
                                "failed to send ForceStop to session actor {}: {}. It might have already stopped.",
                                msg.client_id, e
                            );
                        }
                        info!(
                            "force stop session {} remove from local node session map",
                            msg.client_id
                        );
                        tenant_sessions.remove(&msg.client_id);
                        Ok(())
                    } else {
                        Err(SessionManagerError::SessionNotExisted(msg.client_id))
                    }
                }
            }
        })
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), SessionManagerError>")]
pub struct ForceDisconnect {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<ForceDisconnect> for SessionManagerActor {
    type Result = ResponseFuture<Result<(), SessionManagerError>>;

    fn handle(&mut self, msg: ForceDisconnect, _ctx: &mut Self::Context) -> Self::Result {
        let sessions = self.sessions.get_inner().clone();
        Box::pin(async move {
            match sessions.get(&msg.tenant_id) {
                None => Err(SessionManagerError::TenantNotExisted(msg.tenant_id)),
                Some(tenant_sessions) => {
                    if let Some(session) = tenant_sessions.get(&msg.client_id) {
                        session
                            .session_actor_message_recipient
                            .do_send(SessionActorMessage::ForceDisconnect);
                        Ok(())
                    } else {
                        Err(SessionManagerError::SessionNotExisted(msg.client_id))
                    }
                }
            }
        })
    }
}

pub struct CreateSessionMessageResponse {
    pub session_actor_recipient: Recipient<SessionActorMessage>,

    pub session_present: bool,
}

#[derive(Message)]
#[rtype(result = "Result<CreateSessionMessageResponse, SessionManagerError>")]
pub struct CreateSessionMessage {
    pub tenant_id: String,
    pub client_id: String,
    pub clean_session: bool,
    pub connection_addr: Recipient<ConnectionActorMessage>,
    pub keep_alive: u64,
    pub will_message: Option<WillMessage>,
    pub username: Option<String>,
    pub peer_addr: std::net::SocketAddr,
}

// force disconnect
async fn call_force_disconnect(
    node_id: NodeId,
    node_resolver: Arc<NodeResolver>,
    topic_raft_actor_addr: Addr<TopicRaftActor>,
    tenant_id: String,
    client_id: String,
) -> Result<bool, SessionManagerError> {
    let max_retries = 3;

    let node = node_resolver
        .get_node(node_id, &topic_raft_actor_addr)
        .await
        .ok_or_else(|| SessionManagerError::NodeNotFound(node_id.to_string()))?;

    let mut client = crate::protobuf::cluster_service_client::ClusterServiceClient::new(
        crate::rpc::grpc_client::lazy_channel(&node.rpc_addr)?,
    );

    for i in 0..max_retries {
        info!(
            "force disconnect session {} from node {} in {} retry",
            client_id, node_id, i
        );
        let res = client
            .force_stop_session_actor(ForceStopSessionActorRequest {
                tenant_id: tenant_id.clone(),
                client_id: client_id.clone(),
            })
            .await;
        if res.is_ok() {
            info!(
                "force disconnect session {} from node {} succeed",
                client_id, node_id
            );
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_secs(2 ^ i)).await;
    }
    Ok(false)
}

impl Handler<CreateSessionMessage> for SessionManagerActor {
    type Result = ResponseFuture<Result<CreateSessionMessageResponse, SessionManagerError>>;

    fn handle(&mut self, msg: CreateSessionMessage, _ctx: &mut Self::Context) -> Self::Result {
        let sessions = self.sessions.get_inner();
        let tenant_id = msg.tenant_id.clone();
        let plugin_manager = require_initialized(&self.plugin_manager, "plugin_manager");
        let settings = require_initialized(&self.settings, "settings");
        let node_resolver = require_initialized(&self.node_resolver, "node_resolver");
        let topic_raft_actor_addr = TopicRaftActor::from_registry();
        let session_lifecycle_tx =
            require_initialized(&self.session_lifecycle_tx, "session_lifecycle_tx");
        let session_clock = require_initialized(&self.session_clock, "session_clock");
        let current_node_id = self.current_node_id;
        let router_actors = require_initialized(&self.router_actors, "router_actors");
        let arbiter_pool = require_initialized(&self.arbiter_pool, "arbiter_pool");
        let payload_store = self.payload_store.clone();
        let timer_actor = require_initialized(&self.timer_actor, "timer_actor");
        let metric = require_initialized(&self.metric, "metric");

        let future = async move {
            let plugin_manager = plugin_manager?;
            let settings = settings?;
            let node_resolver = node_resolver?;
            let session_lifecycle_tx = session_lifecycle_tx?;
            let session_clock = session_clock?;
            let router_actors = router_actors?;
            let arbiter_pool = arbiter_pool?;
            let timer_actor = timer_actor?;
            let metric = metric?;

            let tenant_sessions = sessions
                .entry(tenant_id.clone())
                .or_insert_with(|| Arc::new(DashMap::new()))
                .clone();

            // Force previous session to disconnect if exists
            let session_actor_map_service = SessionActorMapService::from_registry();
            let session_actor_map_entry = session_actor_map_service
                .get_session_actor_map(msg.tenant_id.clone(), msg.client_id.clone())
                .await?;

            if let Some(entry) = &session_actor_map_entry {
                if entry.node_id != current_node_id {
                    info!("previous session not in current force disconnect previous session actor map node id: {}", entry.node_id);
                    let res = call_force_disconnect(
                        entry.node_id,
                        node_resolver,
                        topic_raft_actor_addr,
                        msg.tenant_id.clone(),
                        msg.client_id.clone(),
                    )
                    .await;
                    if let Err(err) = res {
                        warn!(
                            "force disconnect previous session actor map node id: {} failed: {}",
                            entry.node_id, err
                        );
                    }
                } else {
                    info!("previous session in current node, force disconnect");
                    if let Some(session) = tenant_sessions.get(&msg.client_id) {
                        let recipient = session.session_actor_message_recipient.clone();
                        drop(session);
                        let res = recipient.send(SessionActorMessage::ForceDisconnect).await;
                        if let Err(err) = res {
                            warn!("force disconnect in current node failed: {}", err);
                        }
                    } else {
                        info!("session map in current not found, maybe node reboot");
                    }
                }
            } else {
                debug!("previous session actor map node id not found");
            }

            debug!(
                "start register session actor map tenant_id: {}, client_id: {}, node_id: {}",
                msg.tenant_id, msg.client_id, current_node_id
            );

            let session_version = session_clock.next();

            if let Err(e) = session_actor_map_service
                .register_session_actor_map(
                    msg.tenant_id.clone(),
                    msg.client_id.clone(),
                    current_node_id,
                    session_version.clone(),
                )
                .await
            {
                match e {
                    crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::SessionVersionRejected {
                        current_version,
                        existing_version,
                    } => {
                        info!(
                            "register session actor map rejected tenant_id: {}, client_id: {}, current_version: {}, existing_version: {}",
                            msg.tenant_id, msg.client_id, current_version, existing_version
                        );
                        return Err(SessionManagerError::NewerSessionExisted);
                    }
                    _ => {
                        return Err(e.into());
                    }
                }
            }

            debug!(
                "register session actor map succeed tenant_id: {}, client_id: {}, node_id: {}",
                msg.tenant_id, msg.client_id, current_node_id
            );
            //

            let sessions_guard = tenant_sessions;

            let mut session_state = Arc::new(RwLock::new(SessionState::new(Duration::from_secs(
                settings.mqtt.inflight_retry_interval_secs,
            ))));

            let session_state_service = SessionStateService::from_registry();

            let mut session_present = false;

            if !msg.clean_session {
                info!(
                    "session {} not clean session, into state recover or create logic.",
                    msg.client_id
                );
                // If raft store not existed, create new session state
                // First check local sessions if exists send reconect
                // If local sessions not existed, recover from raft store
                if let Ok(res) = session_state_service
                    .get_session_state_linearizable(msg.tenant_id.clone(), msg.client_id.clone())
                    .await
                {
                    if let Some(session_state_from_raft) = res {
                        if let Some(session_recipient_wrapper) = sessions_guard.get(&msg.client_id)
                        {
                            info!(
                                "session {} exists in current node, start reconnect",
                                msg.client_id
                            );

                            // Persistent session still run in current node, send reconnect
                            session_recipient_wrapper
                                .session_actor_message_recipient
                                .do_send(SessionActorMessage::Reconnect {
                                    conn: msg.connection_addr.clone(),
                                    keep_alive: msg.keep_alive,
                                    clean_session: msg.clean_session,
                                    username: msg.username.clone(),
                                    will_message: msg.will_message.clone(),
                                    socket_addr: msg.peer_addr,
                                });
                            return Ok(CreateSessionMessageResponse {
                                session_actor_recipient: session_recipient_wrapper
                                    .session_actor_message_recipient
                                    .clone(),
                                session_present: true,
                            });
                        } else {
                            info!(
                                "session {} not exists in current node, recover from raft",
                                msg.client_id
                            );
                            // not in current node, recover from raft
                            session_state = Arc::new(RwLock::new(session_state_from_raft));
                        }
                    } else {
                        info!(
                            "session {} not exists in cluster, create new session state",
                            msg.client_id
                        );
                        session_state_service
                            .create_session_state(msg.tenant_id.clone(), msg.client_id.clone())
                            .await?;
                    }

                    session_present = true;
                }
            } else {
                info!(
                    "session {} is clean session, delete previous session state if exists",
                    msg.client_id
                );
                let _ = session_state_service
                    .delete_session_state(msg.tenant_id.clone(), msg.client_id.clone(), None)
                    .await;
            }
            let plugin_manager_clone = plugin_manager.clone();
            let msg_client_id = msg.client_id.clone();
            let msg_tenant_id = msg.tenant_id.clone();
            let session_version_clone = session_version.clone();
            let session_actor_addr = arbiter_pool.start_actor(move || {
                SessionActor::new(SessionActorConfig {
                    tenant_id: msg_tenant_id,
                    client_id: msg_client_id,
                    clean_session: msg.clean_session,
                    plugin_manager: plugin_manager_clone,
                    inflight_retry_duration_secs: 50,
                    will_message: msg.will_message,
                    keep_alive: msg.keep_alive,
                    connection_actor_addr: msg.connection_addr,
                    peer_addr: msg.peer_addr,
                    session_state,
                    session_lifecycle_tx,
                    session_state_service: SessionStateService::from_registry(),
                    topic_service: TopicService::from_registry(),
                    router_actors,
                    payload_store: payload_store.clone(),
                    timer_actor,
                    metric,
                    session_version: session_version_clone,
                })
            });

            let session_actor_message_recipient = session_actor_addr.clone().recipient();
            let accept_routed_publish_recipient = session_actor_addr.clone().recipient();
            let get_session_info_recipient = session_actor_addr.clone().recipient();
            sessions_guard.insert(
                msg.client_id.clone(),
                SessionActorRecipientWrapper {
                    session_actor_message_recipient: session_actor_message_recipient.clone(),
                    accept_routed_publish_recipient,
                    get_session_info_recipient,
                    session_version,
                },
            );
            Ok(CreateSessionMessageResponse {
                session_actor_recipient: session_actor_message_recipient,
                session_present,
            })
        };
        Box::pin(future)
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), SessionManagerError>")]
struct CreateTenantMessage {
    tenant_id: String,
}

impl Handler<CreateTenantMessage> for SessionManagerActor {
    type Result = ResponseFuture<Result<(), SessionManagerError>>;

    fn handle(&mut self, msg: CreateTenantMessage, _ctx: &mut Self::Context) -> Self::Result {
        let sessions = self.sessions.get_inner();

        Box::pin(async move {
            if !sessions.contains_key(&msg.tenant_id) {
                sessions.insert(msg.tenant_id, Arc::new(DashMap::new()));
                Ok(())
            } else {
                Err(SessionManagerError::TenantHasExisted(msg.tenant_id))
            }
        })
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), SessionManagerError>")]
struct RemoveSessionMessage {
    tenant_id: String,
    client_id: String,
}

impl Handler<RemoveSessionMessage> for SessionManagerActor {
    type Result = ResponseFuture<Result<(), SessionManagerError>>;

    fn handle(&mut self, msg: RemoveSessionMessage, _ctx: &mut Self::Context) -> Self::Result {
        let sessions = self.sessions.get_inner().clone();
        let tenant_id = msg.tenant_id.clone();
        let future = async move {
            match sessions.get(&tenant_id) {
                Some(tenant_sessions) => {
                    let tenant_sessions = tenant_sessions.clone();
                    tenant_sessions.remove(&msg.client_id);
                    info!(
                        "received RemoveSessionMessage, remove session {} from tenant {} succeed",
                        msg.client_id, msg.tenant_id
                    );
                    Ok(())
                }
                None => Err(SessionManagerError::TenantNotExisted(msg.tenant_id)),
            }
        };
        Box::pin(future)
    }
}

#[derive(Message)]
#[rtype(result = "Vec<String>")]
pub struct GetAllTenantIds {}

impl Handler<GetAllTenantIds> for SessionManagerActor {
    type Result = ResponseFuture<Vec<String>>;

    fn handle(&mut self, _msg: GetAllTenantIds, _ctx: &mut Self::Context) -> Self::Result {
        let sessions = self.sessions.get_inner();
        Box::pin(async move { sessions.iter().map(|entry| entry.key().clone()).collect() })
    }
}

#[derive(Message)]
#[rtype(result = "Option<Arc<SessionClock>>")]
pub struct GetLatestSessionClock {}

impl<A, M> MessageResponse<A, M> for SessionClock
where
    A: Actor,
    M: Message<Result = SessionClock>,
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

impl Handler<GetLatestSessionClock> for SessionManagerActor {
    type Result = Option<Arc<SessionClock>>;

    fn handle(&mut self, _msg: GetLatestSessionClock, _ctx: &mut Self::Context) -> Self::Result {
        self.session_clock.clone()
    }
}
