use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    protobuf::ForceStopSessionActorRequest, raft::{
        NodeId, session_actor_map::{
            session_actor_map_raft_actor::{self, SessionActorMapRaftActor},
            types::RenewSession,
        }
    }, session::{session_actor::SessionActor, session_registry::{SessionActorRecipientWrapper, SessionRegistry}}, settings::Settings
};
use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, Recipient, ResponseActFuture, ResponseFuture, Supervised, SystemService, WrapFuture, dev::MessageResponse};
use dashmap::DashMap;
use log::{debug, error, info, warn};
use thiserror::Error;
use tokio::sync::{mpsc::Sender, RwLock};
use yedmq_mqtt::
    MqttPacketV3
;
use yedmq_plugin_host::plugin_manager::PluginManager;

use super::{
    session_actor::{GetSessionInfo, SessionActorMessage, SessionInfo},
    session_actor_map_storage::{SessionClock, SessionVersion},
    session_state_storage::SessionState,
    WillMessage,
};
use crate::connection::ConnectionActorMessage;
use crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor;
use crate::raft::topic::topic_raft_actor::TopicRaftActor;
use crate::router_actor::RouterActor;

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
    SessionActorMapError(#[from] crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError),

    #[error("session state raft error {0}")]
    SessionStateRaftError(#[from] crate::raft::session_state::session_state_raft_actor::SessionStateRaftError),

    #[error("send message error {0}")]
    SendMessageError(#[from] actix::MailboxError),

    #[error("node not found {0}")]
    NodeNotFound(String),
        
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
    },
}

impl SystemService for SessionManagerActor {
    fn service_started(&mut self, _: &mut Context<Self>) {
        info!("SessionManagerActor started");
    }
}

impl Supervised for SessionManagerActor {}

pub struct SessionManagerActor {
    sessions: SessionRegistry,

    plugin_manager: Option<Arc<PluginManager>>,

    session_lifecycle_tx: Option<Sender<SessionLifecycleMessage>>,

    settings: Option<Arc<Settings>>,

    session_clock: Option<Arc<SessionClock>>,

    current_node_id: NodeId,

    router_actor: Option<Addr<RouterActor>>,

    arbiter_pool: Option<Arc<crate::arbiter_pool::ArbiterPool>>,
}

#[derive(Message)]
#[rtype(result = "()" )]
pub struct Initialize {
    pub settings: Arc<Settings>,
    pub plugin_manager: Arc<PluginManager>,
    pub session_clock: Arc<SessionClock>,
    pub session_registry: SessionRegistry,
}

impl Handler<Initialize> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, msg: Initialize, _ctx: &mut Self::Context) -> Self::Result {
        self.settings = Some(msg.settings.clone());
        self.plugin_manager = Some(msg.plugin_manager);
        self.session_clock = Some(msg.session_clock);
        self.current_node_id = msg.settings.cluster.node_id;
        self.sessions = msg.session_registry;
    }
}

impl Default for SessionManagerActor {
    fn default() -> Self {
        Self {
            sessions: SessionRegistry::default(),
            plugin_manager: None,
            session_lifecycle_tx: None,
            settings: None,
            session_clock: None,
            current_node_id: 0,
            router_actor: None,
            arbiter_pool: None,
        }
    }
}


impl Actor for SessionManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let (session_lifecycle_tx, mut session_lifecycle_rx) = tokio::sync::mpsc::channel(10);
        self.session_lifecycle_tx = Some(session_lifecycle_tx);
        let self_addr = ctx.address();
        let session_actor_map_actor_addr = SessionActorMapRaftActor::from_registry();
        let future = async move {
            while let Some(msg) = session_lifecycle_rx.recv().await {
                match msg {
                    SessionLifecycleMessage::SessionStarted => {}
                    SessionLifecycleMessage::SessionActivate => {}
                    SessionLifecycleMessage::SessionDeactivate => {}
                    SessionLifecycleMessage::SessionStopped {
                        tenant_id,
                        client_id,
                    } => {
                        info!(
                            "received session lifectcle message SessionStopped session {} stopped",
                            client_id
                        );
                        let session_option = self_addr.send(GetLatestSessionClock{}).await;
                        match session_option {
                            Err(err) => {
                                error!("get session clock for unregister session actor map error: {}", err);
                                continue;
                            }
                            Ok(Some(session_clock)) => {
                                let session_version = session_clock.next();
                                let res = session_actor_map_actor_addr.send(
                                    crate::raft::session_actor_map::session_actor_map_raft_actor::UnregisterSessionActorMap {
                                        tenant_id: tenant_id.clone(),
                                        client_id: client_id.clone(),
                                        version: session_version.clone(),
                                    }
                                ).await;
                                if let Err(err) = res {
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
                                    .await {
                                    Ok(res) => {
                                        match res {
                                            Ok(_) => {}
                                            Err(e) => {
                                                if let SessionManagerError::TenantNotExisted(tenant_id) = e {
                                                    warn!("tenant {} not existed when removing session from local map, maybe tenant has been removed", tenant_id);
                                                } else {
                                                    error!("failed to remove session from local map: {}", e);
                                                }
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        error!("failed to send RemoveSessionMessage to self: {}", err);
                                    }
                                }
                            }
                            Ok(None) => {
                                error!("session clock not found when unregister session actor map");
                                continue;
                            }
                        };
                    }
                }
            }
        };
        ctx.spawn(future.into_actor(self));

        // Renew session lease every 10 seconds
        ctx.run_interval(Duration::from_secs(10), |_, ctx| {
            ctx.address().do_send(RenewSessionLease {});
        });
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("session manager stopped");
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct SetArbiterPool {
    pub arbiter_pool: Arc<crate::arbiter_pool::ArbiterPool>,
}

impl Handler<SetArbiterPool> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, msg: SetArbiterPool, _ctx: &mut Self::Context) -> Self::Result {
        self.arbiter_pool = Some(msg.arbiter_pool);
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct SetRouterActor{
    pub router_actor: Addr<RouterActor>
}

impl Handler<SetRouterActor> for SessionManagerActor {
    type Result = ();
    fn handle(&mut self, msg: SetRouterActor, _ctx: &mut Self::Context) -> Self::Result {
        println!("set router actor in session manager");
        self.router_actor = Some(msg.router_actor);
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RenewSessionLease {}

impl Handler<RenewSessionLease> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, _msg: RenewSessionLease, ctx: &mut Self::Context) -> Self::Result {
        let sessions = self.sessions.get_inner().clone();
        ctx.spawn(async move {
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

                let session_actor_map_raft_actor_addr = SessionActorMapRaftActor::from_registry();
                for session in &sessions_keys {
                    debug!(
                        "renew session lease for tenant {} session {}",
                        tenant_id, session.session_id
                    );
                    let res = session_actor_map_raft_actor_addr
                        .send(session_actor_map_raft_actor::RenewSession {
                            tenant_id: session.tenant_id.clone(),
                            client_id: session.session_id.clone(),
                        })
                        .await;
                    if let Err(err) = res {
                        error!("failed to renew session lease: {}", err);
                    } else {
                        debug!("renew session lease for tenant {} succeed", tenant_id);
                    }
                }
            }
        }.into_actor(self));
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
                        session.session_actor_message_recipient
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
        ctx.spawn(async move {
            if let Some(tenant_sessions) = sessions.get(&msg.tenant_id) {
                if let Some(session) = tenant_sessions.get(&msg.client_id) {
                     if msg.session_version.is_newer_than(&session.session_version) {
                        match self_addr
                            .send(ForceStop {
                                tenant_id: msg.tenant_id.clone(),
                                client_id: msg.client_id.clone(),
                            })
                            .await {
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
            } else {
                warn!("tenant {} not found", msg.tenant_id);
            }
        }.into_actor(self));
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct SendMessageToSession {
    pub tenant_id: String,
    pub client_id: String,
    pub packet: MqttPacketV3,
}

impl Handler<SendMessageToSession> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, msg: SendMessageToSession, ctx: &mut Self::Context) -> Self::Result {
        let sessions = self.sessions.get_inner().clone();
        ctx.spawn(async move {
            if let Some(tenant_session) = sessions.get(&msg.tenant_id) {
                if let Some(session) = tenant_session.get(&msg.client_id) {
                    session.session_actor_message_recipient.do_send(SessionActorMessage::OutboundMessage(msg.packet));
                }
            } else {
                warn!("tenant {} not found", msg.tenant_id);
            }
        }.into_actor(self));
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
                            .send(GetSessionInfo {})
                            .await?;
                        session_infos.push(session_info);
                    }
                    Ok((total_len as u64, session_infos))
                },
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
                             let res = session
                                .session_actor_message_recipient
                                .send(SessionActorMessage::ForceStop)
                                .await;
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
                        if let Err(e) = session
                            .session_actor_message_recipient
                            .send(SessionActorMessage::ForceStop)
                            .await
                        {
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
                        session.session_actor_message_recipient.do_send(SessionActorMessage::ForceDisconnect);
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
    nodes: &Vec<crate::settings::Node>,
    tenant_id: String,
    client_id: String,
) -> Result<bool, SessionManagerError> {
    let max_retries = 3;

    let node = nodes
        .iter()
        .find(|node| node.id == node_id).ok_or_else(|| SessionManagerError::NodeNotFound(node_id.to_string()))?;

    let addr = format!("http://{}", node.rpc_address);

    let mut client = crate::protobuf::cluster_service_client::ClusterServiceClient::connect(addr.clone()).await?;

    for i in 0..max_retries {
        info!(
            "force disconnect session {} from node {} in {} retry",
            client_id, node_id, i
        );
        let res = client.force_stop_session_actor(ForceStopSessionActorRequest {
            tenant_id: tenant_id.clone(),
            client_id: client_id.clone(),
        }).await;
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
        let plugin_manager = self.plugin_manager.as_ref().unwrap().clone();
        let settings = self.settings.as_ref().unwrap().clone();
        let session_lifecycle_tx = self.session_lifecycle_tx.as_ref()
            .expect("session lifecycle tx must exist").clone();
        let session_clock = self.session_clock.as_ref().unwrap().clone();
        let current_node_id = self.current_node_id;
        let router_actor = self.router_actor.as_ref().unwrap().clone();
        let arbiter_pool = self.arbiter_pool.as_ref().unwrap().clone();

        let future = async move {
            let existing = sessions.get(&tenant_id);
            let tenant_sessions = match existing {
                Some(ts) => ts,
                None => {
                    sessions.insert(tenant_id.clone(), Arc::new(DashMap::new()));
                    sessions.get(&tenant_id).unwrap()
                }
            };

            // Force previous session to disconnect if exists
            let session_actor_map_raft_actor_addr = SessionActorMapRaftActor::from_registry();
            let session_actor_map_entry = session_actor_map_raft_actor_addr.send(
                session_actor_map_raft_actor::GetSessionActorMap{
                    tenant_id: msg.tenant_id.clone(),
                    client_id: msg.client_id.clone(),
                }
            ).await??;

            if let Some(entry) = &session_actor_map_entry {
                if entry.node_id != current_node_id {
                    info!("previous session not in current force disconnect previous session actor map node id: {}", entry.node_id);
                    let res = call_force_disconnect(
                        entry.node_id,
                        &settings.cluster.nodes,
                        msg.tenant_id.clone(),
                        msg.client_id.clone(),
                    )
                    .await;
                    if res.is_err() {
                        warn!(
                            "force disconnect previous session actor map node id: {} failed: {}",
                            entry.node_id,
                            res.unwrap_err()
                        );
                    }
                } else {
                    info!("previous session in current node, force disconnect");
                    if tenant_sessions.get(&msg.client_id).is_some() {
                        let session = tenant_sessions.get(&msg.client_id).unwrap();
                        let res = session
                            .session_actor_message_recipient
                            .send(SessionActorMessage::ForceDisconnect)
                            .await;
                        if res.is_err() {
                            warn!(
                                "force disconnect in current node failed: {}",
                                res.unwrap_err()
                            );
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

            let session_actor_map_raft_actor_addr = SessionActorMapRaftActor::from_registry();

            let res = session_actor_map_raft_actor_addr.send(
                session_actor_map_raft_actor::RegisterSessionActorMap {
                    tenant_id: msg.tenant_id.clone(),
                    client_id: msg.client_id.clone(),
                    node_id: current_node_id,
                    version: session_version.clone(),
                }
            ).await?;

            match res {
                Ok(_) => {}
                Err(e) => {
                    match e {
                        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::SessionVersionRejected { 
                            current_version, 
                            existing_version } => {
                            info!(
                                "register session actor map rejected tenant_id: {}, client_id: {}, current_version: {}, existing_version: {}",
                                msg.tenant_id, msg.client_id, current_version, existing_version
                            );
                            return Err(SessionManagerError::NewerSessionExisted);
                        },
                        _ => {
                            return Err(SessionManagerError::SessionActorMapError(e));
                        }
                    }
                }
            }

            debug!(
                "register session actor map succeed tenant_id: {}, client_id: {}, node_id: {}",
                msg.tenant_id, msg.client_id, current_node_id
            );
            //

            let mut sessions_guard = tenant_sessions;

            let mut session_state = Arc::new(RwLock::new(SessionState::new(Duration::from_secs(
                settings.mqtt.inflight_retry_interval_secs,
            ))));

            let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

            let mut session_present = false;

            if !msg.clean_session {
                info!(
                    "session {} not clean session, into state recover or create logic.",
                    msg.client_id
                );
                // If raft store not existed, create new session state
                // First check local sessions if exists send reconect
                // If local sessions not existed, recover from raft store
                let res = session_state_raft_actor_addr.send(
                    crate::raft::session_state::session_state_raft_actor::GetSessionStateEnsureLinearizable {
                        tenant_id: msg.tenant_id.clone(),
                        client_id: msg.client_id.clone(),
                    },
                ).await?;

                if let Ok(res) = res {
                    if res.is_some() {
                        // check session in current node
                        if sessions_guard.contains_key(&msg.client_id) {
                            info!(
                                "session {} exists in current node, start reconnect",
                                msg.client_id
                            );
                            sessions_guard
                                .get(&msg.client_id)
                                .unwrap()
                                .session_actor_message_recipient
                                .send(SessionActorMessage::Reconnect {
                                    conn: msg.connection_addr.clone(),
                                    keep_alive: msg.keep_alive,
                                    clean_session: msg.clean_session,
                                    username: msg.username.clone(),
                                    will_message: msg.will_message.clone(),
                                    socket_addr: msg.peer_addr,
                                })
                                .await
                                .unwrap();
                            let session_recipient = sessions_guard.get(&msg.client_id).unwrap();
                            return Ok(
                                CreateSessionMessageResponse {
                                    session_actor_recipient: session_recipient.session_actor_message_recipient.clone(),
                                    session_present: true,
                                }
                            );
                            // in current node, send reconnect
                        } else {
                            info!(
                                "session {} not exists in current node, recover from raft",
                                msg.client_id
                            );
                            // not in current node, recover from raft
                            session_state = Arc::new(RwLock::new(res.unwrap()));
                        }
                        //
                    } else {
                        info!(
                            "session {} not exists in cluster, create new session state",
                            msg.client_id
                        );
                        session_state_raft_actor_addr.send(
                            crate::raft::session_state::session_state_raft_actor::CreateSessionState {
                                tenant_id: msg.tenant_id.clone(),
                                client_id: msg.client_id.clone(),
                            },
                        ).await??;
                    }

                    session_present = true;

                }

            }
            let plugin_manager_clone = plugin_manager.clone();
            let msg_client_id = msg.client_id.clone();
            let msg_tenant_id = msg.tenant_id.clone();
            let session_actor_addr = arbiter_pool.start_actor(move || {
                SessionActor::new(
                    msg_tenant_id,
                    msg_client_id,
                    msg.clean_session,
                    plugin_manager_clone,
                    50,
                    msg.will_message,
                    msg.keep_alive,
                    msg.connection_addr,
                    msg.peer_addr,
                    session_state,
                    session_lifecycle_tx,
                    SessionStateRaftActor::from_registry(),
                    TopicRaftActor::from_registry(),
                    router_actor,
                )
            });

            let session_actor_message_recipient = session_actor_addr.clone().recipient();
            let get_session_info_recipient = session_actor_addr.clone().recipient();
            sessions_guard.insert(
                msg.client_id.clone(),
                SessionActorRecipientWrapper {
                    session_actor_message_recipient: session_actor_message_recipient.clone(),
                    get_session_info_recipient,
                    session_version,
                },
            );
            Ok(CreateSessionMessageResponse { session_actor_recipient: session_actor_message_recipient, session_present })
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

impl SessionManagerActor {
    async fn tenant_existed(&self, tenant_identifier: &str) -> bool {
        self.sessions.get_inner().contains_key(tenant_identifier)
    }

    async fn create_tenant(&mut self, tenant_identifier: &str) {
        self.sessions.get_inner().insert(
            tenant_identifier.to_string(),
            Arc::new(DashMap::new()),
        );
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

    fn handle(&mut self, msg: RemoveSessionMessage, ctx: &mut Self::Context) -> Self::Result {

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
                None => {
                    return Err(SessionManagerError::TenantNotExisted(msg.tenant_id));
                }
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
        Box::pin(async move {
           sessions.iter().map(|entry| {
                entry.key().clone()
           }).collect()
        })
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