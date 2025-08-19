use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    plugin_manager::{PluginManager, PluginService},
    protobuf::{cluster_service_server::ClusterService, raft_service_client::RaftServiceClient, ForceStopSessionActorRequest},
    raft::{
        client::base::RaftClientError,
        raft_manager::{RaftManagerError, RaftManagerTrait},
        session_actor_map::{
            session_actor_map_raft_actor::{self, SessionActorMapRaftActor},
            types::RenewSession,
        },
        Node, NodeId,
    },
    session::session_actor::SessionActor,
    settings::Settings,
};
use actix::{
    dev::ContextFutureSpawner, Actor, AsyncContext, Context, Handler, Message, Recipient,
    ResponseFuture, Supervised, SystemService, WrapFuture,
};
use log::{error, info, warn};
use openraft::{error::ClientWriteError, metrics::Wait};
use thiserror::Error;
use tokio::sync::{mpsc::Sender, OnceCell, RwLock};
use yedmq_mqtt::{
    v3::connack::{ConnAckPacketBuilder, ConnackReturnCode},
    MqttPacketV3,
};

use super::{
    session_actor::{GetSessionInfo, SessionActorMessage, SessionInfo},
    session_actor_map_storage::{SessionClock, SessionVersion},
    session_state_storage::SessionState,
    WillMessage,
};
use crate::connection::ConnectionActorMessage;

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

    #[error("raft error {0} ")]
    RaftErr(#[from] RaftManagerError<ClientWriteError<NodeId, Node>>),

    #[error("raft client error {0}")]
    RaftClientErr(#[from] RaftClientError),

    #[error("newer session has existed")]
    NewerSessionExisted,

    #[error("session actor map error {0}")]
    SessionActorMapError(#[from] crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError),
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

struct SessionActorRecipientWrapper {
    session_actor_message_recipient: Recipient<SessionActorMessage>,

    get_session_info_recipient: Recipient<GetSessionInfo>,

    session_version: SessionVersion,
}

impl Default for SessionManagerActor {
    fn default() -> Self {
        SessionManagerActor {
            sessions: HashMap::new(),
            plugin_manager: OnceCell::new(),
            session_lifecycle_tx: None,
            settings: Arc::new(Settings::default()),
            session_clock: OnceCell::new(),
            current_node_id: NodeId::default(),
        }
    }
}


impl SystemService for SessionManagerActor {
    fn service_started(&mut self, ctx: &mut Context<Self>) {
        let settings = self.settings.clone();
        let addr = ctx.address();

        ctx.spawn(
            async move {
                let res =
                    SessionManagerActor::initialize(settings).await;
                addr.do_send(InitializationComplete(res));
            }.into_actor(self)
        );
    }
}

impl Supervised for SessionManagerActor {}

pub struct SessionManagerActor {
    sessions: HashMap<String, Arc<RwLock<HashMap<String, SessionActorRecipientWrapper>>>>,

    plugin_manager: OnceCell<Arc<dyn PluginService + 'static>>,

    session_lifecycle_tx: Option<Sender<SessionLifecycleMessage>>,

    settings: Arc<Settings>,

    session_clock: OnceCell<Arc<SessionClock>>,

    current_node_id: NodeId,
}

impl SessionManagerActor {
    async fn initialize(
        settings: Arc<crate::settings::Settings>,
    ) -> Result<(PluginManager, SessionClock), SessionManagerError> {
        // init plugin manager
        info!("start load plugin manager");
        let plugin_manager =
            PluginManager::new(settings.plugin.dir.clone(), settings.clone()).unwrap();
        let session_clock = SessionClock::new(
            settings.cluster.node_id,
            settings.session.session_clock_path.clone(),
        );
        session_clock.restore().await.unwrap();
        //
        Ok((plugin_manager, session_clock))
    }
}

impl Actor for SessionManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let (session_lifecycle_tx, mut session_lifecycle_rx) = tokio::sync::mpsc::channel(10);
        self.session_lifecycle_tx = Some(session_lifecycle_tx);
        let self_addr = ctx.address();
        let session_clock = self.session_clock.get().unwrap().clone();
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
                        let session_version = session_clock.next();
                        let res = session_actor_map_actor_addr.send(
                            crate::raft::session_actor_map::session_actor_map_raft_actor::UnregisterSessionActorMap {
                                tenant_id: tenant_id.clone(),
                                client_id: client_id.clone(),
                                version: session_version.clone(),
                            }
                        ).await;
                        if let Err(err) = res {
                            error!("failed to unregister session actor map: {}", err);
                        }
                        self_addr
                            .send(RemoveSessionMessage {
                                tenant_id,
                                client_id,
                            })
                            .await
                            .unwrap()
                            .unwrap();
                    }
                }
            }
        };
        ctx.spawn(future.into_actor(self));

        // Renew session lease every 10 seconds
        ctx.run_interval(Duration::from_secs(10), |act, ctx| {
            ctx.address().do_send(RenewSessionLease {});
        });
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("session manager stopped");
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct InitializationComplete(Result<(PluginManager, SessionClock), SessionManagerError>);

impl Handler<InitializationComplete> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, msg: InitializationComplete, ctx: &mut Self::Context) -> Self::Result {
        match msg.0 {
            Ok((plugin_manager, session_clock)) => {
                self.plugin_manager.set(Arc::new(plugin_manager));
                self.session_clock.set(Arc::new(session_clock));
                info!("session manager initialized successfully");
            }
            Err(e) => {
                error!("failed to initialize session manager: {}", e);
            }
        }
    }
}



#[derive(Message)]
#[rtype(result = "()")]
pub struct RenewSessionLease {}

impl Handler<RenewSessionLease> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, _msg: RenewSessionLease, ctx: &mut Self::Context) -> Self::Result {
        for (tenant_id, tenant_sessions) in self.sessions.iter() {
            let tenant_sessions = tenant_sessions.clone();
            let tenant_id = tenant_id.clone();
            async move {
                info!("renew session lease for tenant {}", tenant_id);
                let sessions = tenant_sessions
                    .read()
                    .await
                    .iter()
                    .map(|(client_id, _)| RenewSession {
                        tenant_id: tenant_id.clone(),
                        session_id: client_id.clone(),
                    })
                    .collect::<Vec<RenewSession>>();
                let session_actor_map_raft_actor_addr = SessionActorMapRaftActor::from_registry();
                for session in &sessions {
                    info!(
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
                        info!("renew session lease for tenant {} succeed", tenant_id);
                    }
                }
            }
            .into_actor(self)
            .wait(ctx);
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RemoveExpiredSession {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<RemoveExpiredSession> for SessionManagerActor {
    type Result = ();

    fn handle(&mut self, msg: RemoveExpiredSession, ctx: &mut Self::Context) -> Self::Result {
        info!("remove expired session {}", msg.client_id);
        let tenant_sessions = self.sessions.get(&msg.tenant_id);
        if tenant_sessions.is_none() {
            warn!("tenant {} not found", msg.tenant_id);
            return;
        }
        let tenant_sessions = tenant_sessions.unwrap().clone();
        async move {
            let mut tenant_sessions = tenant_sessions.write().await;
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
            } else {
                warn!("session {} not found in tenant {}, maybe this node has rebooted or the session has been removed", msg.client_id, msg.tenant_id);
                return;
            }
        }
        .into_actor(self)
        .wait(ctx);
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
        let tenant_sessions = self.sessions.get(&msg.tenant_id);
        if let Some(tenant_sessions) = tenant_sessions {
            let tenant_sessions = tenant_sessions.clone();
            let self_addr = ctx.address();
            async move {
                let tenant_sessions = tenant_sessions.read().await;
                let session = tenant_sessions.get(&msg.client_id);
                if let Some(session) = session {
                    if msg.session_version.is_newer_than(&session.session_version) {
                        let _ = self_addr
                            .send(ForceStop {
                                tenant_id: msg.tenant_id.clone(),
                                client_id: msg.client_id.clone(),
                            })
                            .await
                            .unwrap();
                        info!("remove duplicate session {} succeed", msg.client_id);
                    }
                }
            }
            .into_actor(self)
            .wait(ctx);
        } else {
            warn!("tenant {} not found", msg.tenant_id);
        }
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
        info!("send packet to session {}", msg.client_id);
        let tenant_session = self.sessions.get(&msg.tenant_id);
        if let Some(tenant_session) = tenant_session {
            let tenant_session = tenant_session.clone();
            async move {
                let session = tenant_session.read().await;
                let session = session.get(&msg.client_id);
                if let Some(session) = session {
                    let res = session
                        .session_actor_message_recipient
                        .send(SessionActorMessage::OutboundMessage(msg.packet))
                        .await;
                    if let Err(e) = res {
                        error!("send packet to session {} failed: {}", msg.client_id, e);
                    }
                }
            }
            .into_actor(self)
            .wait(ctx);
        } else {
            warn!("tenant {} not found", msg.tenant_id);
        }
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
        let tenant_sessions = self.sessions.get(msg.tenant_id.as_str());
        if tenant_sessions.is_none() {
            Box::pin(async { Err(SessionManagerError::TenantNotExisted(msg.tenant_id)) })
        } else {
            let tenant_sessions = tenant_sessions.unwrap().clone();
            let mut session_infos = Vec::new();
            let f = async move {
                let tenant_sessions = tenant_sessions.read().await;
                let total_len = tenant_sessions.len();
                let tenant_sessions_iter = tenant_sessions.iter();
                let iter = tenant_sessions_iter
                    .skip(msg.offset_param as usize)
                    .take(msg.limit_param as usize);
                for (_, session) in iter {
                    let session_info_recipient = session.get_session_info_recipient.clone();
                    let session_info = session_info_recipient
                        .send(GetSessionInfo {})
                        .await
                        .unwrap();
                    session_infos.push(session_info);
                }
                Ok((total_len as u64, session_infos))
            };
            Box::pin(f)
        }
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
        if self.sessions.contains_key(&msg.tenant_id) == false {
            return Box::pin(async { Err(SessionManagerError::TenantNotExisted(msg.tenant_id)) });
        }
        let tenant_sessions = self.sessions.get(&msg.tenant_id).unwrap().clone();

        let f = async move {
            let tenant_sessions = tenant_sessions.write().await;

            let session = tenant_sessions.get(&msg.client_id);

            if let Some(session) = session {
                // if session version is newer than the one in the message,stop the older session
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
        };
        Box::pin(f)
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
        if self.sessions.contains_key(&msg.tenant_id) == false {
            return Box::pin(async { Err(SessionManagerError::TenantNotExisted(msg.tenant_id)) });
        }
        let tenant_sessions = self.sessions.get(&msg.tenant_id).unwrap().clone();

        let f = async move {
            let mut tenant_sessions = tenant_sessions.write().await;
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
            } else {
                return Err(SessionManagerError::SessionNotExisted(msg.client_id));
            }
            Ok(())
        };

        Box::pin(f)
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
        if self.sessions.contains_key(&msg.tenant_id) == false {
            return Box::pin(async { Err(SessionManagerError::TenantNotExisted(msg.tenant_id)) });
        }
        let tenant_sessions = self.sessions.get(&msg.tenant_id).unwrap().clone();

        let f = async move {
            let tenant_sessions = tenant_sessions.read().await;
            let session = tenant_sessions.get(&msg.client_id);
            if let Some(session) = session {
                session
                    .session_actor_message_recipient
                    .do_send(SessionActorMessage::ForceDisconnect);
            } else {
                return Err(SessionManagerError::SessionNotExisted(msg.client_id));
            }
            Ok(())
        };

        Box::pin(f)
    }
}

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

impl Message for CreateSessionMessage {
    type Result = Result<Recipient<SessionActorMessage>, SessionManagerError>;
}

// force disconnect
async fn call_force_disconnect(
    node_id: NodeId,
    nodes: &Vec<crate::settings::Node>,
    tenant_id: String,
    client_id: String,
) -> Result<bool, tonic::transport::Error> {
    let max_retries = 3;

    let node = nodes
        .iter()
        .find(|node| node.id == node_id).unwrap();

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
    type Result = ResponseFuture<Result<Recipient<SessionActorMessage>, SessionManagerError>>;

    fn handle(&mut self, msg: CreateSessionMessage, _ctx: &mut Self::Context) -> Self::Result {
        if !self.tenant_existed(&msg.tenant_id) {
            info!("tenant not existed, create tenant {}", msg.tenant_id);
            self.create_tenant(&msg.tenant_id);
            //return Err(SessionManagerError::TenantNotExisted(msg.tenant_id));
        }

        let sessions = self.sessions.get(&msg.tenant_id).unwrap().clone();
        let plugin_manager = self.plugin_manager.clone();
        let settings = self.settings.clone();
        let session_lifecycle_tx = self.session_lifecycle_tx.clone().unwrap().clone();

        let session_clock = self.session_clock.get().unwrap().clone();
        let current_node_id = self.current_node_id;


        let future = async move {
            // Force previous session to disconnect if exists
            let session_actor_map_raft_actor_addr = SessionActorMapRaftActor::from_registry();
            let session_actor_map_entry = session_actor_map_raft_actor_addr.send(
                session_actor_map_raft_actor::GetSessionActorMap{
                    tenant_id: msg.tenant_id.clone(),
                    client_id: msg.client_id.clone(),
                }
            ).await.unwrap().unwrap();

            if let Some(entry) = &session_actor_map_entry {
                info!("previous session actor map node id: {}", entry.node_id);
                if entry.node_id != current_node_id {
                    info!("previous session not in current force disconnect previous session actor map node id: {}", entry.node_id);
                    let res = call_force_disconnect(
                        entry.node_id,
                        settings.cluster.nodes.as_ref(),
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
                    let sessions_guard = sessions.read().await;
                    if sessions_guard.get(&msg.client_id).is_some() {
                        let session = sessions_guard.get(&msg.client_id).unwrap();
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
                info!("previous session actor map node id not found");
            }

            info!(
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
            ).await.unwrap();

            match res {
                Ok(response) => {}
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

            info!(
                "register session actor map succeed tenant_id: {}, client_id: {}, node_id: {}",
                msg.tenant_id, msg.client_id, current_node_id
            );
            //

            let mut sessions_guard = sessions.write().await;

            let mut session_state = Arc::new(RwLock::new(SessionState::new(Duration::from_secs(
                settings.mqtt.inflight_retry_interval_secs,
            ))));

            let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

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
                ).await.unwrap();

                if let Ok(res) = res {
                    if res.is_some() {
                        let connack = ConnAckPacketBuilder::new()
                            .set_return_code(ConnackReturnCode::Accpet)
                            .set_session_present(true)
                            .build();
                        msg.connection_addr
                            .send(ConnectionActorMessage::WritePacketToClient(
                                MqttPacketV3::Connack(connack),
                            ))
                            .await
                            .unwrap();
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
                            return Ok(session_recipient.session_actor_message_recipient.clone());
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
                        let res = session_state_raft_actor_addr.send(
                            crate::raft::session_state::session_state_raft_actor::CreateSessionState {
                                tenant_id: msg.tenant_id.clone(),
                                client_id: msg.client_id.clone(),
                            },
                        ).await.unwrap();

                        if res.is_err() {
                            error!("create session state failed: {}", res.unwrap_err());
                            let connack = ConnAckPacketBuilder::new()
                                .set_return_code(ConnackReturnCode::ServerUnavailable)
                                .set_session_present(false)
                                .build();
                            msg.connection_addr
                                .send(ConnectionActorMessage::WritePacketToClient(
                                    MqttPacketV3::Connack(connack),
                                ))
                                .await
                                .unwrap();
                        } else {
                            let connack = ConnAckPacketBuilder::new()
                                .set_return_code(ConnackReturnCode::Accpet)
                                .set_session_present(false)
                                .build();
                            msg.connection_addr
                                .send(ConnectionActorMessage::WritePacketToClient(
                                    MqttPacketV3::Connack(connack),
                                ))
                                .await
                                .unwrap();
                        }
                    }

                }

            } else {
                let connack = ConnAckPacketBuilder::new()
                    .set_return_code(ConnackReturnCode::Accpet)
                    .set_session_present(false)
                    .build();
                msg.connection_addr
                    .send(ConnectionActorMessage::WritePacketToClient(
                        MqttPacketV3::Connack(connack),
                    ))
                    .await
                    .unwrap();
            }
            let session_actor = SessionActor::new(
                msg.tenant_id.clone(),
                msg.client_id.clone(),
                msg.clean_session,
                plugin_manager.get().unwrap().clone(),
                50,
                msg.will_message,
                msg.keep_alive,
                msg.connection_addr,
                msg.peer_addr,
                session_state,
                session_lifecycle_tx,
                current_node_id,
            );

            let session_actor_addr = session_actor.start();
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
            return Ok(session_actor_message_recipient);
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
    type Result = Result<(), SessionManagerError>;

    fn handle(&mut self, msg: CreateTenantMessage, _ctx: &mut Self::Context) -> Self::Result {
        if self.tenant_existed(&msg.tenant_id) {
            return Err(SessionManagerError::TenantHasExisted(msg.tenant_id));
        }
        self.sessions
            .insert(msg.tenant_id.into(), Arc::new(RwLock::new(HashMap::new())));
        Ok(())
    }
}

impl SessionManagerActor {
    fn tenant_existed(&self, tenant_identifier: &str) -> bool {
        return self.sessions.contains_key(tenant_identifier);
    }

    fn create_tenant(&mut self, tenant_identifier: &str) {
        self.sessions.insert(
            tenant_identifier.to_string(),
            Arc::new(RwLock::new(HashMap::new())),
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
    type Result = Result<(), SessionManagerError>;

    fn handle(&mut self, msg: RemoveSessionMessage, ctx: &mut Self::Context) -> Self::Result {
        let tenant_sessions = self.sessions.get_mut(&msg.tenant_id);
        if let Some(sessions) = tenant_sessions {
            let sessions = sessions.clone();
            async move {
                let mut sessions_guard = sessions.write().await;
                sessions_guard.remove(&msg.client_id);
                info!(
                    "received RemoveSessionMessage, remove session {} from tenant {} succeed",
                    msg.client_id, msg.tenant_id
                );
            }
            .into_actor(self)
            .wait(ctx);
        } else {
            return Err(SessionManagerError::TenantNotExisted(msg.tenant_id));
        }
        Ok(())
    }
}
