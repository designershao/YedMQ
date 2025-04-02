use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    plugin_manager::PluginService,
    protobuf::{raft_service_client::RaftServiceClient, ForceSessionDisconnectRequest},
    raft::{
        raft_manager::{RaftManager, RaftManagerError, RaftManagerTrait},
        NodeId,
    },
    router::RouterCmd,
    session::session_actor::SessionActor,
    settings::Settings,
    topic::topic_manager::TopicManagerTrait,
};
use actix::{
    dev::ContextFutureSpawner, Actor, AsyncContext, Context, Handler, Message, Recipient,
    ResponseFuture, WrapFuture,
};
use log::{error, info};
use thiserror::Error;
use tokio::sync::{mpsc::Sender, RwLock};
use yedmq_mqtt::MqttPacketV3;

use super::{
    session_actor::{GetSessionInfo, SessionActorMessage, SessionInfo},
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
    RaftErr(#[from] RaftManagerError),
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
}

pub struct SessionManagerActor {
    sessions: HashMap<String, Arc<RwLock<HashMap<String, SessionActorRecipientWrapper>>>>,

    raft_manager: Arc<dyn RaftManagerTrait>,

    plugin_manager: Arc<dyn PluginService + 'static>,

    topic_manager: Arc<RwLock<dyn TopicManagerTrait>>,

    router_sender: Sender<RouterCmd>,

    session_lifecycle_tx: Option<Sender<SessionLifecycleMessage>>,

    settings: Arc<Settings>,
}

impl SessionManagerActor {
    pub fn new(
        plugin_manager: Arc<dyn PluginService + 'static>,
        topic_manager: Arc<RwLock<dyn TopicManagerTrait>>,
        router_sender: Sender<RouterCmd>,
        settings: Arc<Settings>,
        raft_manager: Arc<dyn RaftManagerTrait>,
    ) -> SessionManagerActor {
        SessionManagerActor {
            sessions: HashMap::new(),
            plugin_manager,
            topic_manager,
            router_sender,
            session_lifecycle_tx: None,
            settings,
            raft_manager,
        }
    }
}

impl Actor for SessionManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let (session_lifecycle_tx, mut session_lifecycle_rx) = tokio::sync::mpsc::channel(10);
        self.session_lifecycle_tx = Some(session_lifecycle_tx);
        let self_addr = ctx.address();
        let future = async move {
            while let Some(msg) = session_lifecycle_rx.recv().await {
                match msg {
                    SessionLifecycleMessage::SessionStarted => {}
                    SessionLifecycleMessage::SessionActivate => {}
                    SessionLifecycleMessage::SessionDeactivate => {}
                    SessionLifecycleMessage::SessionStopped {
                        tenant_id,
                        client_id,
                    } => self_addr.do_send(RemoveSessionMessage {
                        tenant_id,
                        client_id,
                    }),
                }
            }
        };
        ctx.spawn(future.into_actor(self));
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
        let tenant_session = self.sessions.get(&msg.tenant_id).unwrap().clone();
        async move {
            let session = tenant_session.read().await;
            let session = session.get(&msg.client_id);
            if let Some(session) = session {
                session
                    .session_actor_message_recipient
                    .do_send(SessionActorMessage::OutboundMessage(msg.packet));
            }
        }
        .into_actor(self)
        .wait(ctx);
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
pub struct ForceStop {
    pub tenant_id: String,
    pub client_id: String,
}

impl Handler<ForceStop> for SessionManagerActor {
    type Result = ResponseFuture<Result<(), SessionManagerError>>;

    fn handle(&mut self, msg: ForceStop, ctx: &mut Self::Context) -> Self::Result {
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
                info!("force stop session {} remove from local node session map", msg.client_id);
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
    raft_manager: Arc<dyn RaftManagerTrait>,
    tenant_id: String,
    client_id: String,
) -> bool {
    let max_retries = 3;
    let node = raft_manager
        .session_actor_map_raft()
        .get_node_by_id(node_id)
        .await
        .unwrap();

    let addr = format!("http://{}", node.rpc_addr);

    let mut client = RaftServiceClient::connect(addr.clone()).await.unwrap();

    for i in 0..max_retries {
        let res = client
            .session_force_disconnect(ForceSessionDisconnectRequest {
                tenant_id: tenant_id.clone(),
                client_id: client_id.clone(),
            })
            .await;
        if res.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_secs(2 ^ i)).await;
    }
    false
}

impl Handler<CreateSessionMessage> for SessionManagerActor {
    type Result = ResponseFuture<Result<Recipient<SessionActorMessage>, SessionManagerError>>;

    fn handle(&mut self, msg: CreateSessionMessage, ctx: &mut Self::Context) -> Self::Result {
        if !self.tenant_existed(&msg.tenant_id) {
            self.create_tenant(&msg.tenant_id);
            //return Err(SessionManagerError::TenantNotExisted(msg.tenant_id));
        }

        let sessions = self.sessions.get(&msg.tenant_id).unwrap().clone();
        let topic_manager = self.topic_manager.clone();
        let plugin_manager = self.plugin_manager.clone();
        let router_sender = self.router_sender.clone();
        let settings = self.settings.clone();

        let raft_manager = self.raft_manager.clone();

        let future = async move {
            // Force previous session to disconnect if exists
            let session_actor_map_node_id = raft_manager
                .session_actor_map_raft()
                .get_session_actor_map_node_id(&msg.tenant_id, &msg.client_id)
                .await;

            if let Some(node_id) = session_actor_map_node_id {
                if !call_force_disconnect(
                    node_id,
                    raft_manager.clone(),
                    msg.tenant_id.clone(),
                    msg.client_id.clone(),
                )
                .await
                {
                    raft_manager
                        .clone()
                        .session_actor_map_raft()
                        .unregister_session_actor_map(&msg.tenant_id, &msg.client_id, node_id, false)
                        .await
                        .unwrap();
                }
            }

            let current_node_id = raft_manager.session_actor_map_raft().current_node_id();

            let res = raft_manager
                .session_actor_map_raft()
                .register_session_actor_map(&msg.tenant_id, &msg.client_id, current_node_id)
                .await;

            if res.is_err() {
                return Err(SessionManagerError::RaftErr(res.unwrap_err()));
            }
            //

            let mut sessions_guard = sessions.write().await;

            let mut session_state = Arc::new(RwLock::new(SessionState::new(Duration::from_secs(
                settings.mqtt.inflight_retry_interval_secs,
            ))));

            if !msg.clean_session {
                // If raft store not existed, create new session state
                // First check local sessions if exists send reconect
                // If local sessions not existed, recover from raft store
                let session_state_exist = raft_manager
                    .session_state_raft()
                    .session_state_exists(&msg.tenant_id, &msg.client_id)
                    .await;
                if session_state_exist {
                    // check session in current node
                    if sessions_guard.contains_key(&msg.client_id) {
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
                        // in current node, send reconnect
                    } else {
                        // not in current node, recover from raft
                        let session_state_from_raft = raft_manager
                            .session_state_raft()
                            .get_session_state(&msg.tenant_id, &msg.client_id)
                            .await
                            .unwrap();
                        session_state = Arc::new(RwLock::new(session_state_from_raft));
                    }
                    //
                } else {
                    raft_manager
                        .session_state_raft()
                        .create_session_state(
                            &msg.tenant_id,
                            &msg.client_id,
                            settings.mqtt.sys_topic_interval_secs,
                        )
                        .await;
                }
            }
            let session_actor = SessionActor::new(
                msg.tenant_id.clone(),
                msg.client_id.clone(),
                msg.clean_session,
                topic_manager.clone(),
                plugin_manager.clone(),
                router_sender.clone(),
                50,
                msg.will_message,
                msg.keep_alive,
                msg.connection_addr,
                msg.peer_addr,
                session_state,
                raft_manager
            );

            let session_actor_addr = session_actor.start();
            let session_actor_message_recipient = session_actor_addr.clone().recipient();
            let get_session_info_recipient = session_actor_addr.clone().recipient();
            sessions_guard.insert(
                msg.client_id.clone(),
                SessionActorRecipientWrapper {
                    session_actor_message_recipient: session_actor_message_recipient.clone(),
                    get_session_info_recipient,
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
            }
            .into_actor(self)
            .wait(ctx);
        } else {
            return Err(SessionManagerError::TenantNotExisted(msg.tenant_id));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
