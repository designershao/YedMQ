use std::{collections::HashMap, sync::Arc};

use crate::{
    plugin_manager::PluginService, router::RouterCmd, session::session_actor::SessionActor,
    settings::Settings, topic::topic_manager::TopicManager,
};
use actix::{
    dev::ContextFutureSpawner, Actor, ActorFutureExt, AsyncContext, Context, Handler, Message,
    Recipient, WrapFuture,
};
use log::error;
use thiserror::Error;
use tokio::sync::{mpsc::Sender, RwLock};

use super::{connection::ConnectionActorMessage, session_actor::SessionActorMessage, WillMessage};

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

pub struct SessionManagerActor {
    pub sessions: HashMap<String, HashMap<String, Recipient<SessionActorMessage>>>,

    plugin_manager: Arc<dyn PluginService + 'static>,

    topic_manager: Arc<RwLock<TopicManager>>,

    router_sender: Sender<RouterCmd>,

    session_lifecycle_tx: Option<Sender<SessionLifecycleMessage>>,

    settings: Arc<Settings>,
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

impl Handler<CreateSessionMessage> for SessionManagerActor {
    type Result = Result<Recipient<SessionActorMessage>, SessionManagerError>;

    fn handle(&mut self, msg: CreateSessionMessage, ctx: &mut Self::Context) -> Self::Result {
        if !self.tenant_existed(&msg.tenant_id) {
            return Err(SessionManagerError::TenantNotExisted(msg.tenant_id));
        }

        let sessions = self.sessions.get(&msg.tenant_id).unwrap();
        if sessions.contains_key(&msg.client_id) {
            // previously session existed, force disconnect the old connection
            let session_addr = sessions.get(&msg.client_id).unwrap().clone();
            let session_addr_in_async = session_addr.clone();
            async move {
                let res = session_addr_in_async
                    .send(SessionActorMessage::ForceDisconnect)
                    .await;
                if let Err(err) = res {
                    return Err(err);
                } else {
                    let res = session_addr_in_async
                        .send(SessionActorMessage::Reconnect {
                            conn: msg.connection_addr.clone(),
                            keep_alive: msg.keep_alive,
                            clean_session: msg.clean_session,
                            username: msg.username,
                            will_message: msg.will_message,
                            socket_addr: msg.peer_addr,
                        })
                        .await;
                    if let Err(err) = res {
                        return Err(err);
                    }
                }
                Ok(())
            }
            .into_actor(self)
            .map(|res, act, _ctx| {
                if let Err(err) = res {
                    error!("reconnect previeus session error: {}", err);
                }
            })
            .wait(ctx);
            return Ok(session_addr);
        } else {
            let sessions = self.sessions.get_mut(&msg.tenant_id).unwrap();
            let session_actor = SessionActor::new(
                msg.tenant_id.clone(),
                msg.client_id.clone(),
                msg.clean_session,
                self.topic_manager.clone(),
                self.plugin_manager.clone(),
                self.router_sender.clone(),
                50,
                msg.will_message,
                msg.keep_alive,
                msg.connection_addr,
                msg.peer_addr,
            );

            let session_actor_addr = session_actor.start();
            let recipient = session_actor_addr.recipient();
            sessions.insert(msg.client_id.clone(), recipient.clone());
            return Ok(recipient);
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), SessionManagerError>")]
struct CreateTenantMessage {
    tenant_id: String,
}

impl Handler<CreateTenantMessage> for SessionManagerActor {
    type Result = Result<(), SessionManagerError>;

    fn handle(&mut self, msg: CreateTenantMessage, ctx: &mut Self::Context) -> Self::Result {
        if self.tenant_existed(&msg.tenant_id) {
            return Err(SessionManagerError::TenantHasExisted(msg.tenant_id));
        }
        self.sessions.insert(msg.tenant_id.into(), HashMap::new());
        Ok(())
    }
}

impl SessionManagerActor {
    fn tenant_existed(&self, tenant_identifier: &str) -> bool {
        return self.sessions.contains_key(tenant_identifier);
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
        if tenant_sessions.is_some() {
            tenant_sessions.unwrap().remove(&msg.client_id);
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
