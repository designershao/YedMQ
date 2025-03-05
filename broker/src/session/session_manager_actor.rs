use std::{collections::HashMap, sync::Arc};

use crate::{plugin_manager::PluginService, router::RouterCmd, session::session_actor::SessionActor, settings::Settings, topic::topic_manager::TopicManager};
use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, WrapFuture};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc::Sender, RwLock},
};

use super::{connection::ConnectionActor, WillMessage};

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

pub struct SessionManagerActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub sessions: HashMap<String, HashMap<String, Addr<SessionActor<T>>>>,

    plugin_manager: Arc<dyn PluginService + 'static>,

    topic_manager: Arc<RwLock<TopicManager>>,

    router_sender: Sender<RouterCmd>,

    session_lifecycle_tx: Option<Sender<SessionLifecycleMessage>>,

    settings: Arc<Settings>,
}

impl<T> Actor for SessionManagerActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let (session_lifecycle_tx, mut session_lifecycle_rx) = tokio::sync::mpsc::channel(10);
        self.session_lifecycle_tx = Some(session_lifecycle_tx);
        let self_addr = ctx.address();
        let future = async move {
            while let Some(msg) = session_lifecycle_rx.recv().await {
                match msg {
                    SessionLifecycleMessage::SessionStarted => {},
                    SessionLifecycleMessage::SessionActivate => {},
                    SessionLifecycleMessage::SessionDeactivate => {},
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

pub struct CreateSessionMessage<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub tenant_id: String,
    pub client_id: String,
    pub clean_session: bool,
    pub connection_addr: Addr<ConnectionActor<T>>,
    pub keep_alive: u64,
    pub will_message: Option<WillMessage>,
    pub username: Option<String>,
    pub peer_addr: std::net::SocketAddr,
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Message for CreateSessionMessage<T> {
    type Result = Result<Addr<SessionActor<T>>, SessionManagerError>;
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Handler<CreateSessionMessage<T>>
    for SessionManagerActor<T>
{
    type Result = Result<Addr<SessionActor<T>>, SessionManagerError>;

    fn handle(&mut self, msg: CreateSessionMessage<T>, ctx: &mut Self::Context) -> Self::Result {
        if !self.tenant_existed(&msg.tenant_id) {
            return Err(SessionManagerError::TenantNotExisted(msg.tenant_id));
        }
        let sessions = self.sessions.get_mut(&msg.tenant_id).unwrap();
        if sessions.contains_key(&msg.client_id) {
            return Err(SessionManagerError::SessionHasExisted(msg.client_id));
        }
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
            msg.peer_addr
        );

        let session_actor_addr = session_actor.start();
        sessions.insert(msg.client_id.clone(), session_actor_addr.clone());
        Ok(session_actor_addr)
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), SessionManagerError>")]
struct CreateTenantMessage {
    tenant_id: String,
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Handler<CreateTenantMessage>
    for SessionManagerActor<T>
{
    type Result = Result<(), SessionManagerError>;

    fn handle(&mut self, msg: CreateTenantMessage, ctx: &mut Self::Context) -> Self::Result {
        if self.tenant_existed(&msg.tenant_id) {
            return Err(SessionManagerError::TenantHasExisted(msg.tenant_id));
        }
        self.sessions.insert(msg.tenant_id.into(), HashMap::new());
        Ok(())
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> SessionManagerActor<T> {
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

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Handler<RemoveSessionMessage>
    for SessionManagerActor<T>
{
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
