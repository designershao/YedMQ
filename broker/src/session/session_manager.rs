use std::{collections::HashMap, sync::Arc, time::Duration};

use log::warn;
use samoye_mqtt::MqttPacketV3;
use tokio::{io::{AsyncRead, AsyncWrite}, select, sync::{mpsc::{Receiver, Sender}, Mutex, RwLock}};
use anyhow::Result;

use crate::{connection::Connection, inflight::Inflight};

use super::WillMessage;

pub enum ConnectionMessage{

    WritePacket(MqttPacketV3),

    Disconnect
}

pub enum KeepAliveMessage {
    Trigger,
    Stop
}

pub struct SessionContext {

    // MQTT Auth Username
    pub username: Option<String>,

    // Current Session Connection
    pub connection: Sender<ConnectionMessage>,

    // MQTT Keep Alive
    pub keep_alive: u64,

}

pub struct Session {

    // MQTT Will Message
    pub will_message: Option<WillMessage>,

    // MQTT Client Identifier, unique in the tenant
    pub client_identifier: String,

    // Tenant Identifier, unique in the system
    pub tenant_identifier: String,

    // The session subscribed topics
    pub subscription_topics: Vec<String>,

    // The inflight , track all in flights qos packet.
    pub inflight: Inflight,

}

struct SessionWrapper {

    session: Session,

    context: Option<SessionContext>,

    state: SessionState,

    session_receiver: Receiver<SessionMessage>

}

impl SessionWrapper {

    pub async fn keep_alive_task(sender: Sender<SessionMessage>, keep_alive: u64) -> Sender<KeepAliveMessage> {
        let mut keep_alive_interval = tokio::time::interval(Duration::from_secs(keep_alive));
        let (mut keep_alive_sender, mut keep_alive_receiver) = tokio::sync::mpsc::channel(1);
        tokio::task::spawn(async move {
            let mut received = false;
            loop {
                select! {
                    _ = keep_alive_interval.tick() => {
                        if received {
                            received = false;
                        } else {
                            if let Err(_) = sender.send(SessionMessage::KickOff).await {
                                warn!("session sender dropped");
                            }
                        }
                    }
                    msg = keep_alive_receiver.recv() => {
                        match msg {
                            Some(KeepAliveMessage::Trigger) => {
                                received = true
                            },
                            Some(KeepAliveMessage::Stop) => {
                                break;
                            },
                            None => todo!(),
                        }
                    }
                }
            }
        });
        keep_alive_sender
    }
    
    pub async fn run_event_loop(
        &mut self,
        mut session_receiver:Receiver<SessionMessage>,
    ) {
        loop {
            match self.state {
                SessionState::Activate => {
                    select! {
                        msg = session_receiver.recv() => {
                            match msg {
                                Some(session_message) => {
                                    match session_message {
                                        SessionMessage::Activate(session_context) => {
                                            self.context = Some(session_context);
                                        }
                                        SessionMessage::InActivate => {
                                            self.state = SessionState::Inactivate;
                                        }
                                        SessionMessage::ForwardFromRouter(packet) => {
                                            // TODO: write to connection sender
                                        }
                                        SessionMessage::ReceiveFromClient(packet) => {
                                            // TODO: write to connection sender
                                        }
                                        SessionMessage::KickOff => {
                                            self.state = SessionState::Inactivate;
                                            if let Some(context) = &self.context {
                                                if let Err(_) = context.connection.send(ConnectionMessage::Disconnect).await {
                                                    warn!("connection receiver dropped");
                                                }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                None => {
                                }
                            }
                        }
                    }
                },
                SessionState::Inactivate => todo!(),
            }
        }
    }
}


pub enum SessionMessage {

    ForwardFromRouter(MqttPacketV3), //Receive packet from router

    ReceiveFromClient(MqttPacketV3), //Receive packet from client

    KickOff,

    Activate(SessionContext),

    InActivate
}

pub enum SessionState {

    Activate,

    Inactivate

}

pub struct SessionManager {

    active_sessions: HashMap<String, RwLock<HashMap<String, SessionWrapper>>>,

    inactivate_sessions: HashMap<String, RwLock<HashMap<String, SessionWrapper>>>,

}

impl SessionManager
{

    // Get session state
    pub async fn get_session_state(&self, tenant_identifier: &str, client_identifier: &str) -> Option<SessionState> {
        if self.tenant_existed(tenant_identifier) {
            let active_sessions = self.active_sessions.get(tenant_identifier).unwrap().read().await;
            let inactivate_sessions = self.inactivate_sessions.get(tenant_identifier).unwrap().read().await;
            if active_sessions.contains_key(client_identifier) {
                Some(SessionState::Activate)
            } else if inactivate_sessions.contains_key(client_identifier) {
                Some(SessionState::Inactivate)
            } else {
                None
            }
        } else {
            None
        }
    }

    // Create a new tenant
    pub fn create_tenant(&mut self, tenant_identifier: &str) -> Result<()> {
        if !self.active_sessions.contains_key(tenant_identifier) {
            self.active_sessions
                .insert(tenant_identifier.to_string(), RwLock::new(HashMap::new()));
        }
        if !self.inactivate_sessions.contains_key(tenant_identifier) {
            self.inactivate_sessions
                .insert(tenant_identifier.to_string(), RwLock::new(HashMap::new()));
        }
        Ok(())
    }

    // Check if a tenant existed
    fn tenant_existed(&self, tenant_identifier: &str) -> bool {
        return self.active_sessions.contains_key(tenant_identifier);
    }
}