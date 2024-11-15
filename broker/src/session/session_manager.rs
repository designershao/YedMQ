use std::{collections::HashMap, sync::Arc, time::Duration};

use log::{info, warn};
use samoye_mqtt::{v3::{pingresp::PingrespPacket, publish::PublishPacket}, MqttPacketV3};
use samoye_plugin::plugin::{Client, ClientProperties};
use tokio::{select, sync::{mpsc::{Receiver, Sender}, RwLock}};
use anyhow::Result;

use crate::{inflight::Inflight, plugin_manager::PluginManager, router::RouterCmd, topic::TopicManager};

use super::WillMessage;

pub enum ConnectionMessage{

    WritePacket(MqttPacketV3),

    Disconnect
}

enum KeepAliveMessage {

    Trigger,

    Stop

}

#[derive(Debug, PartialEq)]
pub enum KickOffReason {

    KeepAliveExpired,

    Other(String)
}

// Represent session message
pub enum SessionMessage {

    ForwardFromRouter(MqttPacketV3), //Receive packet from router

    ReceiveFromClient(MqttPacketV3), //Receive packet from client

    Activate(SessionContext),

    InActivate,

    KickOff(KickOffReason), // notify session to disconnect current connection with some reason

}


pub struct SessionContext {

    // MQTT Auth Username
    pub username: Option<String>,

    // Current Session Connection
    pub connection: Sender<ConnectionMessage>,

    // MQTT Keep Alive
    pub keep_alive: u64,

    // MQTT Client Info For Plugin
    pub client_info: Client,

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

impl Session {
    async fn new_tx_qos_state_ctx(&self, packet: &MqttPacketV3) {
        self.inflight.register_with_tx_packet(packet).await;
    }

    async fn new_rx_qos_state_ctx(&self, packet: &MqttPacketV3) {
        self.inflight.register_with_rx_packet(packet).await;
    }
}

struct SessionWrapper {

    session: Session,

    context: Option<SessionContext>,

    state: SessionState,

    session_receiver: Receiver<SessionMessage>,

    keep_alive_sender: Option<Sender<KeepAliveMessage>>,

    plugin_manager: Arc<PluginManager>,

    topic_manager: Arc<RwLock<TopicManager>>,

}

// Run keep alive task, when not received pingresp in keep alive time, send kick off to current session
async fn keep_alive_task(sender: Sender<SessionMessage>, keep_alive: u64) -> Sender<KeepAliveMessage> {
    let (keep_alive_sender, mut keep_alive_receiver) = tokio::sync::mpsc::channel(10);
    tokio::task::spawn(async move {
        let mut keep_alive_interval = tokio::time::interval(Duration::from_secs(keep_alive));
        let mut received = false;
        let mut tick_first_raise = true;
        loop {
            select! {
                _ = keep_alive_interval.tick() => {
                    if !tick_first_raise {
                        if received {
                            received = false;
                        } else {
                            // not received pingresp, disconnect
                            if let Err(_) = sender.send(SessionMessage::KickOff(KickOffReason::KeepAliveExpired)).await {
                                warn!("session sender dropped");
                            }
                            //
                        }
                    } else {
                        tick_first_raise = false;
                    }
                }
                msg = keep_alive_receiver.recv() => {
                    match msg {
                        Some(KeepAliveMessage::Trigger) => {
                            received = true
                        },
                        Some(KeepAliveMessage::Stop) => {
                            keep_alive_receiver.close();
                        },
                        None => {
                            // receiver closed, return the task
                            break;
                        },
                    }
                }
            }
        }
    });
    keep_alive_sender
}

impl SessionWrapper {

    // Process publish packet which received from the client connection
    async fn process_publish_packet(&self, packet: &PublishPacket, connection_sender: &Sender<ConnectionMessage>, router_sender: &Sender<RouterCmd>) -> Result<()> {

        let publish_authorization  = self.plugin_manager.do_publish_authorizate(&self.context.as_ref().unwrap().client_info, packet);

        let allow_publish =match publish_authorization {
            Ok(allow) => {
                allow
            },
            Err(e) => { // if plugin do publish_authorizate error, default allow publish
                warn!("publish_authorizate error: {}", e);
                true
            }
        };

        if allow_publish{
            self.plugin_manager.do_on_publish(&self.context.as_ref().unwrap().client_info, &packet);

            if packet.fix_header.qos > Some(0) {
                self.session.new_rx_qos_state_ctx(&MqttPacketV3::Publish(packet.clone())).await;

                let packet = self.session
                    .inflight
                    .get_current_packet(packet.variable_header.packet_identifier.unwrap())
                    .await
                    .unwrap();

                if let Err(e) = connection_sender.send(ConnectionMessage::WritePacket(packet)).await {
                    warn!("write packet error");
                    return Err(anyhow::format_err!("write packet error: {}", e));
                }
            } 
            router_sender.send(RouterCmd::RoutePacket(self.session.tenant_identifier.clone() ,MqttPacketV3::Publish(packet.clone()))).await.unwrap();
        }
        Ok(())
    }
    
    pub async fn run_event_loop(
        &mut self,
        mut session_receiver:Receiver<SessionMessage>,
        session_sender:Sender<SessionMessage>,
        router_sender:Sender<RouterCmd>,
    ) {
        loop {
            match self.state {
                SessionState::Activate => {
                    select! {
                        msg = session_receiver.recv() => {
                            match msg {
                                Some(session_message) => {
                                    match session_message {
                                        SessionMessage::InActivate => {
                                            self.state = SessionState::Inactivate;
                                            if let Some(keep_alive_sender) = &self.keep_alive_sender {
                                                if let Err(_) = keep_alive_sender.send(KeepAliveMessage::Stop).await {
                                                    warn!("keep alive sender dropped when send stop message");
                                                }
                                            }
                                        }
                                        SessionMessage::ForwardFromRouter(packet) => {
                                            // TODO: write to connection sender
                                        }
                                        SessionMessage::ReceiveFromClient(packet) => {
                                            // trigger keep alive
                                            if let Some(keep_alive_sender) = &self.keep_alive_sender {
                                                if let Err(_) = keep_alive_sender.send(KeepAliveMessage::Trigger).await {
                                                    warn!("keep alive sender dropped");
                                                }
                                            }
                                            //

                                            //
                                            match packet {
                                                MqttPacketV3::Publish(publish_packet) => {
                                                    if let Some(context) = &self.context {
                                                        let _ = self.process_publish_packet(&publish_packet, &context.connection, &router_sender);
                                                    }
                                                }
                                                MqttPacketV3::Pingreq(_) => {
                                                    if let Some(context) = &self.context {
                                                        context.connection.send(ConnectionMessage::WritePacket(MqttPacketV3::Pingresp(PingrespPacket::new()))).await.unwrap();
                                                    }
                                                }
                                                _ => {}
                                            }
                                            //

                                            // TODO: write to connection sender
                                        }
                                        SessionMessage::KickOff(some_reason) => {
                                            if let Some(context) = &self.context {
                                                if let Err(_) = context.connection.send(ConnectionMessage::Disconnect).await {
                                                    warn!("connection receiver dropped");
                                                }
                                            }
                                            self.state = SessionState::Inactivate;
                                            self.context = None;
                                            match some_reason {
                                                KickOffReason::KeepAliveExpired => todo!("send will message to all subscribed client"),
                                                KickOffReason::Other(_) => todo!(),
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
                SessionState::Inactivate => {
                    select! {
                        msg = session_receiver.recv() => {
                            match msg {
                                Some(session_message) => {
                                    match session_message {
                                        SessionMessage::Activate(session_context) => {
                                            self.context = Some(session_context);
                                            self.state = SessionState::Activate;
                                            let keep_alive_sender =keep_alive_task(session_sender.clone(), self.context.as_ref().unwrap().keep_alive).await;
                                            self.keep_alive_sender = Some(keep_alive_sender);
                                        }
                                        _ => {}
                                    }
                                }
                                None => {}
                            }
                        }
                    }
                },
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::session::session_manager::{keep_alive_task, KeepAliveMessage, KickOffReason};

    /// Test that the keep alive task should send a SessionMessage::KickOff to the session when keep alive expired.
    #[tokio::test]
    pub async fn test_keep_alive_expired_send_kick_off() {
        let test_keep_alive = 2;

        let (session_sender, mut session_receiver) = tokio::sync::mpsc::channel(100);

        let _keep_alive_sender = keep_alive_task(session_sender.clone(), test_keep_alive).await;

        tokio::time::sleep(Duration::from_secs(test_keep_alive + 1)).await; // wait keep alive expired

        let msg = session_receiver.recv().await;

        if let Some(crate::session::session_manager::SessionMessage::KickOff(reason)) = msg {
            assert_eq!(reason, KickOffReason::KeepAliveExpired);
        } else {
            assert!(false);
        }
    }

    #[tokio::test]
    pub async fn test_keep_alive_trigger() {
        let test_keep_alive = 2;

        let (session_sender,  _session_receiver) = tokio::sync::mpsc::channel(100);

        let keep_alive_sender = keep_alive_task(session_sender.clone(), test_keep_alive).await;

        tokio::time::sleep(Duration::from_secs(test_keep_alive - 1)).await;

        keep_alive_sender.send(KeepAliveMessage::Trigger).await.unwrap();

        tokio::time::sleep(Duration::from_secs(test_keep_alive - 1)).await;

        keep_alive_sender.send(KeepAliveMessage::Stop).await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(keep_alive_sender.is_closed());

    }
}