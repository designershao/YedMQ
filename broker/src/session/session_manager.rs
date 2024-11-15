use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use log::{info, warn};
use samoye_mqtt::{
    v3::{pingresp::PingrespPacket, publish::PublishPacket, suback::SubackPacket, subscribe::SubscribePacket, unsubscribe::UnsubscribePacket},
    MqttPacketV3,
};
use samoye_plugin::plugin::{Client, SubscribeReturnCode};
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
};

use crate::{
    inflight::Inflight,
    plugin_manager::{PluginManager, PluginService},
    router::RouterCmd,
    topic::{self, TopicManager},
};

use super::WillMessage;

pub enum ConnectionMessage {
    WritePacket(MqttPacketV3),

    Disconnect,
}

enum KeepAliveMessage {
    Trigger,

    Stop,
}

#[derive(Debug, PartialEq)]
pub enum KickOffReason {
    KeepAliveExpired,

    Other(String),
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

    plugin_manager: Arc<dyn PluginService>,

    topic_manager: Arc<RwLock<TopicManager>>,
}

// Run keep alive task, when not received pingresp in keep alive time, send kick off to current session
async fn keep_alive_task(
    sender: Sender<SessionMessage>,
    keep_alive: u64,
) -> Sender<KeepAliveMessage> {
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

fn is_allowd_subscribe(subscribe_return_code: &SubscribeReturnCode) -> bool {
    !(matches!(subscribe_return_code, SubscribeReturnCode::Invalid)
        || matches!(subscribe_return_code, SubscribeReturnCode::Failure))
}

impl SessionWrapper {

    async fn process_unsubscribe_packet(
        &mut self,
        unsubscribe_packet: &UnsubscribePacket,
        connection_sender: &Sender<ConnectionMessage>,
    ) -> Result<()> {
        let unsub_topic_filters = &unsubscribe_packet.payload.topic_filters;
        {
            let mut topic_manager = self.topic_manager.write().await;
            for topic in unsub_topic_filters {
                let _ = topic_manager.unsubscription(
                    self.session.tenant_identifier.clone(),
                    self.session.client_identifier.clone(),
                    topic.topic_name.clone(),
                );
            }
        }
        let unsub_ack =
            MqttPacketV3::Unsuback(samoye_mqtt::v3::unsuback::UnSubackPacket::new(
                unsubscribe_packet.variable_header.packet_identifier,
            ));
            
        connection_sender.send(ConnectionMessage::WritePacket(unsub_ack)).await.unwrap();

        Ok(())
    }

    // Process subscribe packet which received from the client connection
    async fn process_subscribe_packet(
        &mut self,
        subscribe_packet: &SubscribePacket,
        connection_sender: &Sender<ConnectionMessage>,
    ) -> Result<()> {
        let subscriptions = &subscribe_packet.payload.topic_filters;

        let packet_identifier = &subscribe_packet.variable_header.packet_identifier;

        let mut retain_messages: Vec<Arc<MqttPacketV3>> = vec![];

        let topic_authorizate_result = self.plugin_manager.do_subscribe_authorizate(
            &self.context.as_ref().unwrap().client_info,
            &subscribe_packet,
        );

        let mut return_code: Vec<samoye_mqtt::v3::suback::ReturnCode> = vec![];
        if let Ok(topic_authorizate_result) = topic_authorizate_result {
            let plugin_return_code = topic_authorizate_result.return_code;
            for i in 0..subscriptions.len() {
                let mut topic_manager = self.topic_manager.write().await;
                let topic = subscribe_packet.payload.topic_filters[i].clone();
                if is_allowd_subscribe(&plugin_return_code[i]) {
                    let sub_result = topic_manager.subscription(
                        self.session.tenant_identifier.clone(),
                        self.session.client_identifier.clone(),
                        topic.topic_name.clone(),
                        topic.qos,
                    );
                    if let Ok(_) = sub_result {
                        match plugin_return_code[i] {
                            SubscribeReturnCode::MaxQosMostOnce => {
                                return_code.push(samoye_mqtt::v3::suback::ReturnCode::MaxQos0);
                            }
                            SubscribeReturnCode::MaxQosLeastOnce => {
                                return_code.push(samoye_mqtt::v3::suback::ReturnCode::MaxQos1);
                            }
                            SubscribeReturnCode::MaxQosExactlyOnce => {
                                return_code.push(samoye_mqtt::v3::suback::ReturnCode::MaxQos2);
                            }
                            _ => {}
                        }
                        self.session
                            .subscription_topics
                            .push(topic.topic_name.clone());
                        let packets = topic_manager.get_retain_publish_packet(
                            self.session.tenant_identifier.clone(),
                            self.session.client_identifier.clone(),
                            topic.topic_name.clone(),
                        );
                        if let Ok(packets) = packets {
                            for packet in packets {
                                retain_messages.push(packet);
                            }
                        }
                    }
                } else {
                    return_code.push(samoye_mqtt::v3::suback::ReturnCode::Failure);
                }
            }
        } else {
            warn!(
                "subscribe_authorizate error: {}",
                topic_authorizate_result.err().unwrap()
            );
        }
        for packet in retain_messages {
            let packet = (*packet).clone();
            connection_sender.send(ConnectionMessage::WritePacket(packet)).await.unwrap();
        }

        connection_sender.send(ConnectionMessage::WritePacket(MqttPacketV3::Suback(SubackPacket::new(
            *packet_identifier,
            return_code,
        ))))
        .await
        .unwrap();

        Ok(())
    }

    // Process publish packet which received from the client connection
    async fn process_publish_packet(
        &self,
        packet: &PublishPacket,
        connection_sender: &Sender<ConnectionMessage>,
        router_sender: &Sender<RouterCmd>,
    ) -> Result<()> {
        let publish_authorization = self
            .plugin_manager
            .do_publish_authorizate(&self.context.as_ref().unwrap().client_info, packet);

        let allow_publish = match publish_authorization {
            Ok(allow) => allow,
            Err(e) => {
                // if plugin do publish_authorizate error, default allow publish
                warn!("publish_authorizate error: {}", e);
                true
            }
        };

        if allow_publish {
            self.plugin_manager
                .do_on_publish(&self.context.as_ref().unwrap().client_info, &packet);

            if packet.fix_header.qos > Some(0) {
                self.session
                    .new_rx_qos_state_ctx(&MqttPacketV3::Publish(packet.clone()))
                    .await;

                let packet = self
                    .session
                    .inflight
                    .get_current_packet(packet.variable_header.packet_identifier.unwrap())
                    .await
                    .unwrap();

                if let Err(e) = connection_sender
                    .send(ConnectionMessage::WritePacket(packet))
                    .await
                {
                    warn!("write packet error");
                    return Err(anyhow::format_err!("write packet error: {}", e));
                }
            }
            router_sender
                .send(RouterCmd::RoutePacket(
                    self.session.tenant_identifier.clone(),
                    MqttPacketV3::Publish(packet.clone()),
                ))
                .await
                .unwrap();
        }
        Ok(())
    }

    pub async fn run_event_loop(
        &mut self,
        mut session_receiver: Receiver<SessionMessage>,
        session_sender: Sender<SessionMessage>,
        router_sender: Sender<RouterCmd>,
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
                                                MqttPacketV3::Puback(puback_packet) => {
                                                    if let Some(context) = &self.context {
                                                        self.session.inflight
                                                            .next_state(puback_packet.variable_header.packet_identifier)
                                                            .await;

                                                        if let Some(p) = self.session
                                                            .inflight
                                                            .get_current_packet(puback_packet.variable_header.packet_identifier)
                                                            .await
                                                        {
                                                            context.connection.send(ConnectionMessage::WritePacket(p)).await.unwrap();
                                                        }
                                                    }
                                                }
                                                MqttPacketV3::Pubrel(pubrel_packet) => {
                                                    if let Some(context) = &self.context {
                                                        self.session.inflight
                                                            .next_state(pubrel_packet.variable_header.packet_identifier)
                                                            .await;
                                                        if let Some(p) = self.session
                                                            .inflight
                                                            .get_current_packet(pubrel_packet.variable_header.packet_identifier)
                                                            .await
                                                        {
                                                            if let Err(_) = context.connection.send(ConnectionMessage::WritePacket(p)).await {
                                                                warn!("connection receiver dropped");
                                                            }
                                                        }
                                                    }
                                                }
                                                MqttPacketV3::Pubcomp(pubcomp_packet) => {
                                                    if let Some(context) = &self.context {
                                                        self.session.inflight
                                                            .next_state(pubcomp_packet.variable_header.packet_identifier)
                                                            .await;
                                                        if let Some(p) = self.session
                                                            .inflight
                                                            .get_current_packet(pubcomp_packet.variable_header.packet_identifier)
                                                            .await
                                                        {
                                                            if let Err(_) = context.connection.send(ConnectionMessage::WritePacket(p)).await {
                                                                warn!("connection receiver dropped");
                                                            }
                                                        }
                                                    }
                                                }
                                                MqttPacketV3::Subscribe(subscribe_packet) => {
                                                    if let Some(context) = &self.context {
                                                        let connection = context.connection.clone();
                                                        if let Err(_) = self.process_subscribe_packet(&subscribe_packet, &connection).await {
                                                            warn!("process subscribe packet error");
                                                        }
                                                    }
                                                }
                                                MqttPacketV3::Unsubscribe(unsubscribe_packet) => {
                                                    if let Some(context) = &self.context {
                                                        let connection = context.connection.clone();
                                                        if let Err(_) = self.process_unsubscribe_packet(&unsubscribe_packet, &connection).await {
                                                            warn!("process unsubscribe packet error");
                                                        }
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
                }
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
                }
            }
        }
    }
}

pub enum SessionState {
    Activate,

    Inactivate,
}

pub struct SessionManager {
    active_sessions: HashMap<String, RwLock<HashMap<String, SessionWrapper>>>,

    inactivate_sessions: HashMap<String, RwLock<HashMap<String, SessionWrapper>>>,
}

impl SessionManager {
    // Get session state
    pub async fn get_session_state(
        &self,
        tenant_identifier: &str,
        client_identifier: &str,
    ) -> Option<SessionState> {
        if self.tenant_existed(tenant_identifier) {
            let active_sessions = self
                .active_sessions
                .get(tenant_identifier)
                .unwrap()
                .read()
                .await;
            let inactivate_sessions = self
                .inactivate_sessions
                .get(tenant_identifier)
                .unwrap()
                .read()
                .await;
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

        let (session_sender, _session_receiver) = tokio::sync::mpsc::channel(100);

        let keep_alive_sender = keep_alive_task(session_sender.clone(), test_keep_alive).await;

        tokio::time::sleep(Duration::from_secs(test_keep_alive - 1)).await;

        keep_alive_sender
            .send(KeepAliveMessage::Trigger)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(test_keep_alive - 1)).await;

        keep_alive_sender
            .send(KeepAliveMessage::Stop)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(keep_alive_sender.is_closed());
    }
}
