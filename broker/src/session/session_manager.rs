use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};
use samoye_mqtt::{
    v3::{
        pingresp::PingrespPacket,
        publish::{PublishPacket, PublishPacketBuilder},
        suback::SubackPacket,
        subscribe::SubscribePacket,
        unsubscribe::UnsubscribePacket,
    },
    MqttPacketV3,
};
use samoye_plugin::plugin::{Client, SubscribeReturnCode};
use thiserror::Error;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        Mutex, RwLock,
    },
};

use crate::{
    inflight::Inflight, plugin_manager::PluginService, router::RouterCmd, topic::TopicManager,
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

enum InflightResendTaskMessage {
    Stop,
}

#[derive(Debug)]
pub enum KickOffReason {
    KeepAliveExpired,

    UnexpectDisconnect,

    InvalidMqttPacket,

    Other(String, Sender<()>),
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

    // MQTT Clean Session Flag
    pub clean_session: bool,
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
    pub inflight: Arc<Mutex<Inflight>>,

}

impl Session {
    async fn new_tx_qos_state_ctx(&self, packet: &MqttPacketV3) {
        let inflight = self.inflight.lock().await;
        inflight.register_with_tx_packet(packet).await;
    }

    async fn new_rx_qos_state_ctx(&self, packet: &MqttPacketV3) {
        let inflight = self.inflight.lock().await;
        inflight.register_with_rx_packet(packet).await;
    }
}

pub struct SessionWrapper {
    session: Session,

    context: Option<SessionContext>,

    state: SessionState,

    session_receiver: Receiver<SessionMessage>,

    keep_alive_sender: Option<Sender<KeepAliveMessage>>,

    inflight_resend_task_sender: Option<Sender<InflightResendTaskMessage>>,

    plugin_manager: Arc<dyn PluginService + 'static>,

    topic_manager: Arc<RwLock<TopicManager>>,

    quit_signal_sender: Option<Sender<()>>

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

async fn inflight_resend_task(
    connection_sender: Sender<ConnectionMessage>,
    inflight: Arc<Mutex<Inflight>>,
    resend_duration_secs: u64,
) -> Sender<InflightResendTaskMessage> {
    let (inflight_resend_sender, mut inflight_resend_receiver) = tokio::sync::mpsc::channel(10);
    tokio::task::spawn(async move {
        let mut resend_interval = tokio::time::interval(Duration::from_secs(resend_duration_secs));
        let mut tick_first_raise = true;

        loop {
            select! {
                _ = resend_interval.tick() => {

                    if !tick_first_raise {
                        let mut inflight = inflight.lock().await;
                        let packets = inflight.get_all_expired_packets_and_refresh_expired_time().await;
                        for packet in packets {
                            if let Err(_) = connection_sender.send(ConnectionMessage::WritePacket(packet.clone())).await {
                                warn!("in flight resend task: connection sender dropped");
                            }
                        }
                        inflight.clean_finished_items().await;
                    } else {
                        tick_first_raise = false
                    }
                }
                msg = inflight_resend_receiver.recv() => {
                    match msg {
                        Some(InflightResendTaskMessage::Stop) => {
                            inflight_resend_receiver.close();
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
    inflight_resend_sender
}

fn is_allowd_subscribe(subscribe_return_code: &SubscribeReturnCode) -> bool {
    !(matches!(subscribe_return_code, SubscribeReturnCode::Invalid)
        || matches!(subscribe_return_code, SubscribeReturnCode::Failure))
}

impl SessionWrapper {
    pub fn new(
        session: Session,
        session_receiver: Receiver<SessionMessage>,
        plugin_manager: Arc<dyn PluginService + 'static>,
        topic_manager: Arc<RwLock<TopicManager>>,
    ) -> Self {
        SessionWrapper {
            session,
            context: None,
            state: SessionState::Inactivate,
            session_receiver,
            keep_alive_sender: None,
            inflight_resend_task_sender: None,
            plugin_manager,
            topic_manager,
            quit_signal_sender: None
        }
    }

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
        let unsub_ack = MqttPacketV3::Unsuback(samoye_mqtt::v3::unsuback::UnSubackPacket::new(
            unsubscribe_packet.variable_header.packet_identifier,
        ));

        connection_sender
            .send(ConnectionMessage::WritePacket(unsub_ack))
            .await
            .unwrap();

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
            connection_sender
                .send(ConnectionMessage::WritePacket(packet))
                .await
                .unwrap();
        }

        connection_sender
            .send(ConnectionMessage::WritePacket(MqttPacketV3::Suback(
                SubackPacket::new(*packet_identifier, return_code),
            )))
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

                let inflight = self.session.inflight.lock().await;
                let packet = inflight
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

    pub async fn start_keep_alive_task(&mut self, session_sender: Sender<SessionMessage>) {
        let keep_alive_sender =
            keep_alive_task(session_sender, self.context.as_ref().unwrap().keep_alive).await;
        self.keep_alive_sender = Some(keep_alive_sender);
    }

    pub async fn start_inflight_task(&mut self, resend_duration_secs: u64) {
        let inflight_resend_sender = inflight_resend_task(
            self.context.as_ref().unwrap().connection.clone(),
            self.session.inflight.clone(),
            resend_duration_secs,
        )
        .await;
        self.inflight_resend_task_sender = Some(inflight_resend_sender);
    }

    pub async fn stop_keep_alive_task(&mut self) {
        if let Some(keep_alive_sender) = &self.keep_alive_sender {
            if let Err(_) = keep_alive_sender.send(KeepAliveMessage::Stop).await {
                warn!("keep alive sender dropped when send stop message");
            }
            self.keep_alive_sender = None;
        }
    }

    pub async fn stop_inflight_task(&mut self) {
        if let Some(inflight_sender) = &self.inflight_resend_task_sender {
            if let Err(_) = inflight_sender.send(InflightResendTaskMessage::Stop).await {
                warn!("inflight sender dropped when send stop message");
            }
            self.inflight_resend_task_sender = None;
        }
    }

    pub async fn process_will_message(&self, router_sender: &Sender<RouterCmd>) {
        if self.session.will_message.is_some() {
            let will_message = self.session.will_message.as_ref().unwrap();
            let publish_packet = PublishPacketBuilder::new(
                will_message.will_topic.clone(),
                will_message.will_message.clone(),
            )
            .retain(will_message.will_retain)
            .qos(will_message.will_qos)
            .build();
            let _ = router_sender
                .send(RouterCmd::RoutePacket(
                    self.session.tenant_identifier.clone(),
                    MqttPacketV3::Publish(publish_packet),
                ))
                .await;
        }
    }

    pub async fn run_event_loop(
        &mut self,
        session_sender: Sender<SessionMessage>,
        router_sender: Sender<RouterCmd>,
        session_manager: Arc<RwLock<SessionManager>>,
        inflight_resend_duration_secs: u64
    ) {
        loop {
            match self.state {
                SessionState::Activate => {
                    select! {
                        msg = self.session_receiver.recv() => {
                            match msg {
                                Some(session_message) => {
                                    match session_message {
                                        SessionMessage::InActivate => {
                                            self.state = SessionState::Inactivate;

                                            let clean_session = self.context.as_ref().unwrap().clean_session;

                                            self.context = None;

                                            self.stop_inflight_task().await;

                                            self.stop_keep_alive_task().await;

                                            if clean_session {
                                                debug!("session {} inactivate, clean session", self.session.client_identifier);
                                                break;
                                            }

                                        }
                                        SessionMessage::ForwardFromRouter(packet) => {
                                            if let Some(context) = &self.context {
                                                match &packet {
                                                    MqttPacketV3::Publish(publish_packet) => {
                                                        if publish_packet.fix_header.qos.unwrap() > 0 {
                                                            self.session.new_tx_qos_state_ctx(&packet).await;
                                                        }
                                                    }
                                                    _ => {}
                                                }
                                                if let Err(_) =context.connection.send(ConnectionMessage::WritePacket(packet)).await {
                                                    warn!("connection sender dropped");
                                                }
                                            }
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
                                                        if let Err(e) = self.process_publish_packet(&publish_packet, &context.connection, &router_sender).await {
                                                            warn!("process publish packet error, {:?}", e);
                                                        }
                                                    }
                                                }
                                                MqttPacketV3::Pingreq(_) => {
                                                    if let Some(context) = &self.context {
                                                        context.connection.send(ConnectionMessage::WritePacket(MqttPacketV3::Pingresp(PingrespPacket::new()))).await.unwrap();
                                                    }
                                                }
                                                MqttPacketV3::Puback(puback_packet) => {
                                                    if let Some(context) = &self.context {
                                                        let mut inflight = self.session.inflight.lock().await;
                                                        inflight
                                                            .next_state(puback_packet.variable_header.packet_identifier)
                                                            .await;

                                                        if let Some(p) = inflight
                                                            .get_current_packet(puback_packet.variable_header.packet_identifier)
                                                            .await
                                                        {
                                                            context.connection.send(ConnectionMessage::WritePacket(p)).await.unwrap();
                                                        }
                                                    }
                                                }
                                                MqttPacketV3::Pubrec(pubrec_packet) => {
                                                    if let Some(context) = &self.context {
                                                        let mut inflight = self.session.inflight.lock().await;
                                                        inflight
                                                            .next_state(pubrec_packet.variable_header.packet_identifier)
                                                            .await;
                                                        if let Some(p) = inflight
                                                            .get_current_packet(pubrec_packet.variable_header.packet_identifier)
                                                            .await
                                                        {
                                                            if let Err(_) = context.connection.send(ConnectionMessage::WritePacket(p)).await {
                                                                warn!("connection receiver dropped");
                                                            }
                                                        }
                                                    }
                                                }
                                                MqttPacketV3::Pubrel(pubrel_packet) => {
                                                    if let Some(context) = &self.context {
                                                        let mut inflight = self.session.inflight.lock().await;
                                                        inflight
                                                            .next_state(pubrel_packet.variable_header.packet_identifier)
                                                            .await;
                                                        if let Some(p) = inflight
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
                                                        let mut inflight = self.session.inflight.lock().await;
                                                        inflight
                                                            .next_state(pubcomp_packet.variable_header.packet_identifier)
                                                            .await;
                                                        if let Some(p) = inflight
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
                                                        if let Err(e) = self.process_unsubscribe_packet(&unsubscribe_packet, &connection).await {
                                                            warn!("process unsubscribe packet error, {:?}", e);
                                                        }
                                                    }
                                                }
                                                MqttPacketV3::Disconnect(_) => {
                                                    debug!("session {} receive disconnect packet", self.session.client_identifier);
                                                    if let Some(context) = &self.context {
                                                        if let Err(_) = context.connection.send(ConnectionMessage::Disconnect).await {
                                                            warn!("connection receiver dropped");
                                                        }
                                                        self.plugin_manager.do_on_disconnect(&self.context.as_ref().unwrap().client_info);

                                                        session_sender.send(SessionMessage::InActivate).await.unwrap();
                                                    }
                                                }
                                                _ => {}
                                            }
                                            //
                                        }
                                        SessionMessage::KickOff(some_reason) => {

                                            let clean_session = self.context.as_ref().unwrap().clean_session;

                                            if let Some(context) = &self.context {
                                                if let Err(_) = context.connection.send(ConnectionMessage::Disconnect).await {
                                                    warn!("connection receiver dropped");
                                                }
                                                self.plugin_manager.do_on_disconnect(&self.context.as_ref().unwrap().client_info);
                                            }

                                            self.state = SessionState::Inactivate;

                                            self.context = None;

                                            // stop keep alive task
                                            self.stop_keep_alive_task().await;
                                            //

                                            // stop inflight resend task
                                            self.stop_inflight_task().await;
                                            //

                                            match some_reason {
                                                KickOffReason::KeepAliveExpired => {
                                                    self.process_will_message(&router_sender).await;
                                                },
                                                KickOffReason::UnexpectDisconnect => {
                                                    self.process_will_message(&router_sender).await;
                                                },
                                                KickOffReason::InvalidMqttPacket => {
                                                    self.process_will_message(&router_sender).await;
                                                }
                                                KickOffReason::Other(msg, quit_signal_sender) => {
                                                    info!("session client id {} kick off reason: {}",self.session.client_identifier, msg);
                                                    self.quit_signal_sender = Some(quit_signal_sender);
                                                },
                                            }

                                            if clean_session {
                                                debug!("session {} clean session, break event loop.", self.session.client_identifier);
                                                break;
                                            } else {
                                                // if some one wait qujit signal, send quit signal
                                                if let Some(quit_signal_sender) = &self.quit_signal_sender {
                                                    if let Err(e) = quit_signal_sender.send(()).await {
                                                        warn!("send quit signal error, {}", e);
                                                    }
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
                }
                SessionState::Inactivate => {
                    select! {
                        msg = self.session_receiver.recv() => {
                            match msg {
                                Some(session_message) => {
                                    match session_message {
                                        SessionMessage::Activate(session_context) => {
                                            debug!("session {} activate", session_context.client_info.client_identifier);
                                            self.context = Some(session_context);
                                            self.state = SessionState::Activate;

                                            // start keep alive task
                                            self.start_keep_alive_task(session_sender.clone()).await;
                                            //

                                            // start inflight resend task
                                            self.start_inflight_task(inflight_resend_duration_secs).await;
                                            //

                                        }
                                        SessionMessage::ForwardFromRouter(packet) => {
                                            match &packet {
                                                MqttPacketV3::Publish(publish_packet) => {
                                                    if publish_packet.fix_header.qos > Some(0) {
                                                        self.session.new_tx_qos_state_ctx(&packet).await;
                                                    }
                                                }
                                                _ => {}
                                            }
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

        debug!("session {} break out the loop, start clean resources", self.session.client_identifier);

        // unsubscribe all topics
        for topic in &self.session.subscription_topics {
            debug!("unsubscribe topic {}", topic);
            if let Err(e) = self.topic_manager.write().await.unsubscription(
                self.session.tenant_identifier.clone(),
                self.session.client_identifier.clone(),
                topic.clone(),
            ) {
                error!(
                    "when exit session event loop, unsubscribe topic {} error: {}",
                    topic, e
                );
            }
        }
        //

        // unregister session from session manager
        debug!(
            "when exit session event loop, unregister session {}",self.session.client_identifier);
        session_manager.write().await.unregister(
            self.session.tenant_identifier.clone(),
            self.session.client_identifier.clone(),
        );
        //

        if let Some(quit_signal_sender) = &self.quit_signal_sender {
            quit_signal_sender.send(()).await.unwrap();
            self.quit_signal_sender = None;
        }

        info!(
            "session {} stoped, return session event loop",
            self.session.client_identifier
        );
    }
}

pub enum SessionState {
    Activate,

    Inactivate,
}

#[derive(Error, Debug)]
pub enum SessionManagerError {
    #[error("tenant {0} not found")]
    TenantNotExisted(String),

    #[error("tenant {0} has existed")]
    TenantHasExisted(String),

    #[error("session {0} not found")]
    SessionNotExisted(String),
}

pub struct SessionManager {
    pub sessions: HashMap<String, HashMap<String, Sender<SessionMessage>>>,
}

impl SessionManager {
    // Create a new tenant
    pub fn create_tenant(&mut self, tenant_identifier: &str) -> Result<()> {
        if self.tenant_existed(tenant_identifier) {
            return Err(anyhow!(SessionManagerError::TenantHasExisted(
                tenant_identifier.into()
            )));
        }
        self.sessions
            .insert(tenant_identifier.into(), HashMap::new());
        Ok(())
    }

    // Check if a tenant existed
    fn tenant_existed(&self, tenant_identifier: &str) -> bool {
        return self.sessions.contains_key(tenant_identifier);
    }

    pub fn get_session_sender(
        &self,
        tenant_identifier: &String,
        client_identifier: &String,
    ) -> Option<Sender<SessionMessage>> {
        if !self.sessions.contains_key(tenant_identifier) {
            return None;
        } else {
            let session_table = self.sessions.get(tenant_identifier).unwrap();
            return session_table.get(client_identifier).cloned();
        }
    }

    pub fn register(
        &mut self,
        tenant_identifier: String,
        client_identifier: String,
        session_sender: Sender<SessionMessage>,
    ) -> Result<()> {
        if !self.sessions.contains_key(&tenant_identifier) {
            return Err(anyhow!(SessionManagerError::TenantNotExisted(
                tenant_identifier
            )));
        } else {
            let session_table = self.sessions.get_mut(&tenant_identifier).unwrap();
            session_table.insert(client_identifier, session_sender);
            Ok(())
        }
    }

    pub fn unregister(&mut self, tenant_identifier: String, client_identifier: String) {
        if !self.sessions.contains_key(&tenant_identifier) {
            return;
        } else {
            let session_table = self.sessions.get_mut(&tenant_identifier).unwrap();
            session_table.remove(&client_identifier);
        }
    }

    pub async fn send_packet(
        &self,
        tenant_identifier: String,
        client_identifier: String,
        packet: &MqttPacketV3,
    ) -> Result<()> {
        if !self.sessions.contains_key(&tenant_identifier) {
            return Err(anyhow!(SessionManagerError::TenantNotExisted(
                tenant_identifier
            )));
        } else {
            let session_table = self.sessions.get(&tenant_identifier).unwrap();
            if let Some(handle) = session_table.get(&client_identifier) {
                if let Err(_) = handle
                    .send(SessionMessage::ForwardFromRouter(packet.clone()))
                    .await
                {
                    warn!("session receiver dropped");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration, vec};

    use samoye_mqtt::v3::{
        pingreq::PingreqPacketBuilder,
        publish::PublishPacketBuilder,
        pubrel::PubRelPacket,
        suback::ReturnCode,
        subscribe::{SubscribePacketBuilder, TopicFilter},
        unsubscribe::UnsubscribePacketBuilder,
    };
    use samoye_plugin::plugin::{Client, ClientProperties, SubscribeReturnCode};
    use tokio::sync::{mpsc::Sender, Mutex, RwLock};

    use crate::{
        inflight::Inflight,
        plugin_manager::PluginService,
        router::RouterCmd,
        session::session_manager::{
            keep_alive_task, KeepAliveMessage, KickOffReason, SessionMessage, SessionWrapper,
        },
        topic::TopicManager,
    };

    use super::{ConnectionMessage, Session, SessionContext, SessionManager};

    /// Test that the keep alive task should send a SessionMessage::KickOff to the session when keep alive expired.
    #[tokio::test]
    pub async fn when_keep_alive_expired_keep_alive_task_shuould_send_kick_off() {
        let test_keep_alive = 2;

        let (session_sender, mut session_receiver) = tokio::sync::mpsc::channel(100);

        let _keep_alive_sender = keep_alive_task(session_sender.clone(), test_keep_alive).await;

        tokio::time::sleep(Duration::from_secs(test_keep_alive + 1)).await; // wait keep alive expired

        let msg = session_receiver.recv().await;

        if let Some(crate::session::session_manager::SessionMessage::KickOff(reason)) = msg {
            match reason {
                KickOffReason::KeepAliveExpired => {
                    assert!(true);
                }
                _ => assert!(false),
            }
        } else {
            assert!(false);
        }
    }

    #[tokio::test]
    pub async fn when_recevie_message_trigger_keep_alive_task_should_not_stop() {
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

    fn mock_session_manager() -> Arc<RwLock<SessionManager>> {
        Arc::new(RwLock::new(SessionManager {
            sessions: HashMap::new(),
        }))
    }

    fn mock_session(client_identifier: &str, tenant_identifier: &str) -> Session {
        Session {
            will_message: None,
            client_identifier: client_identifier.to_string(),
            tenant_identifier: tenant_identifier.to_string(),
            subscription_topics: vec![],
            inflight: Arc::new(Mutex::new(Inflight::new(Duration::from_secs(10)))),
        }
    }

    fn mock_session_context(
        username: &str,
        connection: Sender<ConnectionMessage>,
        keep_alive: u64,
        clean_session: bool,
        client_info: Client,
    ) -> SessionContext {
        SessionContext {
            username: Some(username.to_string()),
            connection,
            keep_alive,
            client_info,
            clean_session,
        }
    }

    struct MockPluginManager;

    impl PluginService for MockPluginManager {
        fn do_on_disconnect(&self, _client: &Client) {
            println!("on_disconnect")
        }

        fn do_on_publish(
            &self,
            _client: &Client,
            _packet: &samoye_mqtt::v3::publish::PublishPacket,
        ) {
            println!("on_publish")
        }

        fn do_publish_authorizate(
            &self,
            _client: &Client,
            _packet: &samoye_mqtt::v3::publish::PublishPacket,
        ) -> anyhow::Result<bool> {
            return Ok(true);
        }

        fn do_subscribe_authorizate(
            &self,
            _client: &Client,
            packet: &samoye_mqtt::v3::subscribe::SubscribePacket,
        ) -> anyhow::Result<samoye_plugin::plugin::SubscribeAuthorizationResult> {
            let length = packet.payload.topic_filters.len();
            return Ok(samoye_plugin::plugin::SubscribeAuthorizationResult {
                return_code: vec![SubscribeReturnCode::MaxQosMostOnce; length],
            });
        }

        fn do_connect_authenticate(
            &self,
            _packet: &samoye_mqtt::v3::connect::ConnectPacket,
        ) -> anyhow::Result<samoye_plugin::plugin::AuthenticationResult> {
            return Ok(samoye_plugin::plugin::AuthenticationResult::Success(
                "tenant_a".into(),
            ));
        }
    }

    #[tokio::test]
    pub async fn when_receive_activate_message_session_wrapper_should_start_keep_alive_task() {
        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: true,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context =
            mock_session_context("username_a", connection_sender, 5, false, client_info);

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let session_sender_clone = session_sender.clone();
        let _join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(), 20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        let msg = connection_receiver.recv().await.unwrap();

        match msg {
            ConnectionMessage::Disconnect => {
                assert!(true)
            }
            _ => {
                assert!(false)
            }
        }
    }

    #[tokio::test]
    pub async fn when_receive_inactivate_message_session_wrapper_should_run_event_loop_continue_if_clean_session_is_false(
    ) {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut _connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: false,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            false,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let session_sender_clone = session_sender.clone();
        let join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(),20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        session_sender
            .send(SessionMessage::InActivate)
            .await
            .unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(keep_alive_expired_secs + 1),
            join_handle,
        )
        .await;

        match result {
            Ok(_) => {
                assert!(false)
            }
            _ => {
                assert!(true)
            }
        }
    }

    #[tokio::test]
    pub async fn when_receive_inactivate_message_session_wrapper_should_stop_event_loop_if_clean_session_is_true(
    ) {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut _connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: true,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            true,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let session_sender_clone = session_sender.clone();
        let join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(),20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        session_sender
            .send(SessionMessage::InActivate)
            .await
            .unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(keep_alive_expired_secs + 1),
            join_handle,
        )
        .await;

        match result {
            Ok(_) => {
                assert!(true)
            }
            _ => {
                assert!(false)
            }
        }
    }

    #[tokio::test]
    pub async fn when_kick_off_session_message_wrapper_should_send_disconnect_to_connection() {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: true,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            true,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let session_sender_clone = session_sender.clone();
        let _join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(), 20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        session_sender
            .send(SessionMessage::KickOff(KickOffReason::InvalidMqttPacket))
            .await
            .unwrap();

        let msg = connection_receiver.recv().await;
        match msg {
            Some(ConnectionMessage::Disconnect) => {
                assert!(true)
            }
            _ => {
                assert!(false)
            }
        }
    }

    #[tokio::test]
    pub async fn when_receive_publish_packet_session_wrapper_should_send_publish_to_router() {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut _connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, mut router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: true,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            true,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let session_sender_clone = session_sender.clone();
        let _join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(), 20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        let packet = PublishPacketBuilder::new("/a/b".to_string(), vec![0x01]).build();

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Publish(packet),
            ))
            .await
            .unwrap();

        let msg = router_receiver.recv().await.unwrap();
        match msg {
            RouterCmd::RoutePacket(tenant_identifier, packet) => {
                assert!(tenant_identifier == "tenant_a");
                match packet {
                    samoye_mqtt::MqttPacketV3::Publish(p) => {
                        assert!(p.variable_header.topic_name == "/a/b");
                        assert!(p.payload.payload.len() == 1);
                        assert!(p.payload.payload[0] == 0x01);
                    }
                    _ => assert!(false),
                }
            }
        }
    }

    #[tokio::test]
    pub async fn when_receive_subscribe_packet_session_wrapper_should_call_topic_manager() {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut _connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, mut _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: true,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            true,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let topic_manger_arc = session_wrapper.topic_manager.clone();

        {
            let mut topic_manager = topic_manger_arc.write().await;
            topic_manager.create_tenant("tenant_a".to_string());
        }

        let session_sender_clone = session_sender.clone();
        let _join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(),20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        let packet = SubscribePacketBuilder::new(123)
            .add_topic_filter(TopicFilter {
                topic_name: "/a/b".to_string(),
                qos: 0,
            })
            .build();

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Subscribe(packet),
            ))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let topic_manger = topic_manger_arc.read().await;

        let topics = topic_manger
            .get_subscriptions("tenant_a".to_string(), "/a/b".to_string())
            .unwrap();
        assert!(topics.len() == 1);
    }

    #[tokio::test]
    pub async fn when_receive_subscribe_packet_session_wrapper_should_return_suback_packet() {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, mut _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: true,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            true,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let topic_manger_arc = session_wrapper.topic_manager.clone();

        {
            let mut topic_manager = topic_manger_arc.write().await;
            topic_manager.create_tenant("tenant_a".to_string());
        }

        let session_sender_clone = session_sender.clone();
        let _join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(), 20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        let packet = SubscribePacketBuilder::new(123)
            .add_topic_filter(TopicFilter {
                topic_name: "/a/b".to_string(),
                qos: 0,
            })
            .build();

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Subscribe(packet),
            ))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let msg = connection_receiver.recv().await.unwrap();

        if let ConnectionMessage::WritePacket(samoye_mqtt::MqttPacketV3::Suback(packet)) = msg {
            assert_eq!(packet.variable_header.packet_identifier, 123);
            assert_eq!(packet.payload.return_code[0], ReturnCode::MaxQos0);
        }
    }

    #[tokio::test]
    pub async fn when_receive_unsubscribe_packet_session_wrapper_should_return_unsuback_packet() {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, mut _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: true,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            true,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let topic_manger_arc = session_wrapper.topic_manager.clone();

        {
            let mut topic_manager = topic_manger_arc.write().await;
            topic_manager.create_tenant("tenant_a".to_string());
        }

        let session_sender_clone = session_sender.clone();
        let _join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(), 20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        let packet = SubscribePacketBuilder::new(123)
            .add_topic_filter(TopicFilter {
                topic_name: "/a/b".to_string(),
                qos: 0,
            })
            .build();

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Subscribe(packet),
            ))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let msg = connection_receiver.recv().await.unwrap();

        if let ConnectionMessage::WritePacket(samoye_mqtt::MqttPacketV3::Suback(packet)) = msg {
            assert_eq!(packet.variable_header.packet_identifier, 123);
            assert_eq!(packet.payload.return_code[0], ReturnCode::MaxQos0);
        }

        let unsubscribe_packet = UnsubscribePacketBuilder::new(123)
            .add_topic_filter(samoye_mqtt::v3::unsubscribe::TopicFilter {
                topic_name: "/a/b".to_string(),
            })
            .build();

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Unsubscribe(unsubscribe_packet),
            ))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let msg = connection_receiver.recv().await.unwrap();

        if let ConnectionMessage::WritePacket(samoye_mqtt::MqttPacketV3::Unsuback(packet)) = msg {
            assert_eq!(packet.variable_header.packet_identifier, 123);
        }
    }

    #[tokio::test]
    pub async fn when_receive_pingreq_packet_session_wrapper_should_return_pingresp_packet() {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, mut _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: true,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            true,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let session_sender_clone = session_sender.clone();
        let _join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(), 20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        let pingreq_packet = PingreqPacketBuilder::new().build();

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Pingreq(pingreq_packet),
            ))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let msg = connection_receiver.recv().await.unwrap();

        if let ConnectionMessage::WritePacket(samoye_mqtt::MqttPacketV3::Pingresp(_)) = msg {
            assert!(true)
        } else {
            assert!(false)
        }
    }

    #[tokio::test]
    pub async fn when_receive_qos_1_publish_packet_session_wrapper_should_finish_the_whole_loop() {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, mut _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: true,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            true,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let topic_manger_arc = session_wrapper.topic_manager.clone();

        {
            let mut topic_manager = topic_manger_arc.write().await;
            topic_manager.create_tenant("tenant_a".to_string());
        }

        let session_sender_clone = session_sender.clone();
        let _join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(), 20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        let qos_1_publish_packet = PublishPacketBuilder::new("/a/b".to_string(), vec![0x01, 0x02])
            .packet_identifier(123)
            .qos(1)
            .build();

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Publish(qos_1_publish_packet),
            ))
            .await
            .unwrap();

        let msg = connection_receiver.recv().await.unwrap();

        if let ConnectionMessage::WritePacket(samoye_mqtt::MqttPacketV3::Puback(packet)) = msg {
            assert_eq!(packet.variable_header.packet_identifier, 123);
        }
    }
    #[tokio::test]
    pub async fn when_receive_qos_2_publish_packet_session_wrapper_should_finish_the_whole_loop() {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, mut _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: true,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            true,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let topic_manger_arc = session_wrapper.topic_manager.clone();

        {
            let mut topic_manager = topic_manger_arc.write().await;
            topic_manager.create_tenant("tenant_a".to_string());
        }

        let session_sender_clone = session_sender.clone();
        let _join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(),20)
                .await;
        });

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        let packet_id = 123;

        let qos_2_publish_packet = PublishPacketBuilder::new("/a/b".to_string(), vec![0x01, 0x02])
            .packet_identifier(packet_id)
            .qos(2)
            .build();

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Publish(qos_2_publish_packet),
            ))
            .await
            .unwrap();

        let msg = connection_receiver.recv().await.unwrap();

        if let ConnectionMessage::WritePacket(samoye_mqtt::MqttPacketV3::Pubrec(packet)) = msg {
            assert_eq!(packet.variable_header.packet_identifier, packet_id);
        }

        let qos_2_pubrel_packet = PubRelPacket::new(packet_id);

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Pubrel(qos_2_pubrel_packet),
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    pub async fn when_session_exsited_session_wrapper_should_set_connack_present_flag() {
        
    }

    #[tokio::test]
    pub async fn when_reactivate_not_clean_session_session_wrapper_should_finish_qos2_whole_loop() {
        let keep_alive_expired_secs = 5;

        let session_mock = mock_session("client_a", "tenant_a");

        let (session_sender, session_receiver) = tokio::sync::mpsc::channel(100);
        let (connection_sender, mut connection_receiver) = tokio::sync::mpsc::channel(100);
        let (router_sender, mut _router_receiver) = tokio::sync::mpsc::channel(100);

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: false,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender.clone(),
            keep_alive_expired_secs,
            false,
            client_info,
        );

        let mut session_wrapper = SessionWrapper::new(
            session_mock,
            session_receiver,
            Arc::new(MockPluginManager),
            Arc::new(RwLock::new(TopicManager::new())),
        );

        let topic_manger_arc = session_wrapper.topic_manager.clone();

        {
            let mut topic_manager = topic_manger_arc.write().await;
            topic_manager.create_tenant("tenant_a".to_string());
        }

        let session_sender_clone = session_sender.clone();
        let _join_handle = tokio::task::spawn(async move {
            session_wrapper
                .run_event_loop(session_sender_clone, router_sender, mock_session_manager(), 20)
                .await;
        });

        let packet_id = 123;

        let qos_2_publish_packet = PublishPacketBuilder::new("/a/b".to_string(), vec![0x01, 0x02])
            .packet_identifier(packet_id)
            .qos(2)
            .build();

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Publish(qos_2_publish_packet),
            ))
            .await
            .unwrap();

        session_sender
            .send(SessionMessage::InActivate)
            .await
            .unwrap();

        let client_info = Client {
            tenant_id: "tenant_a".into(),
            client_identifier: "client_a".into(),
            socket_addr: "127.0.0.1:1234".parse().unwrap(),
            properties: ClientProperties {
                username: Some("username_a".to_string()),
                clean_session: false,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };

        let session_context = mock_session_context(
            "username_a",
            connection_sender,
            keep_alive_expired_secs,
            true,
            client_info,
        );

        session_sender
            .send(SessionMessage::Activate(session_context))
            .await
            .unwrap();

        let msg = connection_receiver.recv().await.unwrap();

        if let ConnectionMessage::WritePacket(samoye_mqtt::MqttPacketV3::Pubrec(packet)) = msg {
            assert_eq!(packet.variable_header.packet_identifier, packet_id);
        } else {
            assert!(false);
        }

        let qos_2_pubrel_packet = PubRelPacket::new(packet_id);

        session_sender
            .send(SessionMessage::ReceiveFromClient(
                samoye_mqtt::MqttPacketV3::Pubrel(qos_2_pubrel_packet),
            ))
            .await
            .unwrap();
    }
}
