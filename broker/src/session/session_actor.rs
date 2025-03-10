use actix::{
    dev::ContextFutureSpawner, Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context,
    Handler, MailboxError, Message, Recipient, SpawnHandle, WrapFuture,
};
use log::warn;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::{mpsc::Sender, Mutex, RwLock};
use yedmq_mqtt::{
    v3::{
        disconnect::DisconnectPacket,
        pingresp::PingrespPacket,
        puback::PubAckPacket,
        pubcomp::PubCompPacket,
        publish::{PublishPacket, PublishPacketBuilder},
        pubrec::PubRecPacket,
        pubrel::PubRelPacket,
        suback::SubackPacket,
        subscribe::SubscribePacket,
        unsuback::UnSubackPacket,
        unsubscribe::UnsubscribePacket,
    },
    MqttPacketV3,
};
use yedmq_plugin::plugin::{Client, ClientProperties};

use crate::{
    inflight::{Inflight, InflightError},
    plugin_manager::{PluginService, SubscribeReturnCode},
    router::RouterCmd,
    topic::topic_manager::TopicManagerTrait,
};

use super::{
    connection::{ConnectionActor, ConnectionActorMessage},
    WillMessage,
};

pub enum QoS {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

impl From<u8> for QoS {
    fn from(qos: u8) -> QoS {
        match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => panic!("Invalid QoS"),
        }
    }
}

#[derive(Debug, Error)]
pub enum SessionActorError {
    #[error("plugin execution error: {0}")]
    PluginExecutionError(#[from] anyhow::Error),

    #[error("connection actor error: {0}")]
    ConnectionActorError(#[from] MailboxError),

    #[error("connection not set")]
    ConnectionNotSet,
}

#[derive(Message)]
#[rtype(result = "()")]
pub enum SessionActorMessage {
    InboundPacket(MqttPacketV3),

    OutboundMessage(MqttPacketV3),

    KeepAliveExpred,

    InflightRetry,

    ForceDisconnect,

    Reconnect {
        conn: Recipient<ConnectionActorMessage>,

        keep_alive: u64,

        clean_session: bool,

        username: Option<String>,

        will_message: Option<WillMessage>,

        socket_addr: std::net::SocketAddr,
    },

    UnexpectClientDisconnected,

    ClientDisconnected,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ClientDisconnected {}

pub enum SessionState {
    Active,

    Inactive,
}

pub struct SessionActor {
    tenant_id: String,

    client_id: String,

    clean_session: bool,

    username: Option<String>,

    will_message: Option<WillMessage>,

    state: SessionState,

    subscriptions: HashMap<String, QoS>,

    plugin_manager: Arc<dyn PluginService + 'static>,

    topic_manager: Arc<RwLock<dyn TopicManagerTrait>>,

    conn_recipient: Option<Recipient<ConnectionActorMessage>>,

    conn_addr: Option<SocketAddr>,

    router_sender: Sender<RouterCmd>,

    inflight: Arc<Mutex<Inflight>>,

    keep_alive: u64,

    keep_alive_expired: bool,

    keep_alive_task_handle: Option<SpawnHandle>,

    inflight_retry_interval: u64,

    inflight_retry_task_handle: Option<SpawnHandle>,

    pending_messages: Vec<MqttPacketV3>,
}

impl Actor for SessionActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let keep_alive_task_handle =
            ctx.run_interval(Duration::from_secs(self.keep_alive), |act, ctx| {
                if act.keep_alive_expired {
                    ctx.address().do_send(SessionActorMessage::KeepAliveExpred);
                }
            });
        self.keep_alive_task_handle = Some(keep_alive_task_handle);

        let inflight_retry_task_handle = ctx.run_interval(
            Duration::from_secs(self.inflight_retry_interval),
            |_act, ctx| {
                ctx.address().do_send(SessionActorMessage::InflightRetry);
            },
        );
        self.inflight_retry_task_handle = Some(inflight_retry_task_handle);
    }
}

fn is_allowd_subscribe(subscribe_return_code: &SubscribeReturnCode) -> bool {
    !(matches!(subscribe_return_code, SubscribeReturnCode::Invalid)
        || matches!(subscribe_return_code, SubscribeReturnCode::Failure))
}

struct HandleSubscribeResult {
    retain_messages: Vec<Arc<MqttPacketV3>>,

    suback_packet: SubackPacket,

    succeed_subscriptions: Vec<(String, QoS)>,
}

struct HandleUnSubscribeResult {
    unsuback_packet: UnSubackPacket,

    succeed_unsubscriptions: Vec<String>,
}

struct HandlePublishResult {
    inflight_packet: Option<MqttPacketV3>,
}

impl From<SubscribeReturnCode> for yedmq_mqtt::v3::suback::ReturnCode {
    fn from(subscribe_return_code: SubscribeReturnCode) -> yedmq_mqtt::v3::suback::ReturnCode {
        match subscribe_return_code {
            SubscribeReturnCode::MaxQosLeastOnce => yedmq_mqtt::v3::suback::ReturnCode::MaxQos0,
            SubscribeReturnCode::MaxQosMostOnce => yedmq_mqtt::v3::suback::ReturnCode::MaxQos1,
            SubscribeReturnCode::MaxQosExactlyOnce => yedmq_mqtt::v3::suback::ReturnCode::MaxQos2,
            SubscribeReturnCode::Failure => yedmq_mqtt::v3::suback::ReturnCode::Failure,
            SubscribeReturnCode::Invalid => yedmq_mqtt::v3::suback::ReturnCode::Invalid,
        }
    }
}

async fn do_handle_unsubscribe(
    unsubscribe_packet: UnsubscribePacket,
    client_info: Client,
    topic_manager: Arc<RwLock<dyn TopicManagerTrait>>,
) -> HandleUnSubscribeResult {
    let unsub_topic_filters = &unsubscribe_packet.payload.topic_filters;
    let mut succeed_unsubscriptions = vec![];
    {
        let topic_manager = topic_manager.write().await;
        for topic in unsub_topic_filters {
            let tenant_id = client_info.tenant_id.clone();
            let client_id = client_info.client_identifier.clone();
            let _ =
                topic_manager.handle_unsubscribe(tenant_id, client_id, topic.topic_name.clone());
            succeed_unsubscriptions.push(topic.topic_name.clone());
        }
    }
    let unsuback_packet = yedmq_mqtt::v3::unsuback::UnSubackPacket::new(
        unsubscribe_packet.variable_header.packet_identifier,
    );
    HandleUnSubscribeResult {
        unsuback_packet,
        succeed_unsubscriptions,
    }
}

async fn do_handle_publish(
    publish_packet: PublishPacket,
    client_info: Client,
    topic_manager: Arc<RwLock<dyn TopicManagerTrait>>,
    plugin_manager: Arc<dyn PluginService>,
    inflight: Arc<Mutex<Inflight>>,
    router_sender: Sender<RouterCmd>,
) -> HandlePublishResult {
    let publish_authorization = plugin_manager
        .do_publish_authorizate(&client_info, &publish_packet)
        .unwrap();

    let mut result = HandlePublishResult {
        inflight_packet: None,
    };

    if publish_authorization {
        plugin_manager.do_on_publish(&client_info, &publish_packet);

        if publish_packet.fix_header.qos > Some(0) {
            let inflight = inflight.lock().await;
            inflight
                .register_with_rx_packet(&MqttPacketV3::Publish(publish_packet.clone()))
                .await;

            let packet = inflight
                .get_current_packet(publish_packet.variable_header.packet_identifier.unwrap())
                .await
                .unwrap();
            result.inflight_packet = Some(packet);
        }

        // process retain messages
        if publish_packet.fix_header.retain == Some(true) {
            // register retain publish packet
            let mut topic_manager = topic_manager.write().await;

            // if publish packet paloyd is empty , clean retained publish packet
            if publish_packet.payload.payload.is_empty() {
                let _ = topic_manager
                    .clean_retain_publish_packet(
                        client_info.tenant_id.clone(),
                        &publish_packet.variable_header.topic_name,
                    )
                    .await;
            } else {
                let _ = topic_manager
                    .register_retain_publish_packet(
                        client_info.tenant_id.clone(),
                        client_info.client_identifier.clone(),
                        &MqttPacketV3::Publish(publish_packet.clone()),
                    )
                    .await;
            }
            //
        }
        //
        router_sender
            .send(RouterCmd::RoutePacket {
                tenant_identifier: client_info.tenant_id.clone(),
                packet: MqttPacketV3::Publish(publish_packet.clone()),
            })
            .await
            .unwrap();
    }

    return result;
}

async fn do_handle_subscribe(
    subscribe_packet: SubscribePacket,
    client_info: Client,
    topic_manager: Arc<RwLock<dyn TopicManagerTrait>>,
    plugin_manager: Arc<dyn PluginService>,
) -> HandleSubscribeResult {
    let tenant_id = client_info.tenant_id.clone();

    let client_id = client_info.client_identifier.clone();

    let subscriptions = &subscribe_packet.payload.topic_filters;

    let mut retain_messages: Vec<Arc<MqttPacketV3>> = vec![];

    let mut return_code: Vec<yedmq_mqtt::v3::suback::ReturnCode> = vec![];

    let mut succeed_subscriptions: Vec<(String, QoS)> = vec![];

    let topic_authorizate_result = plugin_manager
        .do_subscribe_authorizate(&client_info, &subscribe_packet)
        .unwrap();

    let plugin_return_code = topic_authorizate_result.return_code;
    for i in 0..subscriptions.len() {
        let mut topic_manager = topic_manager.write().await;
        let topic = subscribe_packet.payload.topic_filters[i].clone();

        if is_allowd_subscribe(&plugin_return_code[i]) {
            let sub_result = topic_manager
                .handle_subscribe(
                    tenant_id.clone(),
                    client_id.clone(),
                    topic.topic_name.clone(),
                    topic.qos,
                )
                .await;
            if let Ok(_) = sub_result {
                succeed_subscriptions.push((topic.topic_name.clone(), topic.qos.into()));
                return_code.push(plugin_return_code[i].clone().into());
                let packets = topic_manager
                    .get_retain_publish_packet(tenant_id.clone(), topic.topic_name.clone())
                    .await;
                if let Ok(packets) = packets {
                    for packet in packets {
                        retain_messages.push(packet);
                    }
                }
            } else {
                return_code.push(yedmq_mqtt::v3::suback::ReturnCode::Failure);
            }
        }
    }

    let suback_packet = SubackPacket::new(
        subscribe_packet.variable_header.packet_identifier,
        return_code,
    );

    HandleSubscribeResult {
        succeed_subscriptions,
        retain_messages,
        suback_packet,
    }
}

impl SessionActor {
    pub fn new(
        tenant_id: String,
        client_id: String,
        clean_session: bool,
        topic_manager: Arc<RwLock<dyn TopicManagerTrait>>,
        plugin_manager: Arc<dyn PluginService>,
        router_sender: Sender<RouterCmd>,
        inflight_retry_duration_secs: u64,
        will_message: Option<WillMessage>,
        keep_alive: u64,
        connection_actor_addr: Recipient<ConnectionActorMessage>,
        peer_addr: SocketAddr,
    ) -> Self {
        SessionActor {
            topic_manager,
            plugin_manager,
            router_sender,
            conn_recipient: Some(connection_actor_addr),
            conn_addr: Some(peer_addr),
            inflight: Arc::new(Mutex::new(Inflight::new(Duration::from_secs(
                inflight_retry_duration_secs,
            )))),
            state: SessionState::Active,
            keep_alive_task_handle: None,
            subscriptions: HashMap::new(),
            clean_session,
            keep_alive,
            keep_alive_expired: true,
            inflight_retry_interval: inflight_retry_duration_secs,
            inflight_retry_task_handle: None,
            tenant_id,
            client_id,
            will_message,
            username: None,
            pending_messages: vec![],
        }
    }

    fn get_plugin_client_info(&self) -> Client {
        let client_properties = match &self.will_message {
            Some(will_message) => ClientProperties {
                username: self.username.clone(),
                clean_session: self.clean_session,
                will_retain: will_message.will_retain,
                will_topic: Some(will_message.will_topic.clone()),
                will_message: Some(will_message.will_message.clone()),
            },
            None => ClientProperties {
                username: self.username.clone(),
                clean_session: self.clean_session,
                will_retain: false,
                will_topic: None,
                will_message: None,
            },
        };
        Client {
            client_identifier: self.client_id.clone(),
            tenant_id: self.tenant_id.clone(),
            properties: client_properties,
            socket_addr: self.conn_addr.unwrap(),
        }
    }

    fn set_state(&mut self, state: SessionState) {
        self.state = state;
    }

    fn clean_up(&mut self, ctx: &mut <SessionActor as Actor>::Context) {
        if self.clean_session {
            ctx.stop();
        } else {
            self.state = SessionState::Inactive;

            if let Some(handle) = self.keep_alive_task_handle.take() {
                if ctx.cancel_future(handle) {
                    self.keep_alive_task_handle = None;
                }
            }

            if let Some(handle) = self.inflight_retry_task_handle.take() {
                if ctx.cancel_future(handle) {
                    self.inflight_retry_task_handle = None;
                }
            }
        }
    }

    fn handle_publish(
        &mut self,
        publish_packet: PublishPacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        let topic_manager = self.topic_manager.clone();
        let plugin_manager = self.plugin_manager.clone();
        let client_info = self.get_plugin_client_info();
        let inflight = self.inflight.clone();
        let router_sender = self.router_sender.clone();

        async move {
            do_handle_publish(
                publish_packet,
                client_info,
                topic_manager,
                plugin_manager,
                inflight,
                router_sender,
            )
            .await
        }
        .into_actor(self)
        .map(|res, act, _ctx| {
            if let Some(packet) = res.inflight_packet {
                let conn = act.conn_recipient.clone().unwrap();
                conn.do_send(ConnectionActorMessage::WritePacketToClient(packet));
            }
        })
        .wait(ctx);
    }

    fn handle_subscribe(
        &mut self,
        subscribe_packet: SubscribePacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        let topic_manager = self.topic_manager.clone();
        let plugin_manager = self.plugin_manager.clone();
        let client_info = self.get_plugin_client_info();
        async move {
            do_handle_subscribe(subscribe_packet, client_info, topic_manager, plugin_manager).await
        }
        .into_actor(self)
        .map(|res, act, _ctx| {
            let conn = act.conn_recipient.clone().unwrap();
            for packet in res.retain_messages {
                let packet = (*packet).clone();
                conn.do_send(ConnectionActorMessage::WritePacketToClient(packet));
            }
            conn.do_send(ConnectionActorMessage::WritePacketToClient(
                yedmq_mqtt::MqttPacketV3::Suback(res.suback_packet),
            ));
            act.subscriptions.extend(res.succeed_subscriptions);
        })
        .wait(ctx);
    }

    fn handle_pingreq(&mut self) {
        let conn = self.conn_recipient.clone().unwrap();
        conn.do_send(ConnectionActorMessage::WritePacketToClient(
            yedmq_mqtt::MqttPacketV3::Pingresp(PingrespPacket::new()),
        ));
    }

    fn handle_unsubscribe(
        &mut self,
        unsubscribe_packet: UnsubscribePacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        let client_info = self.get_plugin_client_info();
        let topic_manager = self.topic_manager.clone();
        async move { do_handle_unsubscribe(unsubscribe_packet, client_info, topic_manager).await }
            .into_actor(self)
            .map(|res, act, _ctx| {
                act.subscriptions
                    .retain(|k, _| !res.succeed_unsubscriptions.contains(k));
                let conn = act.conn_recipient.clone().unwrap();
                conn.do_send(ConnectionActorMessage::WritePacketToClient(
                    yedmq_mqtt::MqttPacketV3::Unsuback(res.unsuback_packet),
                ));
            })
            .wait(ctx);
    }

    fn handle_pubrel(
        &mut self,
        pubrel_packet: PubRelPacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        // To ensure synchronization of inflight information for QoS 1 and QoS 2,
        // the session actor must use the send method to first send the next stage response packet,
        // then wait for the connection actor to reply, confirming the write is complete,
        // before updating the inflight status again.

        let infight = self.inflight.clone();
        let conn = self.conn_recipient.clone().unwrap();

        async move {
            let mut inflight = infight.lock().await;
            let next_state_packet = inflight
                .get_next_state_packet(pubrel_packet.variable_header.packet_identifier)
                .await;
            if let Some(packet) = next_state_packet {
                if let Err(e) = conn
                    .send(ConnectionActorMessage::WritePacketToClient(packet))
                    .await
                {
                    warn!("write packet to client error: {}", e);
                } else {
                    inflight
                        .next_state(pubrel_packet.variable_header.packet_identifier)
                        .await;
                }
            }
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn handle_pubrec(
        &mut self,
        pubrec_packet: PubRecPacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        // To ensure synchronization of inflight information for QoS 1 and QoS 2,
        // the session actor must use the send method to first send the next stage response packet,
        // then wait for the connection actor to reply, confirming the write is complete,
        // before updating the inflight status again.

        let infight = self.inflight.clone();
        let conn = self.conn_recipient.clone().unwrap();

        async move {
            let mut inflight = infight.lock().await;
            let next_state_packet = inflight
                .get_next_state_packet(pubrec_packet.variable_header.packet_identifier)
                .await;
            if let Some(packet) = next_state_packet {
                if let Err(e) = conn
                    .send(ConnectionActorMessage::WritePacketToClient(packet))
                    .await
                {
                    warn!("write packet to client error: {}", e);
                } else {
                    inflight
                        .next_state(pubrec_packet.variable_header.packet_identifier)
                        .await;
                }
            }
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn handle_puback(
        &mut self,
        puback_packet: PubAckPacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        // To ensure synchronization of inflight information for QoS 1 and QoS 2,
        // the session actor must use the send method to first send the next stage response packet,
        // then wait for the connection actor to reply, confirming the write is complete,
        // before updating the inflight status again.

        let infight = self.inflight.clone();
        let conn = self.conn_recipient.clone().unwrap();

        async move {
            let mut inflight = infight.lock().await;
            let next_state_packet = inflight
                .get_next_state_packet(puback_packet.variable_header.packet_identifier)
                .await;
            if let Some(packet) = next_state_packet {
                if let Err(e) = conn
                    .send(ConnectionActorMessage::WritePacketToClient(packet))
                    .await
                {
                    warn!("write packet to client error: {}", e);
                } else {
                    inflight
                        .next_state(puback_packet.variable_header.packet_identifier)
                        .await;
                }
            }
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn handle_pubcomp(
        &mut self,
        pubcomp_packet: PubCompPacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        // To ensure synchronization of inflight information for QoS 1 and QoS 2,
        // the session actor must use the send method to first send the next stage response packet,
        // then wait for the connection actor to reply, confirming the write is complete,
        // before updating the inflight status again.

        let infight = self.inflight.clone();
        let conn = self.conn_recipient.clone().unwrap();

        async move {
            let mut inflight = infight.lock().await;
            let next_state_packet = inflight
                .get_next_state_packet(pubcomp_packet.variable_header.packet_identifier)
                .await;
            if let Some(packet) = next_state_packet {
                if let Err(e) = conn
                    .send(ConnectionActorMessage::WritePacketToClient(packet))
                    .await
                {
                    warn!("write packet to client error: {}", e);
                } else {
                    inflight
                        .next_state(pubcomp_packet.variable_header.packet_identifier)
                        .await;
                }
            }
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn handle_disconnect(
        &mut self,
        _disconnect_packet: DisconnectPacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        self.clean_will_message();
        self.conn_recipient
            .clone()
            .unwrap()
            .do_send(ConnectionActorMessage::Disconnect);

        self.plugin_manager
            .do_on_disconnect(&self.get_plugin_client_info());
        self.clean_up(ctx);
    }

    fn reset_keep_alive_expired_flag(&mut self) {
        self.keep_alive_expired = false;
    }

    fn clean_will_message(&mut self) {
        self.will_message = None;
    }

    fn send_will_message(&mut self, ctx: &mut <SessionActor as Actor>::Context) {
        let tenant_id = self.tenant_id.clone();
        let will_message = self.will_message.take();
        let router_sender = self.router_sender.clone();
        async move {
            if will_message.is_some() {
                let will_message = will_message.as_ref().unwrap();
                let publish_packet = PublishPacketBuilder::new(
                    will_message.will_topic.clone(),
                    will_message.will_message.clone(),
                )
                .retain(will_message.will_retain)
                .qos(will_message.will_qos)
                .build();
                let _ = router_sender
                    .send(RouterCmd::RoutePacket {
                        tenant_identifier: tenant_id,
                        packet: MqttPacketV3::Publish(publish_packet),
                    })
                    .await;
            }
        }
        .into_actor(self)
        .wait(ctx);
    }
}

impl Handler<SessionActorMessage> for SessionActor {
    type Result = ();
    fn handle(&mut self, msg: SessionActorMessage, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            SessionActorMessage::InboundPacket(packet) => {
                self.reset_keep_alive_expired_flag();
                match packet {
                    MqttPacketV3::Pingreq(_) => {
                        self.handle_pingreq();
                    }
                    MqttPacketV3::Subscribe(subscribe_packet) => {
                        self.handle_subscribe(subscribe_packet, ctx);
                    }
                    MqttPacketV3::Unsubscribe(unsubscribe_packet) => {
                        self.handle_unsubscribe(unsubscribe_packet, ctx);
                    }
                    MqttPacketV3::Publish(publish_packet) => {
                        self.handle_publish(publish_packet, ctx);
                    }
                    MqttPacketV3::Puback(puback_packet) => {
                        self.handle_puback(puback_packet, ctx);
                    }
                    MqttPacketV3::Pubrec(pubrec_packet) => {
                        self.handle_pubrec(pubrec_packet, ctx);
                    }
                    MqttPacketV3::Pubrel(pubrel_packet) => {
                        self.handle_pubrel(pubrel_packet, ctx);
                    }
                    MqttPacketV3::Pubcomp(pubcomp_packet) => {
                        self.handle_pubcomp(pubcomp_packet, ctx);
                    }
                    MqttPacketV3::Disconnect(disconnect_packet) => {
                        self.handle_disconnect(disconnect_packet, ctx);
                    }
                    _ => {}
                }
            }
            SessionActorMessage::OutboundMessage(packet) => {
                if matches!(self.state, SessionState::Active) {
                    if let MqttPacketV3::Publish(mut packet) = packet {
                        let conn = self.conn_recipient.clone().unwrap();
                        let inflight = self.inflight.clone();
                        async move {
                            if packet.fix_header.qos.or(Some(0)).unwrap() > 0 {
                                let res = inflight
                                    .lock()
                                    .await
                                    .register_with_tx_packet(&MqttPacketV3::Publish(packet.clone()))
                                    .await;
                                if let Err(e) = res {
                                    match e {
                                        InflightError::PacketIdentifierHasExisted => {
                                            let packet_id = inflight.lock().await.allocate_packet_id().await;
                                            if let Some(packet_id) = packet_id {
                                                packet.variable_header.packet_identifier = Some(packet_id);
                                            } else {
                                                warn!("infligh has no more packet id available");
                                            }
                                        }
                                    }
                                }
                            }
                            conn.do_send(ConnectionActorMessage::WritePacketToClient(
                                yedmq_mqtt::MqttPacketV3::Publish(packet),
                            ));
                        }
                        .into_actor(self)
                        .wait(ctx);
                    }
                } else {
                    if matches!(packet, MqttPacketV3::Publish(_)) {
                        if let MqttPacketV3::Publish(publish_packet) = packet {
                            if publish_packet.fix_header.qos.or(Some(0)).unwrap() > 0 {
                                self.pending_messages
                                    .push(MqttPacketV3::Publish(publish_packet));
                            }
                        }
                    }
                }
            }
            SessionActorMessage::KeepAliveExpred => {
                // send will message and clean up
                self.send_will_message(ctx);
                self.clean_up(ctx);
                if let Some(recipient) = &self.conn_recipient {
                    recipient.do_send(ConnectionActorMessage::Disconnect);
                }
            }
            SessionActorMessage::InflightRetry => {
                let inflight = self.inflight.clone();
                let conn = self.conn_recipient.clone().unwrap();
                async move {
                    let mut inflight = inflight.lock().await;
                    let packets = inflight
                        .get_all_expired_packets_and_refresh_expired_time()
                        .await;
                    for packet in packets {
                        let result = conn
                            .send(ConnectionActorMessage::WritePacketToClient(packet.1))
                            .await;
                        if let Err(e) = result {
                            warn!("write packet to client error: {}", e);
                        } else {
                            // update inflight
                            inflight.next_state(packet.0).await;
                        }
                        //
                    }
                }
                .into_actor(self)
                .wait(ctx);
            }
            SessionActorMessage::ForceDisconnect => {
                if matches!(self.state, SessionState::Active) {
                    let conn = self.conn_recipient.clone().unwrap();
                    async move {
                        conn.send(ConnectionActorMessage::Disconnect).await.unwrap();
                    }
                    .into_actor(self)
                    .wait(ctx);
                    self.clean_up(ctx);
                }
            }
            SessionActorMessage::UnexpectClientDisconnected => {
                self.send_will_message(ctx);
                self.clean_up(ctx);
            }
            SessionActorMessage::Reconnect {
                conn,
                keep_alive,
                clean_session,
                username,
                will_message,
                socket_addr,
            } => {
                self.set_state(SessionState::Active);
                self.conn_recipient = Some(conn);
                self.will_message = will_message;
                self.clean_session = clean_session;
                self.keep_alive = keep_alive;
                self.username = username;

                // Restart the keep-alive and inflight retry tasks
                let keep_alive_task_handle =
                    ctx.run_interval(Duration::from_secs(self.keep_alive), |act, ctx| {
                        if act.keep_alive_expired {
                            ctx.address().do_send(SessionActorMessage::KeepAliveExpred);
                        }
                    });
                self.keep_alive_task_handle = Some(keep_alive_task_handle);

                let inflight_retry_task_handle = ctx.run_interval(
                    Duration::from_secs(self.inflight_retry_interval),
                    |_act, ctx| {
                        ctx.address().do_send(SessionActorMessage::InflightRetry);
                    },
                );
                self.inflight_retry_task_handle = Some(inflight_retry_task_handle);
                //

                // start consume pending messages
                for msg in self.pending_messages.drain(..) {
                    ctx.address()
                        .do_send(SessionActorMessage::OutboundMessage(msg));
                }
            }
            SessionActorMessage::ClientDisconnected => {
                self.clean_up(ctx);
            }
        }
    }
}

#[cfg(test)]
#[derive(Message)]
#[rtype(result = "usize")]
pub struct GetPendingMessagesCount {}

#[cfg(test)]
impl Handler<GetPendingMessagesCount> for SessionActor {
    type Result = usize;

    fn handle(&mut self, _msg: GetPendingMessagesCount, _ctx: &mut Self::Context) -> Self::Result {
        self.pending_messages.len()
    }
}

#[cfg(test)]
mod tests {
    use mockall::predicate::eq;
    use yedmq_mqtt::v3::{
        pingreq::PingreqPacketBuilder,
        subscribe::{SubscribePacketBuilder, TopicFilter},
    };

    use super::*;
    use crate::{
        plugin_manager::{MockPluginService, SubscribeAuthorizationResult},
        topic::topic_manager::MockTopicManagerTrait,
    };

    struct MockConnectionActor {
        message_sender: Sender<ConnectionActorMessage>,
    }

    impl Actor for MockConnectionActor {
        type Context = Context<Self>;
    }

    impl Handler<ConnectionActorMessage> for MockConnectionActor {
        type Result = ();

        fn handle(&mut self, msg: ConnectionActorMessage, ctx: &mut Self::Context) -> Self::Result {
            let message_sender = self.message_sender.clone();
            ctx.spawn(
                async move {
                    let _ = message_sender.send(msg).await;
                }
                .into_actor(self),
            );
        }
    }

    const KEEP_ALIVE: u64 = 5;
    const INFLIGHT_RETRY: u64 = 5;

    #[actix::test]
    pub async fn when_qos_packet_identifier_exist_in_inflight_should_reallocate_new_one() {

        let mock_topic_manager = MockTopicManagerTrait::new();

        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let mut plugin_service = MockPluginService::new();
        plugin_service
            .expect_do_publish_authorizate()
            .returning(|_, _| std::result::Result::Ok(true));
        plugin_service.expect_do_on_publish().returning(|_, _| ());

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut _router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            false,
            mock_topic_manager,
            Arc::new(plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE + 2 * INFLIGHT_RETRY, // ensure the keep-alive not expired
            connection_recipient.clone(),
            "127.0.0.1:1883".parse().unwrap(),
        );
        let session_actor_addr = session_actor.start();

        let client_publish_packet = PublishPacketBuilder::new("/a/b/c".to_string(), "hello".into())
            .qos(2)
            .packet_identifier(123)
            .build();

        session_actor_addr
            .send(SessionActorMessage::OutboundMessage(MqttPacketV3::Publish(
                client_publish_packet,
            )))
            .await
            .unwrap();

        let msg = message_rx.recv().await.unwrap();
        match msg {
            ConnectionActorMessage::WritePacketToClient(publish) => {
                match publish {
                    yedmq_mqtt::MqttPacketV3::Publish(publish) => {
                        assert_eq!(publish.variable_header.topic_name, "/a/b/c");
                        assert_eq!(publish.fix_header.qos, Some(2));
                    }
                    _ => panic!("expected ConnectionActorMessage::Publish"),
                }
            }
            _ => panic!("expected ConnectionActorMessage::Publish"),
        }

        let client_publish_packet = PublishPacketBuilder::new("/a/b/c".to_string(), "hello".into())
            .qos(2)
            .packet_identifier(123)
            .build();

        session_actor_addr
            .send(SessionActorMessage::OutboundMessage(MqttPacketV3::Publish(
                client_publish_packet,
            )))
            .await
            .unwrap();

        let msg = message_rx.recv().await.unwrap();
        match msg {
            ConnectionActorMessage::WritePacketToClient(publish) => {
                match publish {
                    yedmq_mqtt::MqttPacketV3::Publish(publish) => {
                        assert_eq!(publish.variable_header.topic_name, "/a/b/c");
                        assert_eq!(publish.fix_header.qos, Some(2));
                        assert_ne!(publish.variable_header.packet_identifier.unwrap(), 123);
                    }
                    _ => panic!("expected ConnectionActorMessage::Publish"),
                }
            }
            _ => panic!("expected ConnectionActorMessage::Publish"),
        }
    }

    #[actix::test]
    pub async fn when_session_reactive_should_process_the_outbound_uncomplete_qos_2_message() {

        let mock_topic_manager = MockTopicManagerTrait::new();

        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let mut plugin_service = MockPluginService::new();
        plugin_service
            .expect_do_publish_authorizate()
            .returning(|_, _| std::result::Result::Ok(true));
        plugin_service.expect_do_on_publish().returning(|_, _| ());

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut _router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            false,
            mock_topic_manager,
            Arc::new(plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE + 2 * INFLIGHT_RETRY, // ensure the keep-alive not expired
            connection_recipient.clone(),
            "127.0.0.1:1883".parse().unwrap(),
        );
        let session_actor_addr = session_actor.start();

        let client_publish_packet = PublishPacketBuilder::new("/a/b/c".to_string(), "hello".into())
            .qos(2)
            .packet_identifier(123)
            .build();

        session_actor_addr
            .send(SessionActorMessage::OutboundMessage(MqttPacketV3::Publish(
                client_publish_packet,
            )))
            .await
            .unwrap();

        let msg = message_rx.recv().await.unwrap();
        match msg {
            ConnectionActorMessage::WritePacketToClient(publish) => {
                match publish {
                    yedmq_mqtt::MqttPacketV3::Publish(publish) => {
                        assert_eq!(publish.variable_header.topic_name, "/a/b/c");
                        assert_eq!(publish.fix_header.qos, Some(2));
                    }
                    _ => panic!("expected ConnectionActorMessage::Publish"),
                }
            }
            _ => panic!("expected ConnectionActorMessage::Publish"),
        }

        session_actor_addr
            .send(SessionActorMessage::ClientDisconnected)
            .await
            .unwrap();

        session_actor_addr.send(SessionActorMessage::Reconnect {
            conn: connection_recipient.clone(),
            keep_alive: KEEP_ALIVE,
            clean_session: false,
            username: Some("test".to_string()),
            will_message: None,
            socket_addr: "127.0.0.1:1883".parse().unwrap(),
        }).await.unwrap();

        let client_pubrec_packet = PubRecPacket::new(123);
        session_actor_addr
            .do_send(SessionActorMessage::InboundPacket(MqttPacketV3::Pubrec(client_pubrec_packet)));

        let msg = message_rx.recv().await.unwrap();

        match msg {
            ConnectionActorMessage::WritePacketToClient(pubrel) => {
                match pubrel {
                    yedmq_mqtt::MqttPacketV3::Pubrel(pubrel) => {
                        assert_eq!(pubrel.variable_header.packet_identifier, 123);
                    }
                    _ => panic!("expected ConnectionActorMessage::Pubrel"),
                }
            }
            _ => panic!("expected ConnectionActorMessage::Pubrel"),
        }
    }

    #[actix::test]
    pub async fn when_session_reactive_should_send_message_which_qos_is_not_0() {
        let mock_topic_manager = MockTopicManagerTrait::new();

        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let mut plugin_service = MockPluginService::new();
        plugin_service
            .expect_do_publish_authorizate()
            .returning(|_, _| std::result::Result::Ok(true));
        plugin_service.expect_do_on_publish().returning(|_, _| ());

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut _router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            false,
            mock_topic_manager,
            Arc::new(plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient.clone(),
            "127.0.0.1:1883".parse().unwrap(),
        );
        let session_actor_addr = session_actor.start();

        session_actor_addr
            .send(SessionActorMessage::ClientDisconnected)
            .await
            .unwrap();

        let client_publish_packet = PublishPacketBuilder::new("/a/b/c".to_string(), "hello".into())
            .qos(1)
            .build();

        session_actor_addr
            .send(SessionActorMessage::OutboundMessage(MqttPacketV3::Publish(
                client_publish_packet,
            )))
            .await
            .unwrap();

        session_actor_addr.send(SessionActorMessage::Reconnect {
            conn: connection_recipient.clone(),
            keep_alive: KEEP_ALIVE,
            clean_session: false,
            username: Some("test".to_string()),
            will_message: None,
            socket_addr: "127.0.0.1:1883".parse().unwrap(),
        }).await.unwrap();

        let msg = message_rx.recv().await.unwrap();
        match msg {
            ConnectionActorMessage::WritePacketToClient(publish) => {
                match publish {
                    yedmq_mqtt::MqttPacketV3::Publish(publish) => {
                        assert_eq!(publish.variable_header.topic_name, "/a/b/c");
                        assert_eq!(publish.fix_header.qos, Some(1));
                    }
                    _ => panic!("expected ConnectionActorMessage::Publish"),
                }
            }
            _ => panic!("expected ConnectionActorMessage::Publish"),
        }

    }

    #[actix::test]
    pub async fn when_session_inactivate_should_not_save_outbound_qos0_message() {
        let mock_topic_manager = MockTopicManagerTrait::new();

        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let mut plugin_service = MockPluginService::new();
        plugin_service
            .expect_do_publish_authorizate()
            .returning(|_, _| std::result::Result::Ok(true));
        plugin_service.expect_do_on_publish().returning(|_, _| ());

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut _router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            false,
            mock_topic_manager,
            Arc::new(plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        );
        let session_actor_addr = session_actor.start();

        session_actor_addr
            .send(SessionActorMessage::ClientDisconnected)
            .await
            .unwrap();

        let client_publish_packet =
            PublishPacketBuilder::new("/a/b/c".to_string(), "hello".into()).build();

        session_actor_addr
            .send(SessionActorMessage::OutboundMessage(MqttPacketV3::Publish(
                client_publish_packet,
            )))
            .await
            .unwrap();

        let pending_message_len = session_actor_addr
            .send(GetPendingMessagesCount {})
            .await
            .unwrap();

        assert_eq!(0, pending_message_len);
    }

    #[actix::test]
    pub async fn when_receive_qos_2_packet_should_correctly_handle_the_whole_process() {
        let mock_topic_manager = MockTopicManagerTrait::new();

        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let mut plugin_service = MockPluginService::new();
        plugin_service
            .expect_do_publish_authorizate()
            .returning(|_, _| std::result::Result::Ok(true));
        plugin_service.expect_do_on_publish().returning(|_, _| ());

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut _router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            true,
            mock_topic_manager,
            Arc::new(plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();
        let client_publish_packet = PublishPacketBuilder::new("/a/b/c".to_string(), "hello".into())
            .packet_identifier(123)
            .qos(2)
            .build();

        session_actor
            .send(SessionActorMessage::InboundPacket(MqttPacketV3::Publish(
                client_publish_packet,
            )))
            .await
            .unwrap();

        let message = message_rx.recv().await.unwrap();

        match message {
            ConnectionActorMessage::WritePacketToClient(msg) => match msg {
                MqttPacketV3::Pubrec(packet) => {
                    assert_eq!(packet.variable_header.packet_identifier, 123);
                }
                _ => panic!("Unexpected message"),
            },
            _ => panic!("Unexpected message"),
        }

        let client_pubrel_packet = PubRelPacket::new(123);
        session_actor
            .send(SessionActorMessage::InboundPacket(MqttPacketV3::Pubrel(
                client_pubrel_packet,
            )))
            .await
            .unwrap();

        let message = message_rx.recv().await.unwrap();

        match message {
            ConnectionActorMessage::WritePacketToClient(msg) => match msg {
                MqttPacketV3::Pubcomp(packet) => {
                    assert_eq!(packet.variable_header.packet_identifier, 123);
                }
                _ => panic!("Unexpected message"),
            },
            _ => panic!("Unexpected message"),
        }
    }

    #[actix::test]
    pub async fn when_send_qos_1_packet_if_not_receive_ack_should_retry() {
        let mock_topic_manager = MockTopicManagerTrait::new();

        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let plugin_service = MockPluginService::new();

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut _router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            true,
            mock_topic_manager,
            Arc::new(plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE + INFLIGHT_RETRY * 2, // ensure retry before keep-alive expired
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();
        let client_publish_packet = PublishPacketBuilder::new("/a/b/c".to_string(), "hello".into())
            .packet_identifier(123)
            .qos(1)
            .build();

        session_actor
            .send(SessionActorMessage::OutboundMessage(MqttPacketV3::Publish(
                client_publish_packet,
            )))
            .await
            .unwrap();

        let message = message_rx.recv().await.unwrap();

        match message {
            ConnectionActorMessage::WritePacketToClient(msg) => match msg {
                MqttPacketV3::Publish(packet) => {
                    assert_eq!(packet.variable_header.packet_identifier, Some(123));
                    assert_eq!(packet.fix_header.dup, None);
                }
                _ => panic!("Unexpected message"),
            },
            _ => panic!("Unexpected message"),
        }

        let message = message_rx.recv().await.unwrap();

        match message {
            ConnectionActorMessage::WritePacketToClient(msg) => match msg {
                MqttPacketV3::Publish(packet) => {
                    assert_eq!(packet.variable_header.packet_identifier, Some(123));
                    assert_eq!(packet.fix_header.dup, Some(1));
                }
                _ => panic!("Unexpected message"),
            },
            _ => panic!("Unexpected message"),
        }
    }

    #[actix::test]
    pub async fn when_receive_qos_1_packet_should_correctly_handle_the_whole_process() {
        let mock_topic_manager = MockTopicManagerTrait::new();

        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let mut plugin_service = MockPluginService::new();
        plugin_service
            .expect_do_publish_authorizate()
            .returning(|_, _| std::result::Result::Ok(true));
        plugin_service.expect_do_on_publish().returning(|_, _| ());

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut _router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            true,
            mock_topic_manager,
            Arc::new(plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();
        let client_publish_packet = PublishPacketBuilder::new("/a/b/c".to_string(), "hello".into())
            .packet_identifier(123)
            .qos(1)
            .build();

        session_actor
            .send(SessionActorMessage::InboundPacket(MqttPacketV3::Publish(
                client_publish_packet,
            )))
            .await
            .unwrap();

        let message = message_rx.recv().await.unwrap();

        match message {
            ConnectionActorMessage::WritePacketToClient(msg) => match msg {
                MqttPacketV3::Puback(packet) => {
                    assert_eq!(packet.variable_header.packet_identifier, 123);
                }
                _ => panic!("Unexpected message"),
            },
            _ => panic!("Unexpected message"),
        }
    }

    #[actix::test]
    pub async fn when_receive_pingreq_packet_shoud_send_pingresp_to_connection() {
        let mock_topic_manager = MockTopicManagerTrait::new();
        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let plugin_service = MockPluginService::new();

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            false,
            mock_topic_manager,
            Arc::new(plugin_service),
            tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();

        session_actor
            .send(SessionActorMessage::InboundPacket(MqttPacketV3::Pingreq(
                PingreqPacketBuilder::new().build(),
            )))
            .await
            .unwrap();

        let msg = message_rx.recv().await.unwrap();
        match msg {
            ConnectionActorMessage::WritePacketToClient(packet) => match packet {
                MqttPacketV3::Pingresp(_pingresp_packet) => (),
                _ => panic!("expect pingresp packet"),
            },
            _ => panic!("expect outbound message"),
        }
    }

    #[actix::test]
    pub async fn when_force_disconnect_should_send_disconnect_to_connection() {
        let mock_topic_manager = MockTopicManagerTrait::new();
        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let plugin_service = MockPluginService::new();

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            false,
            mock_topic_manager,
            Arc::new(plugin_service),
            tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();

        session_actor
            .send(SessionActorMessage::ForceDisconnect)
            .await
            .unwrap();

        let msg = message_rx.recv().await.unwrap();
        match msg {
            ConnectionActorMessage::Disconnect => {
                assert!(true);
            }
            _ => {
                unreachable!()
            }
        }
    }

    #[actix::test]
    pub async fn when_receive_publish_packet_with_retain_flag_and_empty_payload_should_unset_reatin_message_in_topic_manager(
    ) {
        let publish_packet_retain = yedmq_mqtt::MqttPacketV3::Publish(
            yedmq_mqtt::v3::publish::PublishPacketBuilder::new("/a/b/c".to_string(), vec![])
                .retain(true)
                .build(),
        );
        let mut mock_topic_manager = MockTopicManagerTrait::new();
        mock_topic_manager
            .expect_clean_retain_publish_packet()
            .times(1)
            .returning(|_, _| {
                let future = async { std::result::Result::Ok(()) };
                Box::pin(future)
            });

        let mut plugin_service = MockPluginService::new();
        plugin_service
            .expect_do_publish_authorizate()
            .returning(|_, _| std::result::Result::Ok(true));
        plugin_service.expect_do_on_publish().returning(|_, _| ());

        let (message_tx, mut _message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            true,
            Arc::new(RwLock::new(mock_topic_manager)),
            Arc::new(plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();

        session_actor
            .send(SessionActorMessage::InboundPacket(publish_packet_retain))
            .await
            .unwrap();

        router_rx.recv().await.unwrap();
    }

    #[actix::test]
    pub async fn when_receive_publish_packet_with_retain_flag_should_set_reatin_message_in_topic_manager(
    ) {
        let publish_packet_retain = yedmq_mqtt::MqttPacketV3::Publish(
            yedmq_mqtt::v3::publish::PublishPacketBuilder::new(
                "/a/b/c".to_string(),
                "hello".as_bytes().to_vec(),
            )
            .retain(true)
            .build(),
        );
        let mut mock_topic_manager = MockTopicManagerTrait::new();
        mock_topic_manager
            .expect_register_retain_publish_packet()
            .times(1)
            .returning(|_, _, _| {
                let future = async { std::result::Result::Ok(()) };
                Box::pin(future)
            });

        let mut plugin_service = MockPluginService::new();
        plugin_service
            .expect_do_publish_authorizate()
            .returning(|_, _| std::result::Result::Ok(true));
        plugin_service.expect_do_on_publish().returning(|_, _| ());

        let (message_tx, mut _message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            false,
            Arc::new(RwLock::new(mock_topic_manager)),
            Arc::new(plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();

        session_actor
            .send(SessionActorMessage::InboundPacket(publish_packet_retain))
            .await
            .unwrap();

        router_rx.recv().await.unwrap();
    }

    #[actix::test]
    async fn when_receive_disconnect_packet_from_connection_session_actor_should_not_stop_if_clean_session_is_false(
    ) {
        let mock_topic_manager = MockTopicManagerTrait::new();
        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let plugin_service = MockPluginService::new();

        let (message_tx, mut _message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            false,
            mock_topic_manager,
            Arc::new(plugin_service),
            tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();

        session_actor
            .send(SessionActorMessage::ClientDisconnected)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(session_actor.connected());
    }

    #[actix::test]
    async fn when_receive_subscribe_packet_from_connection_should_subscribe_topic() {
        let mut mock_topic_manager = MockTopicManagerTrait::new();
        mock_topic_manager
            .expect_handle_subscribe()
            .with(
                eq("tenant_a".to_string()),
                eq("client_a".to_string()),
                eq("/a/b/c".to_string()),
                eq(0),
            )
            .times(1)
            .returning(|_, _, _, _| {
                let future = async { std::result::Result::Ok(()) };
                Box::pin(future)
            });
        mock_topic_manager
            .expect_get_retain_publish_packet()
            .returning(|_, _| {
                let future = async { std::result::Result::Ok(vec![]) };
                Box::pin(future)
            });

        let mut mock_plugin_service = MockPluginService::new();
        mock_plugin_service
            .expect_do_subscribe_authorizate()
            .returning(|_, _| {
                let subscribe_result = SubscribeAuthorizationResult {
                    return_code: vec![SubscribeReturnCode::MaxQosLeastOnce],
                };
                std::result::Result::Ok(subscribe_result)
            });

        let (message_tx, mut _message_rx) = tokio::sync::mpsc::channel(10);
        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut _router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            true,
            Arc::new(RwLock::new(mock_topic_manager)),
            Arc::new(mock_plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();

        let subscribe_packet = SubscribePacketBuilder::new(1)
            .add_topic_filter(TopicFilter {
                topic_name: "/a/b/c".to_string(),
                qos: 0,
            })
            .build();

        session_actor
            .send(SessionActorMessage::InboundPacket(MqttPacketV3::Subscribe(
                subscribe_packet,
            )))
            .await
            .unwrap();

        let msg = _message_rx.recv().await.unwrap();
        match msg {
            ConnectionActorMessage::WritePacketToClient(packet) => match packet {
                MqttPacketV3::Suback(suback_packet) => {
                    assert_eq!(suback_packet.variable_header.packet_identifier, 1);
                    assert_eq!(suback_packet.payload.return_code.len(), 1);
                    assert_eq!(
                        suback_packet.payload.return_code[0],
                        SubscribeReturnCode::MaxQosLeastOnce.into()
                    );
                }
                _ => assert!(false),
            },
            _ => assert!(false),
        }
    }

    #[actix::test]
    async fn when_receive_publish_packet_from_connection_should_send_to_the_router() {
        let mock_topic_manager = MockTopicManagerTrait::new();

        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let mut plugin_service = MockPluginService::new();
        plugin_service
            .expect_do_publish_authorizate()
            .returning(|_, _| std::result::Result::Ok(true));
        plugin_service.expect_do_on_publish().returning(|_, _| ());

        let (message_tx, mut _message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (router_tx, mut router_rx) = tokio::sync::mpsc::channel(10);

        let session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            true,
            mock_topic_manager,
            Arc::new(plugin_service),
            router_tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();
        let client_publish_packet =
            PublishPacketBuilder::new("/a/b/c".to_string(), "hello".into()).build();

        session_actor
            .send(SessionActorMessage::InboundPacket(MqttPacketV3::Publish(
                client_publish_packet,
            )))
            .await
            .unwrap();

        let message = router_rx.recv().await.unwrap();

        match message {
            RouterCmd::RoutePacket {
                tenant_identifier,
                packet,
            } => {
                assert_eq!(tenant_identifier, "tenant_a");
                match packet {
                    MqttPacketV3::Publish(publish_packet) => {
                        assert_eq!(publish_packet.variable_header.topic_name, "/a/b/c");
                        assert_eq!(publish_packet.payload.payload, "hello".as_bytes().to_vec());
                    }
                    _ => assert!(false),
                }
            }
            _ => assert!(false),
        }
    }

    #[actix::test]
    async fn when_receive_outbound_message_should_send_to_connection_actor() {
        let mock_topic_manager = MockTopicManagerTrait::new();
        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let plugin_service = MockPluginService::new();

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        let _session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            true,
            mock_topic_manager,
            Arc::new(plugin_service),
            tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();

        let outbound_publish_packet =
            PublishPacketBuilder::new("/a/b/c".to_string(), "hello".into()).build();

        _session_actor
            .send(SessionActorMessage::OutboundMessage(MqttPacketV3::Publish(
                outbound_publish_packet,
            )))
            .await
            .unwrap();

        let msg = message_rx.recv().await.unwrap();
        match msg {
            ConnectionActorMessage::WritePacketToClient(msg) => match msg {
                MqttPacketV3::Publish(packet) => {
                    assert_eq!(packet.variable_header.topic_name, "/a/b/c");
                    assert_eq!(packet.payload.payload, "hello".as_bytes().to_vec());
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    #[actix::test]
    async fn when_keep_alive_expired_should_send_disconnect_to_connection_actor() {
        let mock_topic_manager = MockTopicManagerTrait::new();
        let mock_topic_manager = Arc::new(RwLock::new(mock_topic_manager));

        let plugin_service = MockPluginService::new();

        let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(10);

        let connection_actor = MockConnectionActor {
            message_sender: message_tx,
        }
        .start();
        let connection_recipient = connection_actor.recipient();

        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        let _session_actor = SessionActor::new(
            "tenant_a".to_string(),
            "client_a".to_string(),
            true,
            mock_topic_manager,
            Arc::new(plugin_service),
            tx,
            INFLIGHT_RETRY,
            None,
            KEEP_ALIVE,
            connection_recipient,
            "127.0.0.1:1883".parse().unwrap(),
        )
        .start();

        let msg = message_rx.recv().await.unwrap();
        match msg {
            ConnectionActorMessage::Disconnect => {
                assert!(true)
            }
            _ => {
                assert!(false)
            }
        }
    }
}
