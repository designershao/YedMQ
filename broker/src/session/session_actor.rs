use actix::{
    dev::ContextFutureSpawner, Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context,
    Handler, MailboxError, Message, SpawnHandle, WrapFuture,
};
use log::warn;
use std::{collections::{HashMap, VecDeque}, net::SocketAddr, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc::Sender, Mutex, RwLock},
};
use yedmq_mqtt::{
    v3::{
        disconnect::DisconnectPacket, pingresp::PingrespPacket, puback::PubAckPacket,
        pubcomp::PubCompPacket, publish::{PublishPacket, PublishPacketBuilder}, pubrec::PubRecPacket, pubrel::PubRelPacket,
        suback::SubackPacket, subscribe::SubscribePacket, unsuback::UnSubackPacket,
        unsubscribe::UnsubscribePacket,
    },
    MqttPacketV3,
};
use yedmq_plugin::plugin::{Client, ClientProperties};

use crate::{
    inflight::Inflight,
    plugin_manager::{PluginService, SubscribeReturnCode},
    router::RouterCmd,
    topic::topic_manager::TopicManager,
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
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ClientDisconnected {}

#[derive(Message)]
#[rtype(result = "()")]
pub struct UnexpectClientDisconnected {}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ForceDisconnect {}


#[derive(Message)]
#[rtype(result = "()")]
pub struct Reconnect<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub conn: Addr<ConnectionActor<T>>,

    pub keep_alive: u64,

    pub clean_session: bool,

    pub username: Option<String>,

    pub will_message: Option<WillMessage>,

    pub socket_addr: std::net::SocketAddr,
}

pub enum SessionState {
    Active,

    Inactive,
}

#[cfg(not(test))]
pub type SessionActor<T> = RealSessionActor<T>;

pub struct RealSessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tenant_id: String,

    client_id: String,

    clean_session: bool,

    username: Option<String>,

    will_message: Option<WillMessage>,

    state: SessionState,

    subscriptions: HashMap<String, QoS>,

    plugin_manager: Arc<dyn PluginService + 'static>,

    topic_manager: Arc<RwLock<TopicManager>>,

    conn: Option<Addr<ConnectionActor<T>>>,

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

impl<T> Actor for RealSessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
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
    topic_manager: Arc<RwLock<TopicManager>>,
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
    topic_manager: Arc<RwLock<TopicManager>>,
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
    topic_manager: Arc<RwLock<TopicManager>>,
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

impl<T> RealSessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(
        tenant_id: String,
        client_id: String,
        clean_session: bool,
        topic_manager: Arc<RwLock<TopicManager>>,
        plugin_manager: Arc<dyn PluginService>,
        router_sender: Sender<RouterCmd>,
        inflight_retry_duration_secs: u64,
        will_message: Option<WillMessage>,
        keep_alive: u64,
    ) -> Self {
        RealSessionActor {
            topic_manager,
            plugin_manager,
            router_sender,
            conn: None,
            conn_addr: None,
            inflight: Arc::new(Mutex::new(Inflight::new(Duration::from_secs(
                inflight_retry_duration_secs,
            )))),
            state: SessionState::Inactive,
            keep_alive_task_handle: None,
            subscriptions: HashMap::new(),
            clean_session,
            keep_alive,
            keep_alive_expired: false,
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

    fn clean_up(&mut self, ctx: &mut <RealSessionActor<T> as Actor>::Context) {
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
        ctx: &mut <RealSessionActor<T> as Actor>::Context,
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
                let conn = act.conn.clone().unwrap();
                conn.do_send(ConnectionActorMessage::WritePacketToClient(packet));
            }
        })
        .wait(ctx);
    }

    fn handle_subscribe(
        &mut self,
        subscribe_packet: SubscribePacket,
        ctx: &mut <RealSessionActor<T> as Actor>::Context,
    ) {
        let topic_manager = self.topic_manager.clone();
        let plugin_manager = self.plugin_manager.clone();
        let client_info = self.get_plugin_client_info();
        async move {
            do_handle_subscribe(subscribe_packet, client_info, topic_manager, plugin_manager).await
        }
        .into_actor(self)
        .map(|res, act, _ctx| {
            let conn = act.conn.clone().unwrap();
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
        let conn = self.conn.clone().unwrap();
        conn.do_send(ConnectionActorMessage::WritePacketToClient(
            yedmq_mqtt::MqttPacketV3::Pingresp(PingrespPacket::new()),
        ));
    }

    fn handle_unsubscribe(
        &mut self,
        unsubscribe_packet: UnsubscribePacket,
        ctx: &mut <RealSessionActor<T> as Actor>::Context,
    ) {
        let client_info = self.get_plugin_client_info();
        let topic_manager = self.topic_manager.clone();
        async move { do_handle_unsubscribe(unsubscribe_packet, client_info, topic_manager).await }
            .into_actor(self)
            .map(|res, act, _ctx| {
                act.subscriptions
                    .retain(|k, _| !res.succeed_unsubscriptions.contains(k));
                let conn = act.conn.clone().unwrap();
                conn.do_send(ConnectionActorMessage::WritePacketToClient(
                    yedmq_mqtt::MqttPacketV3::Unsuback(res.unsuback_packet),
                ));
            })
            .wait(ctx);
    }

    fn handle_pubrel(
        &mut self,
        pubrel_packet: PubRelPacket,
        ctx: &mut <RealSessionActor<T> as Actor>::Context,
    ) {
        // To ensure synchronization of inflight information for QoS 1 and QoS 2,
        // the session actor must use the send method to first send the next stage response packet,
        // then wait for the connection actor to reply, confirming the write is complete,
        // before updating the inflight status again.

        let infight = self.inflight.clone();
        let conn = self.conn.clone().unwrap();

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
        ctx: &mut <RealSessionActor<T> as Actor>::Context,
    ) {
        // To ensure synchronization of inflight information for QoS 1 and QoS 2,
        // the session actor must use the send method to first send the next stage response packet,
        // then wait for the connection actor to reply, confirming the write is complete,
        // before updating the inflight status again.

        let infight = self.inflight.clone();
        let conn = self.conn.clone().unwrap();

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
        ctx: &mut <RealSessionActor<T> as Actor>::Context,
    ) {
        // To ensure synchronization of inflight information for QoS 1 and QoS 2,
        // the session actor must use the send method to first send the next stage response packet,
        // then wait for the connection actor to reply, confirming the write is complete,
        // before updating the inflight status again.

        let infight = self.inflight.clone();
        let conn = self.conn.clone().unwrap();

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
        ctx: &mut <RealSessionActor<T> as Actor>::Context,
    ) {
        // To ensure synchronization of inflight information for QoS 1 and QoS 2,
        // the session actor must use the send method to first send the next stage response packet,
        // then wait for the connection actor to reply, confirming the write is complete,
        // before updating the inflight status again.

        let infight = self.inflight.clone();
        let conn = self.conn.clone().unwrap();

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
        ctx: &mut <RealSessionActor<T> as Actor>::Context,
    ) {
        self.clean_will_message();
        self.conn
            .clone()
            .unwrap()
            .do_send(ConnectionActorMessage::Disconnect);

        self.plugin_manager.do_on_disconnect(&self.get_plugin_client_info());
        self.clean_up(ctx);
    }

    fn reset_keep_alive_expired_flag(&mut self) {
        self.keep_alive_expired = false;
    }

    fn clean_will_message(&mut self) {
        self.will_message = None;
    }

    fn send_will_message(&mut self, ctx: &mut <RealSessionActor<T> as Actor>::Context) {
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
        }.into_actor(self).wait(ctx);
    }
}

impl<T> Handler<SessionActorMessage> for RealSessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
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
                    if let MqttPacketV3::Publish(packet) = packet {
                        let conn = self.conn.clone().unwrap();
                        let inflight = self.inflight.clone();
                        async move {
                            if packet.fix_header.qos.or(Some(0)).unwrap() > 0 {
                                inflight.lock().await.register_with_tx_packet(&MqttPacketV3::Publish(packet.clone())).await;
                            }
                            conn.do_send(ConnectionActorMessage::WritePacketToClient(yedmq_mqtt::MqttPacketV3::Publish(packet)));
                        }.into_actor(self).wait(ctx);
                    }
                } else {
                    self.pending_messages.push(packet);
                }
            }
            SessionActorMessage::KeepAliveExpred => {
                // send will message and clean up
                self.send_will_message(ctx);
                self.clean_up(ctx);
            }
            SessionActorMessage::InflightRetry => {
                let inflight = self.inflight.clone();
                let conn = self.conn.clone().unwrap();
                async move {
                    let mut inflight = inflight.lock().await;
                    let packets = inflight
                        .get_all_expired_packets_and_refresh_expired_time()
                        .await;
                    for packet in packets {
                        let result = conn.send(ConnectionActorMessage::WritePacketToClient(packet.1)).await;
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
        }
    }
}

impl<T> Handler<Reconnect<T>> for RealSessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: Reconnect<T>, ctx: &mut Self::Context) -> Self::Result {
        self.set_state(SessionState::Active);
        self.conn = Some(msg.conn);
        self.will_message = msg.will_message;
        self.clean_session = msg.clean_session;
        self.keep_alive = msg.keep_alive;
        self.username = msg.username;

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
            ctx.address().do_send(SessionActorMessage::OutboundMessage(msg));
        }
    }
}

impl<T> Handler<ClientDisconnected> for RealSessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, _msg: ClientDisconnected, ctx: &mut Self::Context) -> Self::Result {
        self.clean_up(ctx);
    }
}

impl<T> Handler<UnexpectClientDisconnected> for RealSessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, _msg: UnexpectClientDisconnected, ctx: &mut Self::Context) -> Self::Result {
        self.send_will_message(ctx);
        self.clean_up(ctx);
    }
}

impl<T> Handler<ForceDisconnect> for RealSessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();
    
    fn handle(&mut self, _msg: ForceDisconnect, ctx: &mut Self::Context) -> Self::Result {
        if matches!(self.state, SessionState::Active) {
            let conn = self.conn.clone().unwrap();
            async move {
                conn.send(ConnectionActorMessage::Disconnect).await.unwrap();
            }.into_actor(self).wait(ctx);
            self.clean_up(ctx);
        }
    }
}

// For testing
#[cfg(test)]
use actix::actors::mocker::Mocker;
use actix::prelude::*;
#[cfg(test)]
use actix::SystemRegistry;

#[cfg(test)]
pub type SessionActor<T> = Mocker<RealSessionActor<T>>;

