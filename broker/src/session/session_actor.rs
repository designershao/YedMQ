use actix::{
    dev::ContextFutureSpawner, Actor, ActorFutureExt, Addr, AsyncContext, Context, Handler,
    MailboxError, Message, SpawnHandle, WrapFuture,
};
use log::warn;
use std::{collections::HashMap, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc::Sender, Mutex, RwLock},
};
use yedmq_mqtt::{
    v3::{
        disconnect::DisconnectPacket, pingresp::PingrespPacket, puback::PubAckPacket,
        pubcomp::PubCompPacket, publish::PublishPacket, pubrec::PubRecPacket, pubrel::PubRelPacket,
        suback::SubackPacket, subscribe::SubscribePacket, unsuback::UnSubackPacket,
        unsubscribe::UnsubscribePacket,
    },
    MqttPacketV3,
};
use yedmq_plugin::plugin::Client;

use crate::{
    inflight::Inflight,
    plugin_manager::{PluginService, SubscribeReturnCode},
    router::RouterCmd,
    topic::topic_manager::TopicManager,
};

use super::connection::{ConnectionActor, ConnectionActorMessage};

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
    ProcessPacket(MqttPacketV3),

    KeepAliveExpred,

    InflightRetry,
}

pub struct SessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    subscriptions: HashMap<String, QoS>,

    plugin_manager: Arc<dyn PluginService + 'static>,

    topic_manager: Arc<RwLock<TopicManager>>,

    conn: Option<Addr<ConnectionActor<T>>>,

    router_sender: Sender<RouterCmd>,

    inflight: Arc<Mutex<Inflight>>,

    client_info: Arc<Client>,

    keep_alive: u64,

    keep_alive_expired: bool,

    keep_alive_task_handle: Option<SpawnHandle>,

    inflight_retry_interval: u64,

    inflight_retry_task_handle: Option<SpawnHandle>,
}

impl<T> Actor for SessionActor<T>
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
    client_info: Arc<Client>,
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
    client_info: Arc<Client>,
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
    client_info: Arc<Client>,
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

impl<T> SessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn clean_up(&mut self, ctx: &mut <SessionActor<T> as Actor>::Context) {
        ctx.cancel_future(self.keep_alive_task_handle.unwrap());
    }

    fn handle_publish(
        &mut self,
        publish_packet: PublishPacket,
        ctx: &mut <SessionActor<T> as Actor>::Context,
    ) {
        let topic_manager = self.topic_manager.clone();
        let plugin_manager = self.plugin_manager.clone();
        let client_info = self.client_info.clone();
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
        ctx: &mut <SessionActor<T> as Actor>::Context,
    ) {
        let topic_manager = self.topic_manager.clone();
        let plugin_manager = self.plugin_manager.clone();
        let client_info = self.client_info.clone();
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
        ctx: &mut <SessionActor<T> as Actor>::Context,
    ) {
        let client_info = self.client_info.clone();
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
        ctx: &mut <SessionActor<T> as Actor>::Context,
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
                .get_next_state_packet_by_packet(pubrel_packet.variable_header.packet_identifier)
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
        ctx: &mut <SessionActor<T> as Actor>::Context,
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
                .get_next_state_packet_by_packet(pubrec_packet.variable_header.packet_identifier)
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
        ctx: &mut <SessionActor<T> as Actor>::Context,
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
                .get_next_state_packet_by_packet(puback_packet.variable_header.packet_identifier)
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
        ctx: &mut <SessionActor<T> as Actor>::Context,
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
                .get_next_state_packet_by_packet(pubcomp_packet.variable_header.packet_identifier)
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
        ctx: &mut <SessionActor<T> as Actor>::Context,
    ) {
        self.conn
            .clone()
            .unwrap()
            .do_send(ConnectionActorMessage::Disconnect);
        self.plugin_manager.do_on_disconnect(&self.client_info);
        self.clean_up(ctx);
    }

    fn reset_keep_alive_expired_flag(&mut self) {
        self.keep_alive_expired = false;
    }
}

impl<T> Handler<SessionActorMessage> for SessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();
    fn handle(&mut self, msg: SessionActorMessage, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            SessionActorMessage::ProcessPacket(packet) => {
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
            SessionActorMessage::KeepAliveExpred => {
                self.clean_up(ctx);
            }
            SessionActorMessage::InflightRetry => {
                let inflight = self.inflight.clone();
                let conn = self.conn.clone().unwrap();
                async move {
                    let inflight = inflight.lock().await;
                    let packets = inflight
                        .get_all_expired_packets_and_refresh_expired_time()
                        .await;
                    for packet in packets {
                        conn.do_send(ConnectionActorMessage::WritePacketToClient(packet));
                    }
                }
                .into_actor(self)
                .wait(ctx);
            }
        }
    }
}
