use actix::{
    dev::ContextFutureSpawner, Actor, ActorFutureExt, ActorTryFutureExt, Addr, Context, Handler,
    MailboxError, Message, WrapFuture,
};
use log::warn;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::RwLock,
};
use yedmq_mqtt::{
    v3::{suback::SubackPacket, subscribe::SubscribePacket},
    MqttPacketV3,
};
use yedmq_plugin::plugin::{self, Client};

use crate::{
    plugin_manager::{PluginService, SubscribeReturnCode},
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
}

pub struct SessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tenant_id: String,
    client_id: String,
    subscriptions: HashMap<String, QoS>,

    plugin_manager: Arc<dyn PluginService + 'static>,
    topic_manager: Arc<RwLock<TopicManager>>,

    conn: Option<Addr<ConnectionActor<T>>>,

    client_info: Arc<Client>,
}

impl<T> Actor for SessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Context = Context<Self>;
}

fn is_allowd_subscribe(subscribe_return_code: &SubscribeReturnCode) -> bool {
    !(matches!(subscribe_return_code, SubscribeReturnCode::Invalid)
        || matches!(subscribe_return_code, SubscribeReturnCode::Failure))
}

impl<T> SessionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn handle_subscribe(
        &mut self,
        subscribe_packet: SubscribePacket,
        ctx: &mut Context<SessionActor<T>>,
    ) -> Result<(), SessionActorError> {
        if self.conn.is_none() {
            return Err(SessionActorError::ConnectionNotSet);
        }

        let conn_actor = self.conn.clone().unwrap().clone();
        let plugin_manager = self.plugin_manager.clone();
        let topic_manager = self.topic_manager.clone();
        let client_info = self.client_info.clone();
        let tenant_id = self.tenant_id.clone();
        let client_id = self.client_id.clone();

        async move {
            let subscriptions = &subscribe_packet.payload.topic_filters;

            let packet_identifier = &subscribe_packet.variable_header.packet_identifier;

            let mut retain_messages: Vec<Arc<MqttPacketV3>> = vec![];

            let mut return_code: Vec<yedmq_mqtt::v3::suback::ReturnCode> = vec![];

            let topic_authorizate_result =
                plugin_manager.do_subscribe_authorizate(&client_info, &subscribe_packet)?;

            let mut succeed_subscribed_topics: Vec<(String, QoS)> = vec![];
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
                        match plugin_return_code[i] {
                            SubscribeReturnCode::MaxQosMostOnce => {
                                return_code.push(yedmq_mqtt::v3::suback::ReturnCode::MaxQos0);
                                succeed_subscribed_topics
                                    .push((topic.topic_name.clone(), QoS::AtMostOnce));
                            }
                            SubscribeReturnCode::MaxQosLeastOnce => {
                                return_code.push(yedmq_mqtt::v3::suback::ReturnCode::MaxQos1);
                                succeed_subscribed_topics
                                    .push((topic.topic_name.clone(), QoS::AtLeastOnce));
                            }
                            SubscribeReturnCode::MaxQosExactlyOnce => {
                                return_code.push(yedmq_mqtt::v3::suback::ReturnCode::MaxQos2);
                                succeed_subscribed_topics
                                    .push((topic.topic_name.clone(), QoS::ExactlyOnce));
                            }
                            SubscribeReturnCode::Failure => {
                                return_code.push(yedmq_mqtt::v3::suback::ReturnCode::Failure);
                            },
                            SubscribeReturnCode::Invalid => {
                                return_code.push(yedmq_mqtt::v3::suback::ReturnCode::Invalid);
                            },
                        }
                        let packets = topic_manager
                            .get_retain_publish_packet(tenant_id.clone(), topic.topic_name.clone())
                            .await;
                        if let Ok(packets) = packets {
                            for packet in packets {
                                retain_messages.push(packet);
                            }
                        }
                    }
                } else {
                    return_code.push(yedmq_mqtt::v3::suback::ReturnCode::Failure);
                }
            }
            for packet in retain_messages {
                let packet = (*packet).clone();
                conn_actor
                    .send(ConnectionActorMessage::WritePacketToClient(packet))
                    .await?;
            }

            conn_actor
                .send(ConnectionActorMessage::WritePacketToClient(
                    MqttPacketV3::Suback(SubackPacket::new(*packet_identifier, return_code)),
                ))
                .await?;

            Ok(succeed_subscribed_topics)
        }
        .into_actor(self)
        .map(
            |res: Result<Vec<(String, QoS)>, SessionActorError>, act, _ctx| {
                if let Ok(topics) = res {
                    for (topic, qos) in topics {
                        act.subscriptions.insert(topic, qos);
                    }
                } else {
                    warn!("subscribe error: {}", res.err().unwrap());
                }
            },
        )
        .wait(ctx);
        Ok(())
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
                if let MqttPacketV3::Subscribe(subscribe_packet) = packet {
                    if let Err(e) = self.handle_subscribe(subscribe_packet, ctx) {
                        warn!("subscribe error: {}", e);
                    }
                }
            }
        }
    }
}
