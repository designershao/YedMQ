use actix::{dev::{ContextFutureSpawner, MessageResponse}, fut, Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, MailboxError, Message, Recipient, ResponseFuture, SpawnHandle, SystemService, WrapFuture};
use log::{debug, error, info, warn};
use prost_types::Timestamp;
use serde::{Deserialize, Serialize};
use yedmq_plugin_host::{plugin_manager::PluginManager, protocol::plugin_protocol::{AuthAction, AuthorizeRequest, ClientDisconnectedEvent, MessagePublishRequest, MqttMessage}};
use std::{cmp, net::SocketAddr, sync::Arc, time::Duration};
use bytes::Bytes;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Sender},
    RwLock,
};
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

use crate::{
    inflight::{InflightError, InflightState},
    raft::{
        session_state::session_state_raft_actor::RegisterInflightTxPacket, topic::topic_raft_actor
    }, router_actor::RouterActor
};

use super::{
    session_manager_actor::SessionLifecycleMessage,
    session_state_storage::SessionState,
    WillMessage,
};

use crate::connection::{ConnectionActorMessage, DisconnectReason};
use crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor;
use crate::raft::topic::topic_raft_actor::TopicRaftActor;
use crate::raft::payload::PayloadStore;

pub struct Client {

    pub tenant_id: String,

    pub client_identifier: String,

    pub properties: ClientProperties,

    pub socket_addr: std::net::SocketAddr
}

pub struct ClientProperties {

    pub username: Option<String>,

    pub clean_session: bool,

    pub will_retain: bool,

    pub will_topic: Option<String>,

    pub will_message: Option<Vec<u8>>,

}

fn get_protobuf_now_timestamp() -> Timestamp {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}

pub struct SessionInfo {
    pub tenant_identifier: String,

    pub client_identifier: String,

    pub subscription_topics: Vec<String>,

    pub session_state: ActivityState,
}

impl<A, M> MessageResponse<A, M> for SessionInfo
where
    A: Actor,
    M: Message<Result = SessionInfo>,
{
    fn handle(
        self,
        _ctx: &mut <A as Actor>::Context,
        tx: Option<actix::dev::OneshotSender<<M as Message>::Result>>,
    ) {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
#[rtype(result = "SessionInfo")]
pub struct GetSessionInfo {}

#[derive(Message)]
#[rtype(result = "()")]
pub enum SessionActorMessage {
    InboundPacket(MqttPacketV3),

    OutboundMessage(MqttPacketV3),

    KeepAliveExpired,

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

    ForceStop,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ClientDisconnected {}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum ActivityState {
    Active,

    Inactive,
}

pub struct SessionActor {

    tenant_id: String,

    client_id: String,

    clean_session: bool,

    username: Option<String>,

    will_message: Option<WillMessage>,

    activity_state: ActivityState,

    plugin_manager: Arc<PluginManager>,

    conn_recipient: Option<Recipient<ConnectionActorMessage>>,

    conn_addr: Option<SocketAddr>,

    keep_alive: u64,

    keep_alive_expired: bool,

    keep_alive_task_handle: Option<SpawnHandle>,

    inflight_retry_interval: u64,

    inflight_retry_task_handle: Option<SpawnHandle>,

    state: Arc<RwLock<SessionState>>,

    session_lifecycle_tx: mpsc::Sender<SessionLifecycleMessage>,

    session_state_raft_actor: Addr<SessionStateRaftActor>,

    topic_raft_actor: Addr<topic_raft_actor::TopicRaftActor>,

    router_actors: Vec<Addr<RouterActor>>,

    payload_store: Option<Arc<dyn PayloadStore>>,

}

impl Actor for SessionActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let keep_alive_task_handle =
            ctx.run_interval(Duration::from_secs((self.keep_alive as f64 * 1.5) as u64), |act, ctx| {
                debug!(
                    "in keep alive current actor state {:?} ",
                    act.activity_state
                );
                if matches!(act.activity_state, ActivityState::Active)
                    && act.keep_alive_expired {
                        ctx.address().do_send(SessionActorMessage::KeepAliveExpired);
                    }
                act.keep_alive_expired = true // reset keep alive expired flag
            });
        self.keep_alive_task_handle = Some(keep_alive_task_handle);

        let inflight_retry_task_handle = ctx.run_interval(
            Duration::from_secs(self.inflight_retry_interval),
            |act, ctx| {
                debug!(
                    "in inflight retry current actor state {:?} ",
                    act.activity_state
                );
                if matches!(act.activity_state, ActivityState::Active) {
                    ctx.address().do_send(SessionActorMessage::InflightRetry);
                }
            },
        );
        self.inflight_retry_task_handle = Some(inflight_retry_task_handle);

        let state = self.state.clone();
        let tenant_id = self.tenant_id.clone();
        let client_id = self.client_id.clone();
        let session_actor_addr = ctx.address();
        let session_lifecycle_tx = self.session_lifecycle_tx.clone();

        let self_addr = ctx.address();
        let topic_raft_actor = self.topic_raft_actor.clone();
        let session_state_raft_actor = self.session_state_raft_actor.clone();
        let payload_store = self.payload_store.clone();

        async move {
            if let Err(e) = session_lifecycle_tx.send(SessionLifecycleMessage::SessionStarted).await {
                error!("send session started message to session manager error: {}, force stop the current session actor", e);
                self_addr.send(SessionActorMessage::ForceStop).await.unwrap();
                return;
            }
            let session_state_guard = state.write().await;
            let topic_iter = session_state_guard.subscriptions.iter();
            for (topic, qos) in topic_iter {
                info!("recover subscribe topic: {}, qos: {:?}", topic, qos);
                let qos_v = match qos {
                    QoS::AtMostOnce => 0,
                    QoS::AtLeastOnce => 1,
                    QoS::ExactlyOnce => 2,
                };
                match topic_raft_actor.send(
                    crate::raft::topic::topic_raft_actor::Subscribe {
                        tenant_id: tenant_id.clone(),
                        client_identifier: client_id.clone(),
                        topic: topic.clone(),
                        qos: qos_v,
                    },
                ).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        warn!(
                            "persistent session recover subscribe topic {} error, {}",
                            topic.clone(),
                            e
                        );
                    }
                    Err(e) => {
                        warn!(
                            "persistent session recover subscribe topic {} send message to topic raft actor error, {}",
                            topic.clone(),
                            e
                        );
                    }
                }
            }

            //
            loop {
                match session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::PopOfflineMessage {
                        tenant_id: tenant_id.clone(),
                        client_id: client_id.clone(),
                    }
                ).await.unwrap() {
                    Ok(Some(key)) => {
                        if let Some(store) = &payload_store {
                            match store.get(&key).await {
                                Ok(Some(data)) => {
                                    if let Ok(packet) = serde_json::from_slice::<MqttPacketV3>(&data) {
                                        session_actor_addr.do_send(SessionActorMessage::OutboundMessage(packet));
                                    }
                                }
                                Ok(None) => {
                                    warn!("Payload missing during recovery for key: {}", key);
                                }
                                Err(e) => {
                                    error!("Store error during recovery: {}", e);
                                }
                            }
                        }
                    },
                    Ok(None) => break,
                    Err(e) => {
                        warn!("session state raft client pop from pending queue error, {}", e);
                        break
                    },
                }

            }
            //

        }
        .into_actor(self)
        .wait(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("🗑️ session {} stopped", self.client_id);
    }
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

async fn do_handle_unsubscribe(
    unsubscribe_packet: UnsubscribePacket,
    client_info: &Client,
    topic_raft_actor_addr: Addr<TopicRaftActor>
) -> HandleUnSubscribeResult {
    let unsub_topic_filters = &unsubscribe_packet.payload.topic_filters;
    let mut succeed_unsubscriptions = vec![];
    {
        for topic in unsub_topic_filters {
            let tenant_id = client_info.tenant_id.clone();
            let client_id = client_info.client_identifier.clone();
            let res = topic_raft_actor_addr.send(
                topic_raft_actor::Unsubscribe {
                    tenant_id: tenant_id.clone(),
                    client_identifier: client_id.clone(),
                    topic: topic.topic_name.clone(),
                },
            ).await.unwrap();
            if let Err(e) = res {
                error!(
                    "session {} unsubscribe topic {} error: {}",
                    client_info.client_identifier, topic.topic_name, e
                );
            }
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
    plugin_manager: Arc<PluginManager>,
    session_state: Arc<RwLock<SessionState>>,
    clean_session: bool,
    router_actors: Vec<Addr<RouterActor>>,
    session_state_raft_actor: Addr<SessionStateRaftActor>,
    topic_raft_actor: Addr<topic_raft_actor::TopicRaftActor>,
    payload_store: Option<Arc<dyn PayloadStore>>,
) -> HandlePublishResult {

    let authorize_request =AuthorizeRequest {
        tenant_id: client_info.tenant_id.clone(),
        client_id: client_info.client_identifier.clone(),
        username: client_info.properties.username.clone().unwrap_or("".to_string()),
        action: AuthAction::Publish.into(),
        topic: publish_packet.variable_header.topic_name.clone(),
        qos: publish_packet.fix_header.qos.unwrap_or(0) as u32,
        context: None,
    };

    let publish_authorize_result = plugin_manager.call_authorize_hook(authorize_request).await;

    if publish_authorize_result.is_err() {
        return HandlePublishResult {
            inflight_packet: None,
        };
    }

    let publish_authorization = publish_authorize_result.unwrap().authorized;

    let mut result = HandlePublishResult {
        inflight_packet: None,
    };

    if publish_authorization {

        let message_publish_request = MessagePublishRequest {
            message: Some(MqttMessage{
                tenant_id: client_info.tenant_id.clone(),
                client_id: client_info.client_identifier.clone(),
                topic: publish_packet.variable_header.topic_name.clone(),
                payload: publish_packet.payload.payload.to_vec(),
                qos: publish_packet.fix_header.qos.unwrap_or(0) as u32,
                retain: publish_packet.fix_header.retain.unwrap_or(false),
                dup: publish_packet.fix_header.dup.unwrap_or(0) == 1,
                publish_time:None,
                properties: None,
                message_id: None,
            }),
            context: None,
        };

        plugin_manager.call_message_published_hook(message_publish_request).await;

        let mut session_state_guard = session_state.write().await;

        if publish_packet.fix_header.qos > Some(0) {
            debug!("client {} start process packet {:?} ", client_info.client_identifier, publish_packet.clone());
            
            let packet_id = publish_packet.variable_header.packet_identifier.unwrap();
            let qos = publish_packet.fix_header.qos.unwrap() as u8;
            
            let key = if let Some(store) = &payload_store {
                 let k = uuid::Uuid::new_v4().to_string();
                 let data = serde_json::to_vec(&MqttPacketV3::Publish(publish_packet.clone())).unwrap();
                 store.put(&k, bytes::Bytes::from(data)).await.unwrap();
                 k
            } else {
                 warn!("Payload store not available in do_handle_publish");
                 return HandlePublishResult { inflight_packet: None };
            };

            session_state_guard
                .inflight
                .register_with_rx_packet(packet_id, qos, key.clone());

            // if not clean session, should sync inflight rx packet to raft
            if !clean_session {
                let res = session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::RegisterInflightRxPacket {
                        tenant_id: client_info.tenant_id.clone(),
                        client_id: client_info.client_identifier.clone(),
                        packet_id,
                        qos,
                        packet_key: key.clone(),
                    },
                ).await.unwrap();
                if res.is_err() {
                    warn!("inflight register rx packet error, {}", res.unwrap_err());
                    return HandlePublishResult {
                        inflight_packet: None
                    };
                }
            }

            let packet = if qos == 1 {
                MqttPacketV3::Puback(yedmq_mqtt::v3::puback::PubAckPacket::new(packet_id))
            } else {
                MqttPacketV3::Pubrec(yedmq_mqtt::v3::pubrec::PubRecPacket::new(packet_id))
            };
            result.inflight_packet = Some(packet);
        }

        // process retain messages
        if publish_packet.fix_header.retain == Some(true) {
            // register retain publish packet

            // if publish packet paloyd is empty , clean retained publish packet
            if publish_packet.payload.payload.is_empty() {
                match topic_raft_actor.send(
                    topic_raft_actor::CleanRetainPublishPacket {
                        tenant_id: client_info.tenant_id.clone(),
                        topic_filter: publish_packet.variable_header.topic_name.clone(),
                    },
                ).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        error!("clean retain publish packet error: {}", e);
                    }
                    Err(e) => {
                        error!("Failed to send clean retain publish packet to topic raft actor: {}", e);
                    }
                }
            } else {
                match topic_raft_actor.send(
                    topic_raft_actor::RegisterRetainPublishPacket {
                        tenant_id: client_info.tenant_id.clone(),
                        client_id: client_info.client_identifier.clone(),
                        publish_packet: MqttPacketV3::Publish(publish_packet.clone()),
                    },
                ).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        error!("register retain publish packet error: {}", e);
                    }
                    Err(e) => {
                        error!("Failed to send register retain publish packet to topic raft actor: {}", e);
                    }
                }
            }
            //
        }
        //
        // select router actor based on topic
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(&publish_packet.variable_header.topic_name, &mut hasher);
        let hash = std::hash::Hasher::finish(&hasher);
        let router_actor = &router_actors[hash as usize % router_actors.len()];

        router_actor.do_send(crate::router_actor::RoutePacket{
            tenant_id: client_info.tenant_id.clone(),
            packet: MqttPacketV3::Publish(publish_packet.clone()),
        });
    }

    result
}

async fn do_handle_subscribe(
    subscribe_packet: &SubscribePacket,
    client_info: &Client,
    plugin_manager: Arc<PluginManager>,
) -> HandleSubscribeResult {
    let tenant_id = client_info.tenant_id.clone();

    let client_id = client_info.client_identifier.clone();

    let subscriptions = &subscribe_packet.payload.topic_filters;

    let mut retain_messages: Vec<Arc<MqttPacketV3>> = vec![];

    let mut return_code: Vec<yedmq_mqtt::v3::suback::ReturnCode> = vec![];

    let mut succeed_subscriptions: Vec<(String, QoS)> = vec![];

    for i in 0..subscriptions.len() {
        let topic = subscribe_packet.payload.topic_filters[i].clone();
        let authorize_request = AuthorizeRequest {
            tenant_id: tenant_id.clone(),
            client_id: client_id.clone(),
            username: client_info.properties.username.clone().unwrap_or("".to_string()),
            action: AuthAction::Subscribe.into(),
            topic: topic.topic_name.clone(),
            qos: topic.qos.into(),
            context: None,
        };

        let topic_authorizate_result = plugin_manager
            .call_authorize_hook(authorize_request)
            .await
            .unwrap();

        if topic_authorizate_result.authorized {
            let topic_raft_actor_addr = topic_raft_actor::TopicRaftActor::from_registry();
            let sub_result = topic_raft_actor_addr.send(
                topic_raft_actor::Subscribe {
                    tenant_id: tenant_id.clone(),
                    client_identifier: client_id.clone(),
                    topic: topic.topic_name.clone(),
                    qos: topic.qos,
                },
            ).await.unwrap();
            if sub_result.is_ok() {
                succeed_subscriptions.push((topic.topic_name.clone(), topic.qos.into()));
                match topic.qos {
                    0 => {
                        return_code.push(yedmq_mqtt::v3::suback::ReturnCode::MaxQos0);
                    }
                    1 => {
                        return_code.push(yedmq_mqtt::v3::suback::ReturnCode::MaxQos1);
                    }
                    2 => {
                        return_code.push(yedmq_mqtt::v3::suback::ReturnCode::MaxQos2);
                    }
                    _ => {
                        return_code.push(yedmq_mqtt::v3::suback::ReturnCode::Failure);
                    }
                }

                let topic_raft_actor_addr = topic_raft_actor::TopicRaftActor::from_registry();

                let packets = topic_raft_actor_addr.send(
                    topic_raft_actor::GetRetainPublishPacket {
                        tenant_id: tenant_id.clone(),
                        topic: topic.topic_name.clone(),
                    },
                ).await.unwrap();

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

type ThenCallback<A, R> = fn(R, &mut A, &mut Context<A>) -> actix::fut::Ready<R>;

impl SessionActor {
    pub fn new(
        tenant_id: String,
        client_id: String,
        clean_session: bool,
        plugin_manager: Arc<PluginManager>,
        inflight_retry_duration_secs: u64,
        will_message: Option<WillMessage>,
        keep_alive: u64,
        connection_actor_addr: Recipient<ConnectionActorMessage>,
        peer_addr: SocketAddr,
        session_state: Arc<RwLock<SessionState>>,
        session_lifecycle_tx: Sender<SessionLifecycleMessage>,
        session_state_raft_actor: Addr<SessionStateRaftActor>,
        topic_raft_actor: Addr<topic_raft_actor::TopicRaftActor>,
        router_actors: Vec<Addr<RouterActor>>,
        payload_store: Option<Arc<dyn PayloadStore>>,
    ) -> Self {
        SessionActor {
            plugin_manager,
            conn_recipient: Some(connection_actor_addr),
            conn_addr: Some(peer_addr),
            activity_state: ActivityState::Active,
            keep_alive_task_handle: None,
            clean_session,
            keep_alive,
            keep_alive_expired: true,
            inflight_retry_interval: inflight_retry_duration_secs,
            inflight_retry_task_handle: None,
            tenant_id,
            client_id,
            will_message,
            username: None,
            state: session_state,
            session_lifecycle_tx,
            session_state_raft_actor,
            topic_raft_actor,
            router_actors,
            payload_store,
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

    fn set_state(&mut self, ctx: &mut <SessionActor as Actor>::Context, state: ActivityState) {
        info!("set session {} state to {:?}", self.client_id, state);
        self.activity_state = state;
        let session_lifecycle_tx = self.session_lifecycle_tx.clone();
        async move {
            session_lifecycle_tx
                .send(SessionLifecycleMessage::SessionDeactivate)
                .await
                .unwrap();
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn force_stop(&mut self, ctx: &mut <SessionActor as Actor>::Context) {
        // if connection is still alive, stop the connection actor
        if let Some(conn_recipient) = &self.conn_recipient {
            conn_recipient.do_send(ConnectionActorMessage::Disconnect(DisconnectReason::Normal));
        }

        self.notify_session_manager_stopped(ctx, |_, _, ctx| {
            ctx.stop();
            fut::ready(())
        });
    }

    fn notify_session_manager_stopped(
        &mut self,
        ctx: &mut <SessionActor as Actor>::Context,
        callback_fn: ThenCallback<SessionActor, ()>,
    ) {
        let tenant_id = self.tenant_id.clone();
        let client_id = self.client_id.clone();
        let session_lifecycle_tx = self.session_lifecycle_tx.clone();
        async move {
            if let Err(e) = session_lifecycle_tx
                .send(SessionLifecycleMessage::SessionStopped {
                    tenant_id,
                    client_id,
                })
                .await
            {
                error!("send session stopped message error: {}", e);
            }
        }
        .into_actor(self)
        .then(callback_fn)
        .wait(ctx);
    }

    fn handle_publish(
        &mut self,
        publish_packet: PublishPacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        let plugin_manager = self.plugin_manager.clone();
        let client_info = self.get_plugin_client_info();
        let session_state = self.state.clone();
        let clean_session = self.clean_session;
        let router_actors = self.router_actors.clone();
        let session_state_raft_actor = self.session_state_raft_actor.clone();
        let topic_raft_actor = self.topic_raft_actor.clone();
        let payload_store = self.payload_store.clone();

        ctx.spawn(
        async move {
            do_handle_publish(
                publish_packet,
                client_info,
                plugin_manager,
                session_state,
                clean_session,
                router_actors,
                session_state_raft_actor,
                topic_raft_actor,
                payload_store,
            )
            .await
            }
            .into_actor(self)
            .map(|res, act, _ctx| {
                if let Some(packet) = res.inflight_packet {
                    let conn = act.conn_recipient.clone().unwrap();
                    let state = act.state.clone();
                    let payload_store = act.payload_store.clone();
                    let clean_session = act.clean_session;
                    let session_state_raft_actor = act.session_state_raft_actor.clone();
                    let tenant_id = act.tenant_id.clone();
                    let client_id = act.client_id.clone();

                    conn.do_send(ConnectionActorMessage::WritePacketToClient(packet.clone()));

                    // If it was a Puback (QoS 1 Receiver completion), trigger cleanup
                    if matches!(packet, MqttPacketV3::Puback(_)) {
                        if clean_session {
                            actix::spawn(async move {
                                Self::cleanup_local_finished_items(state, payload_store).await;
                            });
                        } else {
                            actix::spawn(async move {
                                let _ = session_state_raft_actor.send(
                                    crate::raft::session_state::session_state_raft_actor::InflightCleanFinishedItems {
                                        tenant_id,
                                        client_id,
                                    }
                                ).await;
                            });
                        }
                    }
                }
            })
        );
    }

    fn handle_subscribe(
        &mut self,
        subscribe_packet: SubscribePacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        let plugin_manager = self.plugin_manager.clone();
        let client_info = self.get_plugin_client_info();
        let conn = self.conn_recipient.clone().unwrap();
        let session_state = self.state.clone();
        let clean_session = self.clean_session;

        async move {
            let res = do_handle_subscribe(
                &subscribe_packet,
                &client_info,
                plugin_manager,
            )
            .await;
            for (i, packet) in res.retain_messages.into_iter().enumerate() {
                let packet = (*packet).clone();
                if let MqttPacketV3::Publish(mut p) = packet {
                    let retained_msg_qos = p.fix_header.qos.unwrap_or(0);
                    let min_qos = cmp::min(retained_msg_qos, subscribe_packet.payload.topic_filters[i].qos as i32);
                    if min_qos == 0 && retained_msg_qos > 0 {
                        p.variable_header.packet_identifier = None;
                        p.fix_header.remaining_length -= 2;
                    }
                    p.fix_header.qos = Some(min_qos);
                    conn.do_send(ConnectionActorMessage::WritePacketToClient(MqttPacketV3::Publish(p)));
                }
            }
            conn.do_send(ConnectionActorMessage::WritePacketToClient(
                yedmq_mqtt::MqttPacketV3::Suback(res.suback_packet),
            ));

            let client_info_ref = &client_info;
            let tenant_id = &client_info_ref.tenant_id;
            let client_id = &client_info_ref.client_identifier;
            if !clean_session {
                for (topic, qos) in res.succeed_subscriptions {
                    let qos_v = match qos {
                        QoS::AtMostOnce => 0,
                        QoS::AtLeastOnce => 1,
                        QoS::ExactlyOnce => 2,
                    };

                    session_state
                        .write()
                        .await
                        .subscriptions
                        .insert(topic.clone(), qos);

                    let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
                    let res = topic_raft_actor_addr.send(
                        crate::raft::topic::topic_raft_actor::Subscribe {
                            tenant_id: tenant_id.clone(),
                            client_identifier: client_id.clone(),
                            topic: topic.clone(),
                            qos: qos_v,
                        },
                    ).await.unwrap();

                    if res.is_err() {
                        warn!(
                            "persistent session add subscribe topic {} error, {}",
                            topic.clone(),
                            res.unwrap_err()
                        );
                    }
                }
            }
        }
        .into_actor(self)
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
        let session_state = self.state.clone();
        let conn = self.conn_recipient.clone().unwrap();
        let clean_session = self.clean_session;
        let topic_raft_actor = self.topic_raft_actor.clone();
        async move {
            let res = do_handle_unsubscribe(unsubscribe_packet, &client_info, topic_raft_actor.clone()).await;
            for topic in res.succeed_unsubscriptions {
                {
                    session_state.write().await.subscriptions.remove(&topic);
                }
                if !clean_session {

                    let res = topic_raft_actor.send(
                        crate::raft::topic::topic_raft_actor::Unsubscribe {
                            tenant_id: client_info.tenant_id.clone(),
                            client_identifier: client_info.client_identifier.clone(),
                            topic: topic.clone(),
                        },
                    ).await.unwrap();

                    if res.is_err() {
                        warn!(
                            "persistent session add unsubscribe topic {} error, {}",
                            topic.clone(),
                            res.unwrap_err()
                        );
                    }
                }
            }
            conn.do_send(ConnectionActorMessage::WritePacketToClient(
                yedmq_mqtt::MqttPacketV3::Unsuback(res.unsuback_packet),
            ));
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn handle_pubrel(
        &mut self,
        pubrel_packet: PubRelPacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        let session_state = self.state.clone();
        let conn = self.conn_recipient.clone().unwrap();
        let clean_session = self.clean_session;
        let client_info = self.get_plugin_client_info();
        let session_state_raft_actor = self.session_state_raft_actor.clone();
        let payload_store = self.payload_store.clone();

        async move {
            let mut session_state_guard = session_state.write().await;
            let packet_id = pubrel_packet.variable_header.packet_identifier;
            
            // Generate Pubcomp response
            let pubcomp = MqttPacketV3::Pubcomp(PubCompPacket::new(packet_id));

            if let Err(e) = conn
                .send(ConnectionActorMessage::WritePacketToClient(pubcomp))
                .await
            {
                warn!("handle pubrel write pubcomp to client error: {}", e);
            } else if clean_session {
                session_state_guard
                    .inflight
                    .next_state(packet_id);
                // Trigger local cleanup
                let state = session_state.clone();
                let store = payload_store.clone();
                actix::spawn(async move {
                    Self::cleanup_local_finished_items(state, store).await;
                });
            } else {
                let res = session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::AdvanceInflightState {
                        tenant_id: client_info.tenant_id.clone(),
                        client_id: client_info.client_identifier.clone(),
                        packet_id: packet_id.into(),
                    },
                ).await.unwrap();
                
                if res.is_ok() {
                    session_state_guard
                        .inflight
                        .next_state(packet_id);
                    
                    // Trigger Raft cleanup
                    let _ = session_state_raft_actor.send(
                        crate::raft::session_state::session_state_raft_actor::InflightCleanFinishedItems {
                            tenant_id: client_info.tenant_id.clone(),
                            client_id: client_info.client_identifier.clone(),
                        },
                    ).await;
                    session_state_guard.inflight.clean_finished_items();
                } else {
                    warn!("handle pubrel raft advance state error {}", res.unwrap_err());
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
        let session_state = self.state.clone();
        let conn = self.conn_recipient.clone().unwrap();
        let clean_session = self.clean_session;
        let client_info = self.get_plugin_client_info();
        let session_state_raft_actor = self.session_state_raft_actor.clone();

        async move {
            let mut session_state_guard = session_state.write().await;
            let packet_id = pubrec_packet.variable_header.packet_identifier;
            
            // Generate Pubrel command
            let pubrel = MqttPacketV3::Pubrel(PubRelPacket::new(packet_id));

            if let Err(e) = conn
                .send(ConnectionActorMessage::WritePacketToClient(pubrel))
                .await
            {
                warn!("handle pubrec write pubrel to client error: {}", e);
            } else if clean_session {
                session_state_guard
                    .inflight
                    .next_state(packet_id);
            } else {
                let res = session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::AdvanceInflightState {
                        tenant_id: client_info.tenant_id.clone(),
                        client_id: client_info.client_identifier.clone(),
                        packet_id: packet_id.into(),
                    },
                ).await.unwrap();

                if res.is_ok() {
                    session_state_guard
                        .inflight
                        .next_state(packet_id);
                } else {
                    warn!("handle pubrec raft advance state error {}", res.unwrap_err());
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
        let session_state = self.state.clone();
        let clean_session = self.clean_session;
        let client_info = self.get_plugin_client_info();
        let session_state_raft_actor = self.session_state_raft_actor.clone();
        let payload_store = self.payload_store.clone();

        async move {
            let mut session_state_guard = session_state.write().await;
            let packet_id = puback_packet.variable_header.packet_identifier;

            if clean_session {
                session_state_guard
                    .inflight
                    .next_state(packet_id);
                let state = session_state.clone();
                let store = payload_store.clone();
                actix::spawn(async move {
                    Self::cleanup_local_finished_items(state, store).await;
                });
            } else {
                let res = session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::AdvanceInflightState {
                        tenant_id: client_info.tenant_id.clone(),
                        client_id: client_info.client_identifier.clone(),
                        packet_id: packet_id.into(),
                    },
                ).await.unwrap();

                if res.is_ok() {
                    session_state_guard
                        .inflight
                        .next_state(packet_id);
                    
                    let _ = session_state_raft_actor.send(
                        crate::raft::session_state::session_state_raft_actor::InflightCleanFinishedItems {
                            tenant_id: client_info.tenant_id.clone(),
                            client_id: client_info.client_identifier.clone(),
                        },
                    ).await;
                    session_state_guard.inflight.clean_finished_items();
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

                let session_state = self.state.clone();

                let clean_session = self.clean_session;

                let client_info = self.get_plugin_client_info();

                let session_state_raft_actor = self.session_state_raft_actor.clone();

                let payload_store = self.payload_store.clone();

        

                async move {

                    let mut session_state_guard = session_state.write().await;

                    let packet_id = pubcomp_packet.variable_header.packet_identifier;

        

                    if clean_session {

                        session_state_guard

                            .inflight

                            .next_state(packet_id);

                        let state = session_state.clone();

                        let store = payload_store.clone();

                        actix::spawn(async move {

                            Self::cleanup_local_finished_items(state, store).await;

                        });

                    } else {

                    let res = session_state_raft_actor.send(

                        crate::raft::session_state::session_state_raft_actor::AdvanceInflightState {

                            tenant_id: client_info.tenant_id.clone(),

                            client_id: client_info.client_identifier.clone(),

                            packet_id: packet_id.into(),

                        },

                    ).await.unwrap();

    

                    if res.is_ok() {

                        session_state_guard

                            .inflight

                            .next_state(packet_id);

                        

                        let _ = session_state_raft_actor.send(

                            crate::raft::session_state::session_state_raft_actor::InflightCleanFinishedItems {

                                tenant_id: client_info.tenant_id.clone(),

                                client_id: client_info.client_identifier.clone(),

                            },

                        ).await;

                        session_state_guard.inflight.clean_finished_items();

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
            .do_send(ConnectionActorMessage::Disconnect(DisconnectReason::Normal));

        let plugin_manager = self.plugin_manager.clone();
        let tenant_id = self.tenant_id.clone();
        let client_id = self.client_id.clone();
        async move {
            let client_disconnected_event = ClientDisconnectedEvent {
                tenant_id,
                client_id,
                reason: "Normal Disconnect".to_string(),
                disconnect_time: Some(get_protobuf_now_timestamp()),
                session_info: None,
            };
            plugin_manager.call_client_disconnected_hook(client_disconnected_event).await;
        }.into_actor(self)
        .wait(ctx);

        if !self.clean_session {
            self.set_state(ctx, ActivityState::Inactive);
        } else {
            self.force_stop(ctx);
        }
    }

    fn reset_keep_alive_expired_flag(&mut self) {
        self.keep_alive_expired = false;
    }

    async fn cleanup_local_finished_items(
        state: Arc<RwLock<SessionState>>,
        payload_store: Option<Arc<dyn PayloadStore>>,
    ) {
        let mut session_state_guard = state.write().await;
        let freed_keys = session_state_guard.inflight.clean_finished_items();
        if let Some(store) = payload_store {
            for key in freed_keys {
                if let Err(e) = store.delete(&key).await {
                    error!("Failed to delete freed payload {}: {}", key, e);
                }
            }
        }
    }

    fn clean_will_message(&mut self) {
        self.will_message = None;
    }

    fn send_will_message(&mut self, ctx: &mut <SessionActor as Actor>::Context, callback_fn: ThenCallback<SessionActor, ()>) {
        let tenant_id = self.tenant_id.clone();
        let will_message = self.will_message.take();
        let router_actors = self.router_actors.clone();
        async move {
            if will_message.is_some() {
                let will_message = will_message.as_ref().unwrap();
                let publish_packet = PublishPacketBuilder::new(
                    will_message.will_topic.clone(),
                    Bytes::copy_from_slice(will_message.will_message.as_slice()),
                )
                .retain(will_message.will_retain)
                .qos(will_message.will_qos)
                .build();
                
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                std::hash::Hash::hash(&will_message.will_topic, &mut hasher);
                let hash = std::hash::Hasher::finish(&hasher);
                let router_actor = &router_actors[hash as usize % router_actors.len()];
                router_actor
                    .do_send(crate::router_actor::RoutePacket {
                        tenant_id,
                        packet: MqttPacketV3::Publish(publish_packet),
                    });
            }
        }
        .into_actor(self)
        .then(callback_fn)
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
                if let MqttPacketV3::Publish(mut publish_packet) = packet {
                    let tenant_id = self.tenant_id.clone();
                    let client_id = self.client_id.clone();
                    let clean_session = self.clean_session;
                    let session_state = self.state.clone();
                    let session_state_raft_actor = self.session_state_raft_actor.clone();
                    let payload_store = self.payload_store.clone();
                    let conn = self.conn_recipient.clone();

                    if matches!(self.activity_state, ActivityState::Active) && conn.is_some() {
                        let conn = conn.unwrap();
                        if publish_packet.fix_header.qos.unwrap_or(0) == 0 {
                            conn.do_send(ConnectionActorMessage::WritePacketToClient(MqttPacketV3::Publish(publish_packet)));
                        } else {
                            ctx.spawn(async move {
                                let mut session_state_guard = session_state.write().await;
                                let packet_id = publish_packet.variable_header.packet_identifier.unwrap();
                                let qos = publish_packet.fix_header.qos.unwrap() as u8;

                                let key = if let Some(store) = &payload_store {
                                    let k = uuid::Uuid::new_v4().to_string();
                                    let data = serde_json::to_vec(&MqttPacketV3::Publish(publish_packet.clone())).unwrap();
                                    if let Err(e) = store.put(&k, bytes::Bytes::from(data)).await {
                                        error!("Failed to store payload: {}", e);
                                        return;
                                    }
                                    k
                                } else {
                                    warn!("Payload store missing");
                                    return;
                                };

                                if clean_session {
                                    if let Err(e) = session_state_guard.inflight.register_with_tx_packet(packet_id, qos, key.clone()) {
                                        if matches!(e, InflightError::PacketIdentifierHasExisted) {
                                            if let Some(new_id) = session_state_guard.inflight.allocate_packet_id() {
                                                publish_packet.variable_header.packet_identifier = Some(new_id);
                                                session_state_guard.inflight.register_with_tx_packet(new_id, qos, key.clone()).unwrap();
                                            } else { return; }
                                        }
                                    }
                                } else {
                                    let res = session_state_raft_actor.send(RegisterInflightTxPacket {
                                        tenant_id: tenant_id.clone(),
                                        client_id: client_id.clone(),
                                        packet_id,
                                        qos,
                                        packet_key: key.clone(),
                                    }).await.unwrap();

                                    if let Err(e) = res {
                                        if let crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::InflightError(InflightError::PacketIdentifierHasExisted) = e {
                                            if let Some(new_id) = session_state_guard.inflight.allocate_packet_id() {
                                                publish_packet.variable_header.packet_identifier = Some(new_id);
                                                let _ = session_state_raft_actor.send(RegisterInflightTxPacket {
                                                    tenant_id,
                                                    client_id,
                                                    packet_id: new_id,
                                                    qos,
                                                    packet_key: key.clone(),
                                                }).await;
                                            }
                                        } else { error!("Raft error: {}", e); return; }
                                    }
                                }
                                let _ = conn.send(ConnectionActorMessage::WritePacketToClient(MqttPacketV3::Publish(publish_packet))).await;
                            }.into_actor(self));
                        }
                    } else if publish_packet.fix_header.qos.unwrap_or(0) > 0 {
                        let client_info = self.get_plugin_client_info();
                        ctx.spawn(async move {
                            let mut session_state_guard = session_state.write().await;
                            let key = if let Some(store) = &payload_store {
                                let k = uuid::Uuid::new_v4().to_string();
                                let data = serde_json::to_vec(&MqttPacketV3::Publish(publish_packet.clone())).unwrap();
                                if let Err(e) = store.put(&k, bytes::Bytes::from(data)).await {
                                    error!("Failed to store offline payload: {}", e);
                                    return;
                                }
                                k
                            } else { return; };

                            session_state_guard.pending_messages.push(key.clone());
                            if !clean_session {
                                let _ = session_state_raft_actor.send(crate::raft::session_state::session_state_raft_actor::StoreOfflineMessage {
                                    tenant_id: client_info.tenant_id.clone(),
                                    client_id: client_info.client_identifier.clone(),
                                    packet_key: key,
                                }).await;
                            }
                        }.into_actor(self));
                    }
                }
            }
            SessionActorMessage::KeepAliveExpired => {
                // send will message and clean up
                self.send_will_message(ctx, |_, actor, ctx| {
                    if let Some(recipient) = &actor.conn_recipient {
                        info!("keep alive expired, stop session {} connection", actor.client_id);
                        recipient.do_send(ConnectionActorMessage::Disconnect(DisconnectReason::KeepAliveExpired));
                        if !actor.clean_session {
                            actor.set_state(ctx, ActivityState::Inactive);
                        } else {
                            actor.force_stop(ctx);
                        }
                    }
                    fut::ready(())
                });
            }
            SessionActorMessage::InflightRetry => {
                let session_state = self.state.clone();
                let session_actor_addr = ctx.address();
                let payload_store = self.payload_store.clone();
                ctx.spawn(
                    async move {
                      let mut session_state_guard = session_state.write().await;
                      let packets = session_state_guard
                        .inflight
                        .get_all_expired_packet_keys_and_refresh_expired_time();
                      
                      if let Some(store) = &payload_store {
                          for (packet_id, key) in packets {
                              let state = session_state_guard.inflight.get_inflight_current_state(packet_id);
                              
                              match state {
                                  Some(InflightState::WaitPubcomp) => {
                                      let packet = MqttPacketV3::Pubrel(PubRelPacket::new(packet_id));
                                      session_actor_addr.do_send(SessionActorMessage::OutboundMessage(packet));
                                  },
                                  Some(InflightState::WaitPubrel) => {
                                      let packet = MqttPacketV3::Pubrec(PubRecPacket::new(packet_id));
                                      session_actor_addr.do_send(SessionActorMessage::OutboundMessage(packet));
                                  },
                                  _ => {
                                      match store.get(&key).await {
                                          Ok(Some(data)) => {
                                              if let Ok(mut packet) = serde_json::from_slice::<MqttPacketV3>(&data) {
                                                  packet.set_dup(1);
                                                  session_actor_addr.do_send(SessionActorMessage::OutboundMessage(packet));
                                              }
                                          }
                                          _ => {}
                                      }
                                  }
                              }
                          }
                      }
                    }.into_actor(self)
                );
            }
            SessionActorMessage::ForceDisconnect => {
                if matches!(self.activity_state, ActivityState::Active) {
                    let conn = self.conn_recipient.clone().unwrap();
                    async move {
                        conn.send(ConnectionActorMessage::Disconnect(DisconnectReason::Normal)).await
                    }
                    .into_actor(self)
                    .then(|res, act, ctx| {
                        let connection_already_stopped = match res {
                            Err(e) => {
                                match e {
                                    MailboxError::Closed => {
                                        // connection already closed
                                        true
                                    }
                                    MailboxError::Timeout => {
                                        warn!("force disconnect send disconnect to connection time out: {}", e);
                                        false
                                    }
                                }
                            }
                            Ok(_) => true
                        };
                        if connection_already_stopped {
                            if !act.clean_session {
                                act.set_state(ctx, ActivityState::Inactive);
                            } else {
                                info!("force disconnect, force stop session {}", act.client_id);
                                act.force_stop(ctx);
                            }
                        }
                        actix::fut::ready(())
                    })
                    .wait(ctx);
                }
            }
            SessionActorMessage::UnexpectClientDisconnected => {
                self.send_will_message(ctx, |_, actor, ctx| {
                    if !actor.clean_session {
                        actor.set_state(ctx, ActivityState::Inactive);
                    } else {
                        actor.force_stop(ctx);
                    }
                    fut::ready(())
                });
            }
            SessionActorMessage::Reconnect {
                conn,
                keep_alive,
                clean_session,
                username,
                will_message,
                ..
            } => {
                self.set_state(ctx, ActivityState::Active);
                self.conn_recipient = Some(conn);
                self.will_message = will_message;
                self.clean_session = clean_session;
                self.keep_alive = keep_alive;
                self.username = username;

                // start consume pending messages
                let session_state = self.state.clone();
                let session_actor_addr = ctx.address().clone();
                let tenant_id = self.tenant_id.clone();
                let client_identifier = self.client_id.clone();
                let topic_raft_actor = self.topic_raft_actor.clone();
                let payload_store = self.payload_store.clone();
                async move {
                    let mut session_state_guard = session_state.write().await;
                    for key in session_state_guard.pending_messages.drain(..) {
                        if let Some(store) = &payload_store {
                            match store.get(&key).await {
                                Ok(Some(data)) => {
                                    if let Ok(packet) = serde_json::from_slice::<MqttPacketV3>(&data) {
                                        session_actor_addr.do_send(SessionActorMessage::OutboundMessage(packet));
                                    }
                                }
                                _ => {
                                    warn!("Payload missing during reconnect for key: {}", key);
                                }
                            }
                        }
                    }
                    // update topic subscribe
                    debug!(
                        "start update topic subscribe for session {}",
                        client_identifier
                    );
                    for (topic, qos) in session_state_guard.subscriptions.iter() {
                        let qos_v = match qos {
                            QoS::AtLeastOnce => 1,
                            QoS::ExactlyOnce => 2,
                            QoS::AtMostOnce => 0,
                        };
                        let res = topic_raft_actor
                            .send(crate::raft::topic::topic_raft_actor::Subscribe {
                                tenant_id: tenant_id.clone(),
                                client_identifier: client_identifier.clone(),
                                topic: topic.clone(),
                                qos: qos_v,
                            })
                            .await
                            .unwrap();
                        if res.is_err() {
                            warn!("handle subscribe topic error {}", res.unwrap_err())
                        }
                    }
                    debug!(
                        "finish update topic subscribe for session {}",
                        client_identifier
                    );
                    //
                }
                .into_actor(self)
                .wait(ctx);
            }
            SessionActorMessage::ClientDisconnected => {
                if !self.clean_session {
                    self.set_state(ctx, ActivityState::Inactive);
                } else {
                    self.force_stop(ctx);
                }
            }
            SessionActorMessage::ForceStop => {
                debug!("receive force stop message for session {}", self.client_id);
                self.force_stop(ctx);
            }
        }
    }
}

impl Handler<GetSessionInfo> for SessionActor {
    type Result = ResponseFuture<SessionInfo>;

    fn handle(&mut self, _msg: GetSessionInfo, _ctx: &mut Self::Context) -> Self::Result {
        let session_state = self.state.clone();
        let activity_state = self.activity_state;
        let tenant_id = self.tenant_id.clone();
        let client_id = self.client_id.clone();

        let future = async move {
            SessionInfo {
                tenant_identifier: tenant_id,
                client_identifier: client_id,
                subscription_topics: session_state
                    .read()
                    .await
                    .subscriptions.keys().cloned()
                    .collect(),
                session_state: activity_state,
            }
        };

        Box::pin(future)
    }
}
