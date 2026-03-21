use actix::{
    dev::{ContextFutureSpawner, MessageResponse},
    fut, Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, MailboxError,
    Message, Recipient, ResponseActFuture, ResponseFuture, SystemService, WrapFuture,
};
use bytes::Bytes;
use log::{debug, error, info, warn};
use prost_types::Timestamp;
use serde::{Deserialize, Serialize};
use std::{
    cmp,
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
    time::Duration,
};
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
use yedmq_plugin_host::{
    plugin_manager::{AuthorizeResult, PluginManager},
    protocol::plugin_protocol::{
        AuthAction, AuthorizeRequest, ClientDisconnectedEvent, MessagePublishRequest, MqttMessage,
    },
};

use crate::{
    inflight::{InflightError, InflightState},
    raft::{
        payload::PayloadError, session_state::session_state_raft_actor::RegisterInflightTxPacket,
        topic::topic_raft_actor,
    },
    router_actor::RouterActor,
};

use super::{
    session_actor_map_storage::SessionVersion, session_manager_actor::SessionLifecycleMessage,
    session_state_storage::SessionState, WillMessage,
};

use crate::connection::{ConnectionActorMessage, DisconnectReason};
use crate::metric::Metric;
use crate::raft::payload::PayloadStore;
use crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor;
use crate::raft::topic::topic_raft_actor::TopicRaftActor;
use crate::timer_actor::{
    RefreshTimer, RegisterInflight, RegisterKeepAlive, RemoveTimer, TimerActor, TimerType,
};

pub struct SessionMetrics {
    pub ip_address: std::sync::RwLock<Option<String>>, // client ip address

    pub connected: AtomicBool, // is client connected

    pub connected_at: AtomicU64, // latest client connected timestamp in milliseconds, 0 means never connected

    pub created_at: u64, // session created timestamp in milliseconds

    pub disconnected_at: AtomicU64, // latest client disconnected timestamp in milliseconds, 0 means never disconnected

    pub messages_received: AtomicU64, // total mqtt messages received

    pub messages_sent: AtomicU64, // total mqtt messages sent
}

impl Default for SessionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionMetrics {
    pub fn get_ipaddress(&self) -> Option<String> {
        match self.ip_address.read() {
            Ok(guard) => guard.clone(),
            Err(_) => None,
        }
    }

    pub fn get_connected(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_connected_at(&self) -> Option<u64> {
        let connected_at = self.connected_at.load(std::sync::atomic::Ordering::Relaxed);
        if connected_at == 0 {
            None
        } else {
            Some(connected_at)
        }
    }

    pub fn get_disconnected_at(&self) -> Option<u64> {
        let disconnected_at = self
            .disconnected_at
            .load(std::sync::atomic::Ordering::Relaxed);
        if disconnected_at == 0 {
            None
        } else {
            Some(disconnected_at)
        }
    }

    pub fn new() -> SessionMetrics {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time is before unix epoch")
            .as_millis() as u64;

        SessionMetrics {
            ip_address: std::sync::RwLock::new(None),

            connected: AtomicBool::new(true),

            connected_at: AtomicU64::new(0),

            created_at: now,

            disconnected_at: AtomicU64::new(0),

            messages_received: AtomicU64::new(0),

            messages_sent: AtomicU64::new(0),
        }
    }

    pub fn set_connected(&self, ip_address: String) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time is before unix epoch")
            .as_millis() as u64;
        self.connected
            .store(true, std::sync::atomic::Ordering::Release);
        self.connected_at
            .store(now, std::sync::atomic::Ordering::Release);
        self.disconnected_at
            .store(0, std::sync::atomic::Ordering::Release);
        if let Ok(mut guard) = self.ip_address.write() {
            *guard = Some(ip_address);
        } else {
            warn!("failed to update ip_address: poisoned lock");
        }
    }

    pub fn set_disconnected(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time is before unix epoch")
            .as_millis() as u64;
        self.connected
            .store(false, std::sync::atomic::Ordering::Release);
        self.connected_at
            .store(0, std::sync::atomic::Ordering::Release);
        self.disconnected_at
            .store(now, std::sync::atomic::Ordering::Release);
        if let Ok(mut guard) = self.ip_address.write() {
            *guard = None;
        } else {
            warn!("failed to clear ip_address: poisoned lock");
        }
    }

    pub fn increase_messages_received(&self) {
        self.messages_received
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increase_messages_sent(&self) {
        self.messages_sent
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct Client {
    pub tenant_id: String,

    pub client_identifier: String,

    pub properties: ClientProperties,

    pub socket_addr: std::net::SocketAddr,
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
        .expect("system time is before unix epoch");
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

    pub created_at: u64,

    pub connected_at: Option<u64>,

    pub disconnected_at: Option<u64>,

    pub messages_received: u64,

    pub messages_sent: u64,

    pub connected: bool,

    pub ip_address: Option<String>,
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

    #[error("delivery error: {0}")]
    DeliveryError(String),
}

#[derive(Message)]
#[rtype(result = "SessionInfo")]
pub struct GetSessionInfo {}

#[derive(Message)]
#[rtype(result = "()")]
struct AllInflightRetryImmediate {}

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
#[rtype(result = "Result<(), SessionActorError>")]
pub struct AcceptRoutedPublish {
    pub packet: MqttPacketV3,
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

    inflight_retry_interval: u64,

    state: Arc<RwLock<SessionState>>,

    session_lifecycle_tx: mpsc::Sender<SessionLifecycleMessage>,

    session_state_raft_actor: Addr<SessionStateRaftActor>,

    topic_raft_actor: Addr<topic_raft_actor::TopicRaftActor>,

    router_actors: Vec<Addr<RouterActor>>,

    payload_store: Option<Arc<dyn PayloadStore>>,

    timer_actor: Addr<TimerActor>,

    metric: Arc<Metric>,

    session_version: SessionVersion,

    session_metrics: Arc<SessionMetrics>,
}

impl Actor for SessionActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.start_inflight_and_keep_alive_timer(ctx);

        let state = self.state.clone();
        let tenant_id = self.tenant_id.clone();
        let client_id = self.client_id.clone();
        let session_actor_addr = ctx.address();
        let session_lifecycle_tx = self.session_lifecycle_tx.clone();

        let self_addr = ctx.address();
        let topic_raft_actor = self.topic_raft_actor.clone();
        let session_state_raft_actor = self.session_state_raft_actor.clone();
        let payload_store = self.payload_store.clone();
        let clean_session = self.clean_session;

        async move {
            if let Err(e) = session_lifecycle_tx.send(SessionLifecycleMessage::SessionStarted).await {
                error!("send session started message to session manager error: {}, force stop the current session actor", e);
                self_addr.do_send(SessionActorMessage::ForceStop);
                return;
            }
            let subscriptions: Vec<(String, QoS)> = {
                let session_state_guard = state.read().await;
                session_state_guard.subscriptions.iter()
                    .map(|(topic, qos)| (topic.clone(), qos.clone()))
                    .collect()
            };
            // Only recover subscriptions if NOT clean session
            if !clean_session {
                // send update session state message to raft actor
                let _ = session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::UpdateSessionConnectionState {
                        tenant_id: tenant_id.clone(),
                        client_id: client_id.clone(),
                        disconnected_at: None,
                    }
                ).await;
                //

                for (topic, qos) in subscriptions {
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
            }

            //
            if !clean_session {
                loop {
                    match session_state_raft_actor.send(
                        crate::raft::session_state::session_state_raft_actor::PopOfflineMessage {
                            tenant_id: tenant_id.clone(),
                            client_id: client_id.clone(),
                        }
                    ).await {
                        Ok(Ok((Some(key), payload_opt))) => {
                            let data = if let Some(payload) = payload_opt {
                                 Some(bytes::Bytes::from(payload))
                            } else if let Some(store) = &payload_store {
                                match store.get(&key).await {
                                    Ok(res) => res,
                                    Err(e) => {
                                        error!("Store error during recovery: {}", e);
                                        None
                                    }
                                }
                            } else {
                                None
                            };

                            if let Some(data) = data {
                                 if let Ok(packet) = serde_json::from_slice::<MqttPacketV3>(&data) {
                                      session_actor_addr.do_send(SessionActorMessage::OutboundMessage(packet));
                                 }
                            } else {
                                 warn!("Payload missing during recovery for key: {}", key);
                            }
                        },
                        Ok(Ok((None, _))) => {
                            info!("recovery from pending messages finished");
                            break
                        },
                        Ok(Err(e)) => {
                            warn!("session state raft client pop from pending queue error, {}", e);
                            break
                        },
                        Err(e) => {
                            error!("SessionStateRaftActor unavailable when pop pending message during recovery, {}", e);
                            break
                        }
                    }

                }
            }
            //

            self_addr.do_send(AllInflightRetryImmediate {});
        }
            .into_actor(self)
            .wait(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        self.stop_inflight_and_keep_alive_timer();
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

#[derive(Debug, Error)]
enum HandlePublishError {
    #[error("Packet ID does not exist")]
    PacketIdNotExist,

    #[error("Payload store error: {0}")]
    PayloadStoreError(#[from] PayloadError),

    #[error("Route error: {0}")]
    RouteError(String),
}

struct HandlePublishResult {
    inflight_packet: Option<MqttPacketV3>,
}

struct HandlePublishContext {
    client_info: Client,
    plugin_manager: Arc<PluginManager>,
    session_state: Arc<RwLock<SessionState>>,
    clean_session: bool,
    router_actors: Vec<Addr<RouterActor>>,
    session_state_raft_actor: Addr<SessionStateRaftActor>,
    topic_raft_actor: Addr<topic_raft_actor::TopicRaftActor>,
    payload_store: Option<Arc<dyn PayloadStore>>,
}

struct OutboundPublishDeliveryContext {
    tenant_id: String,
    client_id: String,
    clean_session: bool,
    activity_state: ActivityState,
    session_state: Arc<RwLock<SessionState>>,
    session_state_raft_actor: Addr<SessionStateRaftActor>,
    payload_store: Option<Arc<dyn PayloadStore>>,
    conn: Option<Recipient<ConnectionActorMessage>>,
}

async fn deliver_outbound_publish(
    mut publish_packet: PublishPacket,
    context: OutboundPublishDeliveryContext,
) -> Result<(), SessionActorError> {
    let qos = publish_packet.fix_header.qos.unwrap_or(0) as u8;

    if matches!(context.activity_state, ActivityState::Active) {
        let conn = context.conn.ok_or(SessionActorError::ConnectionNotSet)?;
        if qos == 0 {
            conn.send(ConnectionActorMessage::WritePacketToClient(
                MqttPacketV3::Publish(publish_packet),
            ))
            .await?
            .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
            return Ok(());
        }

        let packet_id = publish_packet
            .variable_header
            .packet_identifier
            .ok_or_else(|| {
                SessionActorError::DeliveryError(
                    "QoS > 0 publish packet missing packet identifier".to_string(),
                )
            })?;

        let store = context.payload_store.as_ref().ok_or_else(|| {
            SessionActorError::DeliveryError("payload store missing".to_string())
        })?;
        let key = uuid::Uuid::new_v4().to_string();
        let data = serde_json::to_vec(&MqttPacketV3::Publish(publish_packet.clone()))
            .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
        store
            .put(&key, bytes::Bytes::from(data))
            .await
            .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;

        if context.clean_session {
            let mut session_state_guard = context.session_state.write().await;
            if let Err(e) = session_state_guard
                .inflight
                .register_with_tx_packet(packet_id, qos, key.clone())
            {
                if matches!(e, InflightError::PacketIdentifierHasExisted) {
                    if let Some(new_id) = session_state_guard.inflight.allocate_packet_id() {
                        publish_packet.variable_header.packet_identifier = Some(new_id);
                        session_state_guard
                            .inflight
                            .register_with_tx_packet(new_id, qos, key.clone())
                            .map_err(|inner| {
                                SessionActorError::DeliveryError(inner.to_string())
                            })?;
                    } else {
                        return Err(SessionActorError::DeliveryError(
                            "failed to allocate new inflight packet id".to_string(),
                        ));
                    }
                } else {
                    return Err(SessionActorError::DeliveryError(e.to_string()));
                }
            }
        } else {
            match context
                .session_state_raft_actor
                .send(RegisterInflightTxPacket {
                    tenant_id: context.tenant_id.clone(),
                    client_id: context.client_id.clone(),
                    packet_id,
                    qos,
                    packet_key: key.clone(),
                })
                .await?
            {
                Ok(()) => {}
                Err(e) => {
                    if let crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::InflightError(InflightError::PacketIdentifierHasExisted) = e {
                        let new_id = {
                            let mut session_state_guard = context.session_state.write().await;
                            session_state_guard.inflight.allocate_packet_id()
                        };
                        if let Some(new_id) = new_id {
                            publish_packet.variable_header.packet_identifier = Some(new_id);
                            context
                                .session_state_raft_actor
                                .send(RegisterInflightTxPacket {
                                    tenant_id: context.tenant_id.clone(),
                                    client_id: context.client_id.clone(),
                                    packet_id: new_id,
                                    qos,
                                    packet_key: key.clone(),
                                })
                                .await?
                                .map_err(|inner| {
                                    SessionActorError::DeliveryError(inner.to_string())
                                })?;
                        } else {
                            return Err(SessionActorError::DeliveryError(
                                "failed to allocate new inflight packet id".to_string(),
                            ));
                        }
                    } else {
                        return Err(SessionActorError::DeliveryError(e.to_string()));
                    }
                }
            }
        }

        conn.send(ConnectionActorMessage::WritePacketToClient(
            MqttPacketV3::Publish(publish_packet),
        ))
        .await?
        .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;

        return Ok(());
    }

    if qos == 0 {
        return Ok(());
    }

    let store = context
        .payload_store
        .as_ref()
        .ok_or_else(|| SessionActorError::DeliveryError("payload store missing".to_string()))?;
    let key = uuid::Uuid::new_v4().to_string();
    let data = serde_json::to_vec(&MqttPacketV3::Publish(publish_packet.clone()))
        .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
    store
        .put(&key, bytes::Bytes::from(data))
        .await
        .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;

    if !context.clean_session {
        context
            .session_state_raft_actor
            .send(crate::raft::session_state::session_state_raft_actor::StoreOfflineMessage {
                tenant_id: context.tenant_id.clone(),
                client_id: context.client_id.clone(),
                packet_key: key.clone(),
            })
            .await?
            .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;

        let mut session_state_guard = context.session_state.write().await;
        session_state_guard.pending_messages.push(key);
    } else {
        store
            .delete(&key)
            .await
            .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
    }

    Ok(())
}

async fn do_handle_unsubscribe(
    unsubscribe_packet: UnsubscribePacket,
    client_info: &Client,
    topic_raft_actor_addr: Addr<TopicRaftActor>,
) -> HandleUnSubscribeResult {
    let unsub_topic_filters = &unsubscribe_packet.payload.topic_filters;
    let mut succeed_unsubscriptions = vec![];
    {
        for topic in unsub_topic_filters {
            let tenant_id = client_info.tenant_id.clone();
            let client_id = client_info.client_identifier.clone();
            match topic_raft_actor_addr
                .send(topic_raft_actor::Unsubscribe {
                    tenant_id: tenant_id.clone(),
                    client_identifier: client_id.clone(),
                    topic: topic.topic_name.clone(),
                })
                .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    error!(
                        "session {} unsubscribe topic {} error: {}",
                        client_info.client_identifier, topic.topic_name, e
                    );
                }
                Err(e) => {
                    error!(
                            "TopicRaftActor unavailable when session {} unsubscribe topic {}, error: {}",
                            client_info.client_identifier, topic.topic_name, e
                        );
                }
            }
            // warning: even if the unsubscription fails in raft layer,
            // we still return success to client and remove the subscription in memory,
            // this may cause inconsistency between session state and topic state in raft,
            // but mqtt 3.1.1 protocol does not specify the behavior when unsubscription fails,
            // and this can avoid client being stuck in retry loop when raft layer is unavailable
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
    context: HandlePublishContext,
) -> Result<HandlePublishResult, HandlePublishError> {
    let authorize_request = AuthorizeRequest {
        tenant_id: context.client_info.tenant_id.clone(),
        client_id: context.client_info.client_identifier.clone(),
        username: context
            .client_info
            .properties
            .username
            .clone()
            .unwrap_or("".to_string()),
        action: AuthAction::Publish.into(),
        topic: publish_packet.variable_header.topic_name.clone(),
        qos: publish_packet.fix_header.qos.unwrap_or(0) as u32,
        context: None,
    };

    let publish_authorize_result = context
        .plugin_manager
        .call_authorize_hook(authorize_request)
        .await;

    if publish_authorize_result.is_err() {
        return Ok(HandlePublishResult {
            inflight_packet: None,
        });
    }

    let publish_authorization = publish_authorize_result
        .unwrap_or(AuthorizeResult {
            authorized: false,
            reason: Some("Authorization failed".to_string()),
            modified_context: HashMap::new(),
        })
        .authorized;

    let mut result = HandlePublishResult {
        inflight_packet: None,
    };

    if publish_authorization {
        let message_publish_request = MessagePublishRequest {
            message: Some(MqttMessage {
                tenant_id: context.client_info.tenant_id.clone(),
                client_id: context.client_info.client_identifier.clone(),
                topic: publish_packet.variable_header.topic_name.clone(),
                payload: publish_packet.payload.payload.to_vec(),
                qos: publish_packet.fix_header.qos.unwrap_or(0) as u32,
                retain: publish_packet.fix_header.retain.unwrap_or(false),
                dup: publish_packet.fix_header.dup.unwrap_or(0) == 1,
                publish_time: None,
                properties: None,
                message_id: None,
            }),
            context: None,
        };

        context
            .plugin_manager
            .call_message_published_hook(message_publish_request)
            .await;

        let mut session_state_guard = context.session_state.write().await;

        if publish_packet.fix_header.qos > Some(0) {
            debug!(
                "client {} start process packet {:?} ",
                context.client_info.client_identifier,
                publish_packet.clone()
            );

            let packet_id = match publish_packet.variable_header.packet_identifier {
                Some(id) => id,
                None => {
                    warn!("Packet ID is required for QoS > 0");
                    return Err(HandlePublishError::PacketIdNotExist);
                }
            };
            let qos = publish_packet.fix_header.qos.unwrap_or(0) as u8;

            let key = if let Some(store) = &context.payload_store {
                let k = uuid::Uuid::new_v4().to_string();
                let data = serde_json::to_vec(&MqttPacketV3::Publish(publish_packet.clone()))
                    .map_err(|e| {
                        HandlePublishError::PayloadStoreError(PayloadError::Serialization(
                            e.to_string(),
                        ))
                    })?;
                store.put(&k, bytes::Bytes::from(data)).await?;
                k
            } else {
                warn!("Payload store not available in do_handle_publish");
                return Err(HandlePublishError::PayloadStoreError(
                    PayloadError::Storage("Payload store not available".to_string()),
                ));
            };

            session_state_guard
                .inflight
                .register_with_rx_packet(packet_id, qos, key.clone());

            // if not clean session, should sync inflight rx packet to raft
            if !context.clean_session {
                match context.session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::RegisterInflightRxPacket {
                        tenant_id: context.client_info.tenant_id.clone(),
                        client_id: context.client_info.client_identifier.clone(),
                        packet_id,
                        qos,
                        packet_key: key.clone(),
                    },
                ).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        warn!("inflight register rx packet error, {}", e);
                        return Ok(HandlePublishResult {
                            inflight_packet: None,
                        });
                    }
                    Err(e) => {
                        error!("SessionStateRaftActor unavailable when register inflight rx packet, {}", e);
                        return Ok(HandlePublishResult {
                            inflight_packet: None,
                        });
                    }
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
                match context
                    .topic_raft_actor
                    .send(topic_raft_actor::CleanRetainPublishPacket {
                        tenant_id: context.client_info.tenant_id.clone(),
                        topic_filter: publish_packet.variable_header.topic_name.clone(),
                    })
                    .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        error!("clean retain publish packet error: {}", e);
                    }
                    Err(e) => {
                        error!(
                            "Failed to send clean retain publish packet to topic raft actor: {}",
                            e
                        );
                    }
                }
            } else {
                match context
                    .topic_raft_actor
                    .send(topic_raft_actor::RegisterRetainPublishPacket {
                        tenant_id: context.client_info.tenant_id.clone(),
                        client_id: context.client_info.client_identifier.clone(),
                        publish_packet: MqttPacketV3::Publish(publish_packet.clone()),
                    })
                    .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        error!("register retain publish packet error: {}", e);
                    }
                    Err(e) => {
                        error!(
                            "Failed to send register retain publish packet to topic raft actor: {}",
                            e
                        );
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
        let router_actor = &context.router_actors[hash as usize % context.router_actors.len()];

        router_actor
            .send(crate::router_actor::RoutePacket {
                tenant_id: context.client_info.tenant_id.clone(),
                packet: MqttPacketV3::Publish(publish_packet.clone()),
            })
            .await
            .map_err(|e| HandlePublishError::RouteError(e.to_string()))?
            .map_err(|e| HandlePublishError::RouteError(e.to_string()))?;
    }

    Ok(result)
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
            username: client_info
                .properties
                .username
                .clone()
                .unwrap_or("".to_string()),
            action: AuthAction::Subscribe.into(),
            topic: topic.topic_name.clone(),
            qos: topic.qos.into(),
            context: None,
        };

        let topic_authorizate_result = plugin_manager
            .call_authorize_hook(authorize_request)
            .await
            .unwrap_or(AuthorizeResult {
                authorized: false,
                reason: Some("Authorization failed".to_string()),
                modified_context: HashMap::new(),
            }); // if plugin call fails, treat it as unauthorized

        if topic_authorizate_result.authorized {
            let topic_raft_actor_addr = topic_raft_actor::TopicRaftActor::from_registry();
            match topic_raft_actor_addr
                .send(topic_raft_actor::Subscribe {
                    tenant_id: tenant_id.clone(),
                    client_identifier: client_id.clone(),
                    topic: topic.topic_name.clone(),
                    qos: topic.qos,
                })
                .await
            {
                Ok(Ok(_)) => {
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

                    match topic_raft_actor_addr
                        .send(topic_raft_actor::GetRetainPublishPacket {
                            tenant_id: tenant_id.clone(),
                            topic: topic.topic_name.clone(),
                        })
                        .await
                    {
                        Ok(Ok(mut packets)) => {
                            retain_messages.append(&mut packets);
                        }
                        Ok(Err(e)) => {
                            warn!(
                                    "Failed to get retain publish packet from topic raft actor for session {} subscribe topic {}, error: {}",
                                    client_id, topic.topic_name, e
                                );
                        }
                        Err(e) => {
                            error!("TopicRaftActor unavailable when session {} get retain messages topic {}, error: {}", client_id, topic.topic_name, e);
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!(
                        "session {} subscribe topic {} error: {}",
                        client_id, topic.topic_name, e
                    );
                    return_code.push(yedmq_mqtt::v3::suback::ReturnCode::Failure);
                }
                Err(e) => {
                    warn!(
                        "TopicRaftActor unavailable when session {} subscribe topic {}, error: {}",
                        client_id, topic.topic_name, e
                    );
                    return_code.push(yedmq_mqtt::v3::suback::ReturnCode::Failure);
                }
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

pub struct SessionActorConfig {
    pub tenant_id: String,
    pub client_id: String,
    pub clean_session: bool,
    pub plugin_manager: Arc<PluginManager>,
    pub inflight_retry_duration_secs: u64,
    pub will_message: Option<WillMessage>,
    pub keep_alive: u64,
    pub connection_actor_addr: Recipient<ConnectionActorMessage>,
    pub peer_addr: SocketAddr,
    pub session_state: Arc<RwLock<SessionState>>,
    pub session_lifecycle_tx: Sender<SessionLifecycleMessage>,
    pub session_state_raft_actor: Addr<SessionStateRaftActor>,
    pub topic_raft_actor: Addr<topic_raft_actor::TopicRaftActor>,
    pub router_actors: Vec<Addr<RouterActor>>,
    pub payload_store: Option<Arc<dyn PayloadStore>>,
    pub timer_actor: Addr<TimerActor>,
    pub metric: Arc<Metric>,
    pub session_version: SessionVersion,
}

impl SessionActor {
    pub fn new(config: SessionActorConfig) -> Self {
        let session_metrics = SessionMetrics::new();
        session_metrics.set_connected(config.peer_addr.ip().to_string());

        SessionActor {
            plugin_manager: config.plugin_manager,
            conn_recipient: Some(config.connection_actor_addr),
            conn_addr: Some(config.peer_addr),
            activity_state: ActivityState::Active,
            clean_session: config.clean_session,
            keep_alive: config.keep_alive,
            keep_alive_expired: true,
            inflight_retry_interval: config.inflight_retry_duration_secs,
            tenant_id: config.tenant_id,
            client_id: config.client_id,
            will_message: config.will_message,
            username: None,
            state: config.session_state,
            session_lifecycle_tx: config.session_lifecycle_tx,
            session_state_raft_actor: config.session_state_raft_actor,
            topic_raft_actor: config.topic_raft_actor,
            router_actors: config.router_actors,
            payload_store: config.payload_store,
            timer_actor: config.timer_actor,
            metric: config.metric,
            session_version: config.session_version,
            session_metrics: Arc::new(session_metrics),
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
            socket_addr: self
                .conn_addr
                .expect("session connection address should existed"),
        }
    }

    fn register_inflight_retry_timer(&self, ctx: &mut <SessionActor as Actor>::Context) {
        let inflight_retry_duration = Duration::from_secs(self.inflight_retry_interval);

        self.timer_actor.do_send(RegisterInflight {
            tenant_id: self.tenant_id.clone(),
            session_id: self.client_id.clone(),
            inflight_retry_duration,
            addr: ctx.address().recipient(),
        });
    }

    fn start_inflight_and_keep_alive_timer(&self, ctx: &mut <SessionActor as Actor>::Context) {
        let keep_alive_duration = Duration::from_secs((self.keep_alive as f64 * 1.5) as u64);

        self.timer_actor.do_send(RegisterKeepAlive {
            tenant_id: self.tenant_id.clone(),
            session_id: self.client_id.clone(),
            keep_alive: keep_alive_duration,
            addr: ctx.address().recipient(),
        });

        self.register_inflight_retry_timer(ctx);
    }

    fn stop_inflight_and_keep_alive_timer(&self) {
        self.timer_actor.do_send(RemoveTimer {
            tenant_id: self.tenant_id.clone(),
            session_id: self.client_id.clone(),
            timer_type: TimerType::Inflight,
        });
        self.timer_actor.do_send(RemoveTimer {
            tenant_id: self.tenant_id.clone(),
            session_id: self.client_id.clone(),
            timer_type: TimerType::KeepAlive,
        });
    }

    fn set_state(&mut self, ctx: &mut <SessionActor as Actor>::Context, state: ActivityState) {
        info!("set session {} state to {:?}", self.client_id, state);
        let session_state_raft_actor = self.session_state_raft_actor.clone();
        let tenant_id = self.tenant_id.clone();
        let client_id = self.client_id.clone();
        let clean_session = self.clean_session;

        match state {
            ActivityState::Inactive => {
                self.stop_inflight_and_keep_alive_timer();
                if !clean_session {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("system time is before unix epoch")
                        .as_secs();
                    ctx.spawn(async move {
                        let _ = session_state_raft_actor.send(
                            crate::raft::session_state::session_state_raft_actor::UpdateSessionConnectionState {
                                tenant_id,
                                client_id,
                                disconnected_at: Some(now),
                            }
                        ).await;
                    }.into_actor(self));
                }
            }
            ActivityState::Active => {
                if !clean_session {
                    ctx.spawn(async move {
                        let _ = session_state_raft_actor.send(
                            crate::raft::session_state::session_state_raft_actor::UpdateSessionConnectionState {
                                tenant_id,
                                client_id,
                                disconnected_at: None,
                            }
                        ).await;
                    }.into_actor(self));
                }
            }
        }
        self.activity_state = state;
        let session_lifecycle_tx = self.session_lifecycle_tx.clone();
        async move {
            match session_lifecycle_tx
                .send(SessionLifecycleMessage::SessionDeactivate)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "send session deactivate message to session manager actor error: {}",
                        e
                    );
                }
            }
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn force_stop(&mut self, ctx: &mut <SessionActor as Actor>::Context) {
        self.stop_inflight_and_keep_alive_timer();
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
        let version = self.session_version.clone();
        async move {
            if let Err(e) = session_lifecycle_tx
                .send(SessionLifecycleMessage::SessionStopped {
                    tenant_id,
                    client_id,
                    version,
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
        self.metric.increase_messages_received();
        self.session_metrics.increase_messages_received();
        let context = HandlePublishContext {
            client_info: self.get_plugin_client_info(),
            plugin_manager: self.plugin_manager.clone(),
            session_state: self.state.clone(),
            clean_session: self.clean_session,
            router_actors: self.router_actors.clone(),
            session_state_raft_actor: self.session_state_raft_actor.clone(),
            topic_raft_actor: self.topic_raft_actor.clone(),
            payload_store: self.payload_store.clone(),
        };

        ctx.spawn(
            async move {
                do_handle_publish(publish_packet, context).await
            }
                .into_actor(self)
                .map(|result, act, _ctx| {
                    match result {
                        Ok(res) => {
                            if let Some(packet) = res.inflight_packet {
                                let conn = if let Some(recipient) = &act.conn_recipient {
                                    recipient
                                } else {
                                    return;
                                };
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
                        },
                        Err(e) => {
                            match e {
                                HandlePublishError::PacketIdNotExist => {
                                    // Qos > 0 publish packet without packet id, this should not happen, force stop the session
                                    _ctx.address().do_send(SessionActorMessage::ForceDisconnect);
                                }
                                HandlePublishError::PayloadStoreError(e) => {
                                    error!("Failed to store payload for incoming publish packet: {}", e);
                                }
                                HandlePublishError::RouteError(e) => {
                                    error!("Failed to route incoming publish packet: {}", e);
                                }
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
        let conn = if let Some(recipient) = &self.conn_recipient {
            recipient.clone()
        } else {
            return; // If connection recipient is none, it means the connection is already closed, no need to handle subscribe
        };
        let session_state = self.state.clone();
        let clean_session = self.clean_session;
        let metric = self.metric.clone();

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

            for _ in 0..res.succeed_subscriptions.len() {
                metric.increase_subscriptions_count();
            }

            if !clean_session {
                for (topic, qos) in res.succeed_subscriptions {

                    let qos_v = match qos {
                        QoS::AtMostOnce => 0,
                        QoS::AtLeastOnce => 1,
                        QoS::ExactlyOnce => 2,
                    };
                    let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();
                    match session_state_raft_actor_addr.send(
                        crate::raft::session_state::session_state_raft_actor::SubscribeTopic {
                            tenant_id: tenant_id.clone(),
                            client_id: client_id.clone(),
                            topic: topic.clone(),
                            qos: qos_v,
                        },
                    ).await {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => {
                            warn!(
                                "persistent session raft add subscribe topic {} error, {}",
                                topic.clone(),
                                e
                            );
                        }
                        Err(e) => {
                            warn!(
                                "SessionStateRaftActor unavailable when session add subscribe topic {}, error: {}",
                                topic.clone(),
                                e
                            );
                        }
                    }
                }
            } else {
                for (topic, qos) in res.succeed_subscriptions {
                    session_state
                        .write()
                        .await
                        .subscriptions
                        .insert(topic.clone(), qos);
                }
            }
        }
            .into_actor(self)
            .wait(ctx);
    }

    fn handle_pingreq(&mut self) {
        if let Some(recipient) = &self.conn_recipient {
            recipient.do_send(ConnectionActorMessage::WritePacketToClient(
                yedmq_mqtt::MqttPacketV3::Pingresp(PingrespPacket::new()),
            ));
        }
    }

    fn handle_unsubscribe(
        &mut self,
        unsubscribe_packet: UnsubscribePacket,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        let conn = if let Some(recipient) = &self.conn_recipient {
            recipient.clone()
        } else {
            return; // If connection recipient is none, it means the connection is already closed, no need to handle unsubscribe
        };
        let client_info = self.get_plugin_client_info();
        let session_state = self.state.clone();
        let clean_session = self.clean_session;
        let topic_raft_actor = self.topic_raft_actor.clone();
        let metric = self.metric.clone();
        async move {
            let res = do_handle_unsubscribe(unsubscribe_packet, &client_info, topic_raft_actor.clone()).await;
            for _ in 0..res.succeed_unsubscriptions.len() {
                metric.decrease_subscriptions_count();
            }
            for topic in res.succeed_unsubscriptions {
                {
                    session_state.write().await.subscriptions.remove(&topic);
                }
                if !clean_session {
                    let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();
                    match session_state_raft_actor_addr.send(
                        crate::raft::session_state::session_state_raft_actor::UnsubscribeTopic {
                            tenant_id: client_info.tenant_id.clone(),
                            client_id: client_info.client_identifier.clone(),
                            topic: topic.clone(),
                        },
                    ).await {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => {
                            error!(
                                "persistent session raft remove subscribe topic {} error, {}",
                                topic.clone(),
                                e
                            );
                        }
                        Err(e) => {
                            error!(
                                "SessionStateRaftActor unavailable when session unsubscribe topic {}, error: {}",
                                topic.clone(),
                                e
                            );
                        }
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
        let conn = if let Some(recipient) = &self.conn_recipient {
            recipient.clone()
        } else {
            return; // If connection recipient is none, it means the connection is already closed, no need to handle pubrel
        };
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
                match session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::AdvanceInflightState {
                        tenant_id: client_info.tenant_id.clone(),
                        client_id: client_info.client_identifier.clone(),
                        packet_id,
                    },
                ).await {
                    Ok(Ok(())) => {
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
                    }
                    Ok(Err(e)) => {
                        error!(
                            "handle pubrel raft advance state error {}",
                            e
                        );
                    }
                    Err(e) => {
                        error!(
                            "SessionStateRaftActor unavailable when handling pubrel, error: {}",
                            e
                        );
                    }
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
        let conn = if let Some(recipient) = &self.conn_recipient {
            recipient.clone()
        } else {
            return; // If connection recipient is none, it means the connection is already closed, no need to handle pubrec
        };
        let clean_session = self.clean_session;
        let client_info = self.get_plugin_client_info();
        let session_state_raft_actor = self.session_state_raft_actor.clone();

        async move {
            let packet_id = pubrec_packet.variable_header.packet_identifier;

            // Generate Pubrel command
            let pubrel = MqttPacketV3::Pubrel(PubRelPacket::new(packet_id));

            if let Err(e) = conn
                .send(ConnectionActorMessage::WritePacketToClient(pubrel))
                .await
            {
                warn!("handle pubrec write pubrel to client error: {}", e);
            } else if clean_session {
                let mut session_state_guard = session_state.write().await;
                session_state_guard.inflight.next_state(packet_id);
            } else {
                match session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::AdvanceInflightState {
                        tenant_id: client_info.tenant_id.clone(),
                        client_id: client_info.client_identifier.clone(),
                        packet_id,
                    },
                ).await {
                    Ok(Ok(())) => {
                        let mut session_state_guard = session_state.write().await;
                        session_state_guard.inflight.next_state(packet_id);
                    }
                    Ok(Err(e)) => {
                        warn!(
                            "handle pubrec raft advance state error {}",
                            e
                        );
                    }
                    Err(e) => {
                        warn!(
                            "SessionStateRaftActor unvailable when handling pubrec, error: {}",
                            e
                        );
                    }
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
            let packet_id = puback_packet.variable_header.packet_identifier;

            if clean_session {
                let mut session_state_guard = session_state.write().await;
                session_state_guard.inflight.next_state(packet_id);
                let state = session_state.clone();
                let store = payload_store.clone();
                actix::spawn(async move {
                    Self::cleanup_local_finished_items(state, store).await;
                });
            } else {
                match session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::AdvanceInflightState {
                        tenant_id: client_info.tenant_id.clone(),
                        client_id: client_info.client_identifier.clone(),
                        packet_id,
                    },
                ).await {
                    Ok(Ok(())) => {
                        {
                            let mut session_state_guard = session_state.write().await;
                            session_state_guard.inflight.next_state(packet_id);
                        }

                        match session_state_raft_actor.send(
                            crate::raft::session_state::session_state_raft_actor::InflightCleanFinishedItems {
                                tenant_id: client_info.tenant_id.clone(),
                                client_id: client_info.client_identifier.clone(),
                            },
                        ).await {
                            Ok(Ok(())) => {
                                let mut session_state_guard = session_state.write().await;
                                session_state_guard.inflight.clean_finished_items();
                            }
                            Ok(Err(e)) => {
                                warn!(
                                    "handle puback raft clean finished items error {}",
                                    e
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "SessionStateRaftActor unvailable when handling puback clean finished items, error: {}",
                                    e
                                );
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(
                            "handle puback raft advance state error {}",
                            e
                        );
                    }
                    Err(e) => {
                        warn!(
                            "SessionStateRaftActor unvailable when handling puback, error: {}",
                            e
                        );
                    }
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
            let packet_id = pubcomp_packet.variable_header.packet_identifier;


            if clean_session {
                let mut session_state_guard = session_state.write().await;
                session_state_guard.inflight.next_state(packet_id);

                let state = session_state.clone();

                let store = payload_store.clone();

                actix::spawn(async move {
                    Self::cleanup_local_finished_items(state, store).await;
                });
            } else {

                match session_state_raft_actor.send(
                    crate::raft::session_state::session_state_raft_actor::AdvanceInflightState {
                        tenant_id: client_info.tenant_id.clone(),

                        client_id: client_info.client_identifier.clone(),

                        packet_id,

                    },
                ).await {
                    Ok(Ok(())) => {
                        {
                            let mut session_state_guard = session_state.write().await;
                            session_state_guard.inflight.next_state(packet_id);
                        }


                        match session_state_raft_actor.send(
                            crate::raft::session_state::session_state_raft_actor::InflightCleanFinishedItems {
                                tenant_id: client_info.tenant_id.clone(),

                                client_id: client_info.client_identifier.clone(),

                            },
                        ).await {
                            Ok(Ok(())) => {
                                let mut session_state_guard = session_state.write().await;
                                session_state_guard.inflight.clean_finished_items();
                            }
                            Ok(Err(e)) => {
                                warn!(
                                    "handle pubcomp raft clean finished items error {}",
                                    e
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "SessionStateRaftActor unvailable when handling pubcomp clean finished items, error: {}",
                                    e
                                );
                            }
                        }

                    }
                    Ok(Err(e)) => {
                        warn!(
                            "handle pubcomp raft advance state error {}",
                            e
                        );
                    }
                    Err(e) => {
                        warn!(
                            "SessionStateRaftActor unvailable when handling pubcomp, error: {}",
                            e
                        );
                    }
                };
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

        if let Some(recipient) = &self.conn_recipient {
            recipient.do_send(ConnectionActorMessage::Disconnect(DisconnectReason::Normal));
        }

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
            plugin_manager
                .call_client_disconnected_hook(client_disconnected_event)
                .await;
        }
        .into_actor(self)
        .wait(ctx);

        if !self.clean_session {
            self.set_state(ctx, ActivityState::Inactive);
            self.session_metrics.set_disconnected();
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

    fn send_will_message(
        &mut self,
        ctx: &mut <SessionActor as Actor>::Context,
        callback_fn: ThenCallback<SessionActor, ()>,
    ) {
        let tenant_id = self.tenant_id.clone();
        let will_message = self.will_message.take();
        let router_actors = self.router_actors.clone();
        async move {
            if let Some(will_message) = will_message {
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
                router_actor.do_send(crate::router_actor::RoutePacket {
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
                self.timer_actor.do_send(RefreshTimer {
                    tenant_id: self.tenant_id.clone(),
                    session_id: self.client_id.clone(),
                    timer_type: TimerType::KeepAlive,
                });
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
                if let MqttPacketV3::Publish(publish_packet) = packet {
                    self.metric.increase_messages_sent();
                    self.session_metrics.increase_messages_sent();
                    let delivery_context = OutboundPublishDeliveryContext {
                        tenant_id: self.tenant_id.clone(),
                        client_id: self.client_id.clone(),
                        clean_session: self.clean_session,
                        activity_state: self.activity_state,
                        session_state: self.state.clone(),
                        session_state_raft_actor: self.session_state_raft_actor.clone(),
                        payload_store: self.payload_store.clone(),
                        conn: self.conn_recipient.clone(),
                    };

                    ctx.spawn(
                        async move { deliver_outbound_publish(publish_packet, delivery_context).await }
                            .into_actor(self)
                            .map(|result, act, _| {
                                if let Err(e) = result {
                                    error!(
                                        "failed to deliver outbound publish for session {}: {}",
                                        act.client_id, e
                                    );
                                }
                            }),
                    );
                }
            }
            SessionActorMessage::KeepAliveExpired => {
                // send will message and clean up
                self.send_will_message(ctx, |_, actor, ctx| {
                    if let Some(recipient) = &actor.conn_recipient {
                        info!(
                            "keep alive expired, stop session {} connection",
                            actor.client_id
                        );
                        recipient.do_send(ConnectionActorMessage::Disconnect(
                            DisconnectReason::KeepAliveExpired,
                        ));
                        if !actor.clean_session {
                            actor.set_state(ctx, ActivityState::Inactive);
                            actor.session_metrics.set_disconnected();
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
                self.register_inflight_retry_timer(ctx);
                ctx.spawn(
                    async move {
                        let mut session_state_guard = session_state.write().await;
                        let packets = session_state_guard
                            .inflight
                            .get_all_expired_packet_keys_and_refresh_expired_time();

                        if let Some(store) = &payload_store {
                            for (packet_id, key) in packets {
                                let state = session_state_guard
                                    .inflight
                                    .get_inflight_current_state(packet_id);

                                match state {
                                    Some(InflightState::WaitPubcomp) => {
                                        let packet =
                                            MqttPacketV3::Pubrel(PubRelPacket::new(packet_id));
                                        session_actor_addr
                                            .do_send(SessionActorMessage::OutboundMessage(packet));
                                    }
                                    Some(InflightState::WaitPubrel) => {
                                        let packet =
                                            MqttPacketV3::Pubrec(PubRecPacket::new(packet_id));
                                        session_actor_addr
                                            .do_send(SessionActorMessage::OutboundMessage(packet));
                                    }
                                    Some(InflightState::WaitPubrec)
                                    | Some(InflightState::WaitPuback) => {
                                        if let Ok(Some(data)) = store.get(&key).await {
                                            if let Ok(mut packet) =
                                                serde_json::from_slice::<MqttPacketV3>(&data)
                                            {
                                                packet.set_dup(1);
                                                session_actor_addr.do_send(
                                                    SessionActorMessage::OutboundMessage(packet),
                                                );
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    .into_actor(self),
                );
            }
            SessionActorMessage::ForceDisconnect => {
                if matches!(self.activity_state, ActivityState::Active) {
                    //If the session is active, it means the connection is still alive, we can send disconnect message to connection actor to trigger will message and proper clean up
                    let conn = if let Some(recipient) = &self.conn_recipient {
                        recipient.clone()
                    } else {
                        return;
                    };
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
                                    act.session_metrics.set_disconnected();
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
                        actor.session_metrics.set_disconnected();
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
                socket_addr,
            } => {
                self.set_state(ctx, ActivityState::Active);
                self.conn_recipient = Some(conn);
                self.will_message = will_message;
                self.clean_session = clean_session;
                self.keep_alive = keep_alive;
                self.username = username;
                self.stop_inflight_and_keep_alive_timer();
                self.start_inflight_and_keep_alive_timer(ctx);

                self.session_metrics.set_connected(socket_addr.to_string());

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
                                    if let Ok(packet) =
                                        serde_json::from_slice::<MqttPacketV3>(&data)
                                    {
                                        session_actor_addr
                                            .do_send(SessionActorMessage::OutboundMessage(packet));
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
                        match topic_raft_actor
                            .send(crate::raft::topic::topic_raft_actor::Subscribe {
                                tenant_id: tenant_id.clone(),
                                client_identifier: client_identifier.clone(),
                                topic: topic.clone(),
                                qos: qos_v,
                            })
                            .await {
                                Ok(Ok(())) => {}
                                Ok(Err(e)) => {
                                    warn!("handle subscribe topic error for session {}, topic {}, error: {}", client_identifier, topic, e);
                                }
                                Err(e) => {
                                    warn!("TopicRaftActor unavailable when subscribe topic for session {}, topic {}, error: {}", client_identifier, topic, e);
                                }
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
                    self.session_metrics.set_disconnected();
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
        let session_metric = self.session_metrics.clone();

        let future = async move {
            SessionInfo {
                tenant_identifier: tenant_id,
                client_identifier: client_id,
                subscription_topics: session_state
                    .read()
                    .await
                    .subscriptions
                    .keys()
                    .cloned()
                    .collect(),
                session_state: activity_state,
                created_at: session_metric.created_at,
                connected_at: session_metric.get_connected_at(),
                disconnected_at: session_metric.get_disconnected_at(),
                messages_received: session_metric
                    .messages_received
                    .load(std::sync::atomic::Ordering::Relaxed),
                messages_sent: session_metric
                    .messages_sent
                    .load(std::sync::atomic::Ordering::Relaxed),
                connected: session_metric.get_connected(),
                ip_address: session_metric.get_ipaddress(),
            }
        };

        Box::pin(future)
    }
}

impl Handler<AcceptRoutedPublish> for SessionActor {
    type Result = ResponseActFuture<Self, Result<(), SessionActorError>>;

    fn handle(&mut self, msg: AcceptRoutedPublish, _ctx: &mut Self::Context) -> Self::Result {
        let delivery_context = OutboundPublishDeliveryContext {
            tenant_id: self.tenant_id.clone(),
            client_id: self.client_id.clone(),
            clean_session: self.clean_session,
            activity_state: self.activity_state,
            session_state: self.state.clone(),
            session_state_raft_actor: self.session_state_raft_actor.clone(),
            payload_store: self.payload_store.clone(),
            conn: self.conn_recipient.clone(),
        };

        Box::pin(
            async move {
                match msg.packet {
                    MqttPacketV3::Publish(publish_packet) => {
                        deliver_outbound_publish(publish_packet, delivery_context).await
                    }
                    other => Err(SessionActorError::DeliveryError(format!(
                        "unsupported routed packet: {:?}",
                        other
                    ))),
                }
            }
            .into_actor(self),
        )
    }
}

impl Handler<AllInflightRetryImmediate> for SessionActor {
    type Result = ();

    fn handle(&mut self, _msg: AllInflightRetryImmediate, ctx: &mut Self::Context) -> Self::Result {
        let session_state = self.state.clone();
        let session_actor_addr = ctx.address();
        let payload_store = self.payload_store.clone();
        ctx.spawn(
            async move {
                let mut session_state_guard = session_state.write().await;
                let packets = session_state_guard
                    .inflight
                    .get_all_packet_keys_and_refresh_expired_time();

                if let Some(store) = &payload_store {
                    for (packet_id, key) in packets {
                        let state = session_state_guard
                            .inflight
                            .get_inflight_current_state(packet_id);

                        match state {
                            Some(InflightState::WaitPubcomp) => {
                                let packet = MqttPacketV3::Pubrel(PubRelPacket::new(packet_id));
                                session_actor_addr
                                    .do_send(SessionActorMessage::OutboundMessage(packet));
                            }
                            Some(InflightState::WaitPubrel) => {
                                let packet = MqttPacketV3::Pubrec(PubRecPacket::new(packet_id));
                                session_actor_addr
                                    .do_send(SessionActorMessage::OutboundMessage(packet));
                            }
                            Some(InflightState::WaitPubrec) | Some(InflightState::WaitPuback) => {
                                if let Ok(Some(data)) = store.get(&key).await {
                                    if let Ok(mut packet) =
                                        serde_json::from_slice::<MqttPacketV3>(&data)
                                    {
                                        packet.set_dup(1);
                                        session_actor_addr
                                            .do_send(SessionActorMessage::OutboundMessage(packet));
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            .into_actor(self),
        );
    }
}
