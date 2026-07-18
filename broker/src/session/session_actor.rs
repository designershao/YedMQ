use actix::{
    dev::{ContextFutureSpawner, MessageResponse},
    fut, Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, MailboxError,
    Message, Recipient, ResponseFuture, WrapFuture,
};
use bytes::Bytes;
use log::{debug, error, info, warn};
use prost_types::Timestamp;
use serde::{Deserialize, Serialize};
use std::{
    cmp,
    collections::{HashMap, HashSet, VecDeque},
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
use yedmq_mqtt::packet::{
    Ack, Packet, Properties, ProtocolVersion, Publish, ReasonCode, RetainHandling, Suback,
    Subscribe, Unsuback, Unsubscribe,
};
use yedmq_plugin_host::{
    plugin_manager::{AuthorizeResult, PluginManager},
    protocol::plugin_protocol::{
        AuthAction, AuthorizeRequest, ClientDisconnectedEvent, MessagePublishRequest, MqttMessage,
        SubscribeRequest, TopicFilter,
    },
};

use crate::{
    inflight::{InflightError, InflightState},
    mqtt_properties::{properties_to_struct, subscribe_context},
    raft::payload::PayloadError,
    router_actor::RouterActor,
    session::session_state_service::SessionStateService,
    stored_packet::{deserialize_stored_packet, serialize_stored_packet},
    topic::{shared_subscription::parse_shared_subscription_filter, topic_service::TopicService},
};

use super::{
    session_actor_map_storage::SessionVersion,
    session_manager_actor::SessionLifecycleMessage,
    session_state_storage::{SessionState, SubscriptionState},
    WillMessage,
};

use crate::connection::{ConnectionActorMessage, DisconnectReason};
use crate::metric::Metric;
use crate::mqtt_message_expiry;
use crate::raft::payload::PayloadStore;
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

fn ack_with_reason(packet_identifier: u16, reason_code: ReasonCode) -> Ack {
    Ack {
        protocol_version: ProtocolVersion::V3_1_1,
        packet_identifier,
        reason_code,
        properties: Properties::default(),
    }
}

fn success_ack(packet_identifier: u16) -> Ack {
    ack_with_reason(packet_identifier, ReasonCode::Success)
}

fn inbound_publish_ack(publish: &Publish, reason_code: ReasonCode) -> Option<Packet> {
    let packet_identifier = publish.packet_identifier?;
    match publish.qos {
        1 => Some(Packet::Puback(ack_with_reason(
            packet_identifier,
            reason_code,
        ))),
        2 => Some(Packet::Pubrec(ack_with_reason(
            packet_identifier,
            reason_code,
        ))),
        _ => None,
    }
}

fn inbound_publish_success_ack(publish: &Publish) -> Option<Packet> {
    inbound_publish_ack(publish, ReasonCode::Success)
}

fn updated_session_expiry_interval(
    current: Option<u32>,
    requested: Option<u32>,
) -> Result<Option<u32>, ReasonCode> {
    if matches!((current, requested), (Some(current), Some(requested)) if requested > current) {
        return Err(ReasonCode::ProtocolError);
    }
    Ok(requested.or(current))
}

fn granted_qos_reason(qos: u8) -> ReasonCode {
    match qos {
        0 => ReasonCode::GrantedQos0,
        1 => ReasonCode::GrantedQos1,
        2 => ReasonCode::GrantedQos2,
        _ => ReasonCode::UnspecifiedError,
    }
}

fn subscribe_precheck_reason(
    _protocol_version: ProtocolVersion,
    topic_filter: &str,
) -> Option<ReasonCode> {
    if topic_filter.starts_with("$share/") {
        match parse_shared_subscription_filter(topic_filter) {
            Ok(Some(_)) => None,
            Ok(None) => None,
            Err(_) => Some(ReasonCode::TopicFilterInvalid),
        }
    } else {
        None
    }
}

fn authorization_failure_reason(authorize_result: &AuthorizeResult) -> Option<ReasonCode> {
    if authorize_result.authorized {
        None
    } else {
        Some(ReasonCode::NotAuthorized)
    }
}

fn publish_topic_hash(topic: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(topic, &mut hasher);
    std::hash::Hasher::finish(&hasher)
}

fn build_will_publish(will_message: &WillMessage, now: u64) -> Publish {
    let mut properties = will_message.properties.clone();
    properties.will_delay_interval = None;

    let mut publish_packet = Publish {
        protocol_version: will_message.protocol_version,
        topic_name: will_message.will_topic.clone(),
        payload: Bytes::copy_from_slice(will_message.will_message.as_slice()),
        qos: will_message.will_qos,
        retain: will_message.will_retain,
        dup: false,
        packet_identifier: None,
        properties,
        expires_at_unix_secs: None,
    };
    mqtt_message_expiry::stamp_publish_expiry(&mut publish_packet, now);
    publish_packet
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum QoS {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

impl QoS {
    pub fn as_u8(&self) -> u8 {
        match self {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }
    }
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
    InboundPacket(Packet),

    OutboundMessage(Packet),

    KeepAliveExpired,

    InflightRetry,

    ForceDisconnect,

    Reconnect {
        conn: Recipient<ConnectionActorMessage>,

        keep_alive: u64,

        clean_session: bool,

        protocol_version: ProtocolVersion,

        session_expiry_interval: Option<u32>,

        receive_maximum: u16,

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
    pub packet: Packet,
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

    protocol_version: ProtocolVersion,

    session_expiry_interval: Option<u32>,

    receive_maximum: u16,

    outbound_receive_slots_in_use: usize,

    deferred_outbound_publishes: VecDeque<Publish>,

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

    session_state_service: SessionStateService,

    topic_service: TopicService,

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
        let topic_service = self.topic_service.clone();
        let session_state_service = self.session_state_service.clone();
        let payload_store = self.payload_store.clone();
        let clean_session = self.clean_session;
        let protocol_version = self.protocol_version;

        async move {
            if let Err(e) = session_lifecycle_tx.send(SessionLifecycleMessage::SessionStarted).await {
                error!("send session started message to session manager error: {}, force stop the current session actor", e);
                self_addr.do_send(SessionActorMessage::ForceStop);
                return;
            }
            let subscriptions: Vec<(String, SubscriptionState)> = {
                let session_state_guard = state.read().await;
                session_state_guard.subscriptions.iter()
                    .map(|(topic, subscription)| (topic.clone(), subscription.clone()))
                    .collect()
            };
            // Only recover subscriptions if NOT clean session
            if !clean_session {
                // send update session state message to raft actor
                let _ = session_state_service
                    .update_session_connection_state(
                        tenant_id.clone(),
                        client_id.clone(),
                        None,
                        None,
                    )
                    .await;
                //

                for (topic, subscription) in subscriptions {
                    info!(
                        "recover subscribe topic: {}, subscription: {:?}",
                        topic, subscription
                    );
                    if let Err(e) = topic_service
                        .subscribe_with_options(
                            tenant_id.clone(),
                            client_id.clone(),
                            topic.clone(),
                            subscription.qos.as_u8(),
                            subscription.no_local,
                            subscription.retain_as_published,
                        )
                        .await
                    {
                        warn!(
                            "persistent session recover subscribe topic {} error, {}",
                            topic.clone(),
                            e
                        );
                    }
                }
            }

            //
            if !clean_session {
                loop {
                    match session_state_service
                        .pop_offline_message(tenant_id.clone(), client_id.clone())
                        .await
                    {
                        Ok((Some(key), payload_opt)) => {
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
                                 if let Ok(mut packet) = deserialize_stored_packet(&data) {
                                      if mqtt_message_expiry::prepare_packet_for_delivery(
                                          &mut packet,
                                          protocol_version,
                                          mqtt_message_expiry::now_unix_secs(),
                                      ) {
                                          session_actor_addr.do_send(SessionActorMessage::OutboundMessage(packet));
                                      }
                                 }
                            } else {
                                 warn!("Payload missing during recovery for key: {}", key);
                            }
                        },
                        Ok((None, _)) => {
                            info!("recovery from pending messages finished");
                            break
                        },
                        Err(e) => {
                            warn!("session state raft client pop from pending queue error, {}", e);
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
    retain_messages: Vec<RetainedPublishDelivery>,

    suback_packet: Suback,

    succeed_subscriptions: Vec<(String, SubscriptionState)>,
}

struct RetainedPublishDelivery {
    packet: Arc<Packet>,
    requested_qos: u8,
    retain_as_published: bool,
    subscriber_protocol: ProtocolVersion,
}

struct HandleUnSubscribeResult {
    unsuback_packet: Unsuback,

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
    inflight_packet: Option<Packet>,
}

struct HandlePublishContext {
    client_info: Client,
    plugin_manager: Arc<PluginManager>,
    session_state: Arc<RwLock<SessionState>>,
    clean_session: bool,
    router_actors: Vec<Addr<RouterActor>>,
    session_state_service: SessionStateService,
    topic_service: TopicService,
    payload_store: Option<Arc<dyn PayloadStore>>,
}

struct OutboundPublishDeliveryContext {
    tenant_id: String,
    client_id: String,
    clean_session: bool,
    protocol_version: ProtocolVersion,
    activity_state: ActivityState,
    session_state: Arc<RwLock<SessionState>>,
    session_state_service: SessionStateService,
    payload_store: Option<Arc<dyn PayloadStore>>,
    conn: Option<Recipient<ConnectionActorMessage>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboundPublishDeliveryStatus {
    Sent,
    NotSent,
}

async fn deliver_outbound_publish(
    mut publish_packet: Publish,
    context: OutboundPublishDeliveryContext,
) -> Result<OutboundPublishDeliveryStatus, SessionActorError> {
    if !mqtt_message_expiry::prepare_publish_for_delivery(
        &mut publish_packet,
        context.protocol_version,
        mqtt_message_expiry::now_unix_secs(),
    ) {
        return Ok(OutboundPublishDeliveryStatus::NotSent);
    }

    let qos = publish_packet.qos;

    if matches!(context.activity_state, ActivityState::Active) {
        let conn = context.conn.ok_or(SessionActorError::ConnectionNotSet)?;
        if qos == 0 {
            conn.send(ConnectionActorMessage::WritePacketToClient(
                Packet::Publish(publish_packet),
            ))
            .await?
            .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
            return Ok(OutboundPublishDeliveryStatus::Sent);
        }

        if publish_packet.packet_identifier.is_none() {
            let packet_id = {
                let mut session_state_guard = context.session_state.write().await;
                session_state_guard.inflight.allocate_packet_id()
            }
            .ok_or_else(|| {
                SessionActorError::DeliveryError(
                    "failed to allocate new inflight packet id".to_string(),
                )
            })?;
            publish_packet.packet_identifier = Some(packet_id);
        }

        let packet_id = publish_packet
            .packet_identifier
            .expect("packet id allocated");

        let store = context
            .payload_store
            .as_ref()
            .ok_or_else(|| SessionActorError::DeliveryError("payload store missing".to_string()))?;
        let key = uuid::Uuid::new_v4().to_string();
        let mut stored_packet = Packet::Publish(publish_packet.clone());
        let mut stored_packet_needs_refresh = false;
        let data = serialize_stored_packet(&stored_packet)
            .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
        store
            .put(&key, bytes::Bytes::from(data))
            .await
            .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;

        if context.clean_session {
            let mut session_state_guard = context.session_state.write().await;
            if let Err(e) =
                session_state_guard
                    .inflight
                    .register_with_tx_packet(packet_id, qos, key.clone())
            {
                if matches!(e, InflightError::PacketIdentifierHasExisted) {
                    if let Some(new_id) = session_state_guard.inflight.allocate_packet_id() {
                        publish_packet.packet_identifier = Some(new_id);
                        stored_packet = Packet::Publish(publish_packet.clone());
                        stored_packet_needs_refresh = true;
                        session_state_guard
                            .inflight
                            .register_with_tx_packet(new_id, qos, key.clone())
                            .map_err(|inner| SessionActorError::DeliveryError(inner.to_string()))?;
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
                .session_state_service
                .register_inflight_tx_packet(
                    context.tenant_id.clone(),
                    context.client_id.clone(),
                    packet_id,
                    qos,
                    key.clone(),
                )
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    if let crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::InflightError(InflightError::PacketIdentifierHasExisted) = e {
                        let new_id = {
                            let mut session_state_guard = context.session_state.write().await;
                            session_state_guard.inflight.allocate_packet_id()
                        };
                        if let Some(new_id) = new_id {
                            publish_packet.packet_identifier = Some(new_id);
                            stored_packet = Packet::Publish(publish_packet.clone());
                            stored_packet_needs_refresh = true;
                            context
                                .session_state_service
                                .register_inflight_tx_packet(
                                    context.tenant_id.clone(),
                                    context.client_id.clone(),
                                    new_id,
                                    qos,
                                    key.clone(),
                                )
                                .await
                                .map_err(|inner| SessionActorError::DeliveryError(inner.to_string()))?;
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

            let packet_id = publish_packet
                .packet_identifier
                .expect("packet id registered in persistent session");
            let mut session_state_guard = context.session_state.write().await;
            if session_state_guard
                .inflight
                .get_inflight_current_state(packet_id)
                .is_none()
            {
                session_state_guard
                    .inflight
                    .register_with_tx_packet(packet_id, qos, key.clone())
                    .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
            }
        }

        if stored_packet_needs_refresh {
            let data = serialize_stored_packet(&stored_packet)
                .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
            store
                .put(&key, bytes::Bytes::from(data))
                .await
                .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
            debug!(
                "session {} outbound publish stored with reallocated packet id",
                context.client_id
            );
        }

        conn.send(ConnectionActorMessage::WritePacketToClient(
            Packet::Publish(publish_packet),
        ))
        .await?
        .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;

        return Ok(OutboundPublishDeliveryStatus::Sent);
    }

    if qos == 0 {
        return Ok(OutboundPublishDeliveryStatus::NotSent);
    }

    let store = context
        .payload_store
        .as_ref()
        .ok_or_else(|| SessionActorError::DeliveryError("payload store missing".to_string()))?;
    let key = uuid::Uuid::new_v4().to_string();
    let data = serialize_stored_packet(&Packet::Publish(publish_packet.clone()))
        .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
    store
        .put(&key, bytes::Bytes::from(data))
        .await
        .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;

    if !context.clean_session {
        context
            .session_state_service
            .store_offline_message(
                context.tenant_id.clone(),
                context.client_id.clone(),
                key.clone(),
            )
            .await
            .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;

        let mut session_state_guard = context.session_state.write().await;
        session_state_guard.pending_messages.push(key);
    } else {
        store
            .delete(&key)
            .await
            .map_err(|e| SessionActorError::DeliveryError(e.to_string()))?;
    }

    Ok(OutboundPublishDeliveryStatus::NotSent)
}

async fn collect_inflight_retry_packets(
    inflight_entries: Vec<(u16, String, InflightState)>,
    payload_store: Option<Arc<dyn PayloadStore>>,
) -> Vec<Packet> {
    let mut retry_packets = Vec::new();
    let now = mqtt_message_expiry::now_unix_secs();

    for (packet_id, key, state) in inflight_entries {
        match state {
            InflightState::WaitPubcomp => {
                retry_packets.push(Packet::Pubrel(success_ack(packet_id)));
            }
            InflightState::WaitPubrel => {
                retry_packets.push(Packet::Pubrec(success_ack(packet_id)));
            }
            InflightState::WaitPubrec | InflightState::WaitPuback => {
                let Some(store) = &payload_store else {
                    warn!(
                        "skip inflight publish retry for packet {} because payload store is missing",
                        packet_id
                    );
                    continue;
                };

                match store.get(&key).await {
                    Ok(Some(data)) => match deserialize_stored_packet(&data) {
                        Ok(mut packet) => {
                            if mqtt_message_expiry::is_packet_expired(&packet, now) {
                                debug!(
                                    "skip expired inflight publish retry for packet {}",
                                    packet_id
                                );
                                continue;
                            }
                            packet.set_dup(1);
                            retry_packets.push(packet);
                        }
                        Err(e) => {
                            warn!(
                                "skip inflight publish retry for packet {} because payload decode failed: {}",
                                packet_id, e
                            );
                        }
                    },
                    Ok(None) => {
                        warn!(
                            "skip inflight publish retry for packet {} because payload {} is missing",
                            packet_id, key
                        );
                    }
                    Err(e) => {
                        warn!(
                            "skip inflight publish retry for packet {} because payload load failed: {}",
                            packet_id, e
                        );
                    }
                }
            }
            _ => {}
        }
    }

    retry_packets
}

async fn write_inflight_retry_packets(
    conn: Recipient<ConnectionActorMessage>,
    packets: Vec<Packet>,
) {
    for packet in packets {
        if let Err(e) = conn
            .send(ConnectionActorMessage::WritePacketToClient(packet))
            .await
        {
            warn!("inflight retry write to connection actor failed: {}", e);
        }
    }
}

async fn do_handle_unsubscribe(
    unsubscribe_packet: Unsubscribe,
    client_info: &Client,
    topic_service: TopicService,
) -> HandleUnSubscribeResult {
    let unsub_topic_filters = &unsubscribe_packet.topics;
    let mut succeed_unsubscriptions = vec![];
    {
        for topic in unsub_topic_filters {
            let tenant_id = client_info.tenant_id.clone();
            let client_id = client_info.client_identifier.clone();
            if let Err(e) = topic_service
                .unsubscribe(tenant_id.clone(), client_id.clone(), topic.clone())
                .await
            {
                error!(
                    "session {} unsubscribe topic {} error: {}",
                    client_info.client_identifier, topic, e
                );
            }
            // warning: even if the unsubscription fails in raft layer,
            // we still return success to client and remove the subscription in memory,
            // this may cause inconsistency between session state and topic state in raft,
            // but mqtt 3.1.1 protocol does not specify the behavior when unsubscription fails,
            // and this can avoid client being stuck in retry loop when raft layer is unavailable
            succeed_unsubscriptions.push(topic.clone());
        }
    }
    let unsuback_packet = Unsuback {
        protocol_version: unsubscribe_packet.protocol_version,
        packet_identifier: unsubscribe_packet.packet_identifier,
        reason_codes: vec![ReasonCode::Success; succeed_unsubscriptions.len().max(1)],
        properties: Properties::default(),
    };
    HandleUnSubscribeResult {
        unsuback_packet,
        succeed_unsubscriptions,
    }
}

async fn do_handle_publish(
    publish_packet: Publish,
    context: HandlePublishContext,
) -> Result<HandlePublishResult, HandlePublishError> {
    let mut publish_packet = publish_packet;
    let mut result = HandlePublishResult {
        inflight_packet: None,
    };

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
        topic: publish_packet.topic_name.clone(),
        qos: publish_packet.qos as u32,
        context: properties_to_struct(&publish_packet.properties),
    };

    let publish_authorize_result = context
        .plugin_manager
        .call_authorize_hook(authorize_request)
        .await;

    let publish_authorize_result = match publish_authorize_result {
        Ok(result) => result,
        Err(e) => {
            warn!("publish authorize hook failed: {}", e);
            result.inflight_packet =
                inbound_publish_ack(&publish_packet, ReasonCode::UnspecifiedError);
            return Ok(result);
        }
    };

    if publish_authorize_result.authorized {
        if mqtt_message_expiry::is_publish_expired(
            &publish_packet,
            mqtt_message_expiry::now_unix_secs(),
        ) {
            if publish_packet.retain && publish_packet.payload.is_empty() {
                if let Err(e) = context
                    .topic_service
                    .clean_retain_publish_packet(
                        context.client_info.tenant_id.clone(),
                        publish_packet.topic_name.clone(),
                    )
                    .await
                {
                    error!("clean expired retain tombstone publish packet error: {}", e);
                }
            }
            result.inflight_packet = inbound_publish_success_ack(&publish_packet);
            return Ok(result);
        }

        let publish_policy_result = context
            .plugin_manager
            .call_on_message_publish(MessagePublishRequest {
                message: Some(mqtt_message_from_publish(
                    &publish_packet,
                    &context.client_info,
                )),
                context: None,
            })
            .await;

        let publish_policy_result = match publish_policy_result {
            Ok(result) => result,
            Err(e) => {
                warn!("on message publish hook failed: {}", e);
                result.inflight_packet = inbound_publish_success_ack(&publish_packet);
                return Ok(result);
            }
        };

        if !publish_policy_result.allow {
            result.inflight_packet = inbound_publish_success_ack(&publish_packet);
            return Ok(result);
        }

        if let Some(modified_message) = publish_policy_result.modified_message {
            apply_modified_mqtt_message(&mut publish_packet, modified_message);
        }

        context
            .plugin_manager
            .call_message_published_hook(MessagePublishRequest {
                message: Some(mqtt_message_from_publish(
                    &publish_packet,
                    &context.client_info,
                )),
                context: None,
            })
            .await;

        let mut session_state_guard = context.session_state.write().await;

        if publish_packet.qos > 0 {
            debug!(
                "client {} start process packet {:?} ",
                context.client_info.client_identifier,
                publish_packet.clone()
            );

            let packet_id = match publish_packet.packet_identifier {
                Some(id) => id,
                None => {
                    warn!("Packet ID is required for QoS > 0");
                    return Err(HandlePublishError::PacketIdNotExist);
                }
            };
            let qos = publish_packet.qos;

            let key = if let Some(store) = &context.payload_store {
                let k = uuid::Uuid::new_v4().to_string();
                let data = serialize_stored_packet(&Packet::Publish(publish_packet.clone()))
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
                match context
                    .session_state_service
                    .register_inflight_rx_packet(
                        context.client_info.tenant_id.clone(),
                        context.client_info.client_identifier.clone(),
                        packet_id,
                        qos,
                        key.clone(),
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        warn!("inflight register rx packet error, {}", e);
                        return Ok(HandlePublishResult {
                            inflight_packet: None,
                        });
                    }
                }
            }

            let packet = if qos == 1 {
                Packet::Puback(success_ack(packet_id))
            } else {
                Packet::Pubrec(success_ack(packet_id))
            };
            result.inflight_packet = Some(packet);
        }

        // process retain messages
        if publish_packet.retain {
            // register retain publish packet

            // if publish packet paloyd is empty , clean retained publish packet
            if publish_packet.payload.is_empty() {
                if let Err(e) = context
                    .topic_service
                    .clean_retain_publish_packet(
                        context.client_info.tenant_id.clone(),
                        publish_packet.topic_name.clone(),
                    )
                    .await
                {
                    error!("clean retain publish packet error: {}", e);
                }
            } else {
                if let Err(e) = context
                    .topic_service
                    .register_retain_publish_packet(
                        context.client_info.tenant_id.clone(),
                        context.client_info.client_identifier.clone(),
                        Packet::Publish(publish_packet.clone()),
                    )
                    .await
                {
                    error!("register retain publish packet error: {}", e);
                }
            }
            //
        }
        //
        // select router actor based on topic
        let hash = publish_topic_hash(&publish_packet.topic_name);
        let router_actor = &context.router_actors[hash as usize % context.router_actors.len()];

        router_actor
            .send(crate::router_actor::RoutePacket {
                tenant_id: context.client_info.tenant_id.clone(),
                packet: Packet::Publish(publish_packet.clone()),
                source_client_identifier: Some(context.client_info.client_identifier.clone()),
            })
            .await
            .map_err(|e| HandlePublishError::RouteError(e.to_string()))?
            .map_err(|e| HandlePublishError::RouteError(e.to_string()))?;
    } else {
        result.inflight_packet = inbound_publish_ack(&publish_packet, ReasonCode::NotAuthorized);
        return Ok(result);
    }

    Ok(result)
}

fn mqtt_message_from_publish(publish_packet: &Publish, client_info: &Client) -> MqttMessage {
    MqttMessage {
        tenant_id: client_info.tenant_id.clone(),
        client_id: client_info.client_identifier.clone(),
        topic: publish_packet.topic_name.clone(),
        payload: publish_packet.payload.to_vec(),
        qos: publish_packet.qos as u32,
        retain: publish_packet.retain,
        dup: publish_packet.dup,
        publish_time: None,
        properties: properties_to_struct(&publish_packet.properties),
        message_id: None,
    }
}

fn apply_modified_mqtt_message(publish_packet: &mut Publish, modified_message: MqttMessage) {
    if !modified_message.topic.is_empty() {
        publish_packet.topic_name = modified_message.topic;
    }
    publish_packet.payload = Bytes::from(modified_message.payload);
    publish_packet.retain = modified_message.retain;
    publish_packet.dup = modified_message.dup;
}

async fn do_handle_subscribe(
    subscribe_packet: &Subscribe,
    client_info: &Client,
    plugin_manager: Arc<PluginManager>,
    topic_service: TopicService,
    existing_subscriptions: HashSet<String>,
) -> HandleSubscribeResult {
    let tenant_id = client_info.tenant_id.clone();

    let client_id = client_info.client_identifier.clone();

    let subscriptions = &subscribe_packet.topics;

    let mut retain_messages: Vec<RetainedPublishDelivery> = vec![];

    let mut return_code: Vec<ReasonCode> = vec![];

    let mut succeed_subscriptions: Vec<(String, SubscriptionState)> = vec![];

    if subscribe_packet.protocol_version == ProtocolVersion::V5_0
        && !subscribe_packet
            .properties
            .subscription_identifiers
            .is_empty()
    {
        return HandleSubscribeResult {
            retain_messages,
            suback_packet: Suback {
                protocol_version: subscribe_packet.protocol_version,
                packet_identifier: subscribe_packet.packet_identifier,
                reason_codes: vec![
                    ReasonCode::SubscriptionIdentifiersNotSupported;
                    subscriptions.len()
                ],
                properties: Properties::default(),
            },
            succeed_subscriptions,
        };
    }

    for i in 0..subscriptions.len() {
        let topic = subscribe_packet.topics[i].clone();
        if let Some(reason) =
            subscribe_precheck_reason(subscribe_packet.protocol_version, &topic.topic_filter)
        {
            return_code.push(reason);
            continue;
        }

        let authorize_request = AuthorizeRequest {
            tenant_id: tenant_id.clone(),
            client_id: client_id.clone(),
            username: client_info
                .properties
                .username
                .clone()
                .unwrap_or("".to_string()),
            action: AuthAction::Subscribe.into(),
            topic: topic.topic_filter.clone(),
            qos: topic.qos.into(),
            context: Some(subscribe_context(&topic, &subscribe_packet.properties)),
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
            let subscribe_policy_result = plugin_manager
                .call_on_message_subscribe(SubscribeRequest {
                    client_id: client_id.clone(),
                    subscriptions: vec![TopicFilter {
                        topic: topic.topic_filter.clone(),
                        qos: topic.qos as u32,
                        options: Some(subscribe_context(&topic, &subscribe_packet.properties)),
                    }],
                    context: None,
                })
                .await;

            let subscribe_policy_item = match subscribe_policy_result {
                Ok(result) => result
                    .result
                    .into_iter()
                    .find(|item| item.topic == topic.topic_filter),
                Err(e) => {
                    warn!("on message subscribe hook failed: {}", e);
                    None
                }
            };

            let Some(subscribe_policy_item) = subscribe_policy_item else {
                return_code.push(ReasonCode::NotAuthorized);
                continue;
            };

            if !subscribe_policy_item.allowed {
                return_code.push(ReasonCode::NotAuthorized);
                continue;
            }

            let granted_qos = cmp::min(cmp::min(subscribe_policy_item.granted_qos, topic.qos), 2);

            match topic_service
                .subscribe_with_options(
                    tenant_id.clone(),
                    client_id.clone(),
                    topic.topic_filter.clone(),
                    granted_qos,
                    topic.no_local,
                    topic.retain_as_published,
                )
                .await
            {
                Ok(()) => {
                    succeed_subscriptions.push((
                        topic.topic_filter.clone(),
                        SubscriptionState::new(
                            granted_qos.into(),
                            topic.no_local,
                            topic.retain_as_published,
                        ),
                    ));
                    return_code.push(granted_qos_reason(granted_qos));

                    let send_retained = match topic.retain_handling {
                        RetainHandling::SendAtSubscribe => true,
                        RetainHandling::SendAtSubscribeIfNew => {
                            !existing_subscriptions.contains(&topic.topic_filter)
                        }
                        RetainHandling::DoNotSend => false,
                    };

                    if send_retained {
                        // For shared subscriptions, use the normalized inner filter for retained lookup
                        let retain_filter =
                            match parse_shared_subscription_filter(&topic.topic_filter) {
                                Ok(Some(shared)) => shared.topic_filter,
                                _ => topic.topic_filter.clone(),
                            };
                        match topic_service
                            .get_retain_publish_packets_linearizable(
                                tenant_id.clone(),
                                retain_filter,
                            )
                            .await
                        {
                            Ok(packets) => {
                                retain_messages.extend(packets.into_iter().map(|packet| {
                                    RetainedPublishDelivery {
                                        packet,
                                        requested_qos: granted_qos,
                                        retain_as_published: topic.retain_as_published,
                                        subscriber_protocol: subscribe_packet.protocol_version,
                                    }
                                }));
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to get retain publish packet for session {} subscribe topic {}, error: {}",
                                    client_id, topic.topic_filter, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "session {} subscribe topic {} error: {}",
                        client_id, topic.topic_filter, e
                    );
                    return_code.push(ReasonCode::UnspecifiedError);
                }
            }
        } else if let Some(reason) = authorization_failure_reason(&topic_authorizate_result) {
            return_code.push(reason);
        }
    }

    let suback_packet = Suback {
        protocol_version: subscribe_packet.protocol_version,
        packet_identifier: subscribe_packet.packet_identifier,
        reason_codes: return_code,
        properties: Properties::default(),
    };

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
    pub protocol_version: ProtocolVersion,
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: u16,
    pub plugin_manager: Arc<PluginManager>,
    pub inflight_retry_duration_secs: u64,
    pub will_message: Option<WillMessage>,
    pub keep_alive: u64,
    pub connection_actor_addr: Recipient<ConnectionActorMessage>,
    pub peer_addr: SocketAddr,
    pub session_state: Arc<RwLock<SessionState>>,
    pub session_lifecycle_tx: Sender<SessionLifecycleMessage>,
    pub session_state_service: SessionStateService,
    pub topic_service: TopicService,
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
            protocol_version: config.protocol_version,
            session_expiry_interval: config.session_expiry_interval,
            receive_maximum: config.receive_maximum,
            outbound_receive_slots_in_use: 0,
            deferred_outbound_publishes: VecDeque::new(),
            keep_alive: config.keep_alive,
            keep_alive_expired: true,
            inflight_retry_interval: config.inflight_retry_duration_secs,
            tenant_id: config.tenant_id,
            client_id: config.client_id,
            will_message: config.will_message,
            username: None,
            state: config.session_state,
            session_lifecycle_tx: config.session_lifecycle_tx,
            session_state_service: config.session_state_service,
            topic_service: config.topic_service,
            router_actors: config.router_actors,
            payload_store: config.payload_store,
            timer_actor: config.timer_actor,
            metric: config.metric,
            session_version: config.session_version,
            session_metrics: Arc::new(session_metrics),
        }
    }

    fn receive_maximum_limit(&self) -> usize {
        usize::from(self.receive_maximum.max(1))
    }

    fn outbound_publish_uses_receive_slot(&self, publish_packet: &Publish) -> bool {
        self.protocol_version == ProtocolVersion::V5_0
            && matches!(self.activity_state, ActivityState::Active)
            && publish_packet.qos > 0
    }

    fn can_reserve_receive_slot(&self) -> bool {
        self.outbound_receive_slots_in_use < self.receive_maximum_limit()
    }

    fn reserve_receive_slot(&mut self) {
        self.outbound_receive_slots_in_use += 1;
    }

    fn release_receive_maximum_slot_and_drain(
        &mut self,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        if self.outbound_receive_slots_in_use > 0 {
            self.outbound_receive_slots_in_use -= 1;
        }
        self.drain_deferred_outbound_publishes(ctx);
    }

    fn drain_deferred_outbound_publishes(&mut self, ctx: &mut <SessionActor as Actor>::Context) {
        loop {
            let Some(next_publish) = self.deferred_outbound_publishes.front() else {
                break;
            };
            if self.outbound_publish_uses_receive_slot(next_publish)
                && !self.can_reserve_receive_slot()
            {
                break;
            }

            let publish_packet = self
                .deferred_outbound_publishes
                .pop_front()
                .expect("front item exists");
            self.start_outbound_publish_delivery(publish_packet, ctx);
        }
    }

    fn start_outbound_publish_delivery(
        &mut self,
        publish_packet: Publish,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        let receive_slot_reserved = self.outbound_publish_uses_receive_slot(&publish_packet);
        if receive_slot_reserved && !self.can_reserve_receive_slot() {
            self.deferred_outbound_publishes.push_back(publish_packet);
            return;
        }
        if receive_slot_reserved {
            self.reserve_receive_slot();
        }

        let delivery_context = OutboundPublishDeliveryContext {
            tenant_id: self.tenant_id.clone(),
            client_id: self.client_id.clone(),
            clean_session: self.clean_session,
            protocol_version: self.protocol_version,
            activity_state: self.activity_state,
            session_state: self.state.clone(),
            session_state_service: self.session_state_service.clone(),
            payload_store: self.payload_store.clone(),
            conn: self.conn_recipient.clone(),
        };

        ctx.spawn(
            async move { deliver_outbound_publish(publish_packet, delivery_context).await }
                .into_actor(self)
                .map(move |result, act, ctx| match result {
                    Ok(OutboundPublishDeliveryStatus::Sent) => {}
                    Ok(OutboundPublishDeliveryStatus::NotSent) => {
                        if receive_slot_reserved {
                            act.release_receive_maximum_slot_and_drain(ctx);
                        }
                    }
                    Err(e) => {
                        error!("Failed to deliver outbound publish: {}", e);
                        if receive_slot_reserved {
                            act.release_receive_maximum_slot_and_drain(ctx);
                        }
                    }
                }),
        );
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
        let session_state_service = self.session_state_service.clone();
        let tenant_id = self.tenant_id.clone();
        let client_id = self.client_id.clone();
        let clean_session = self.clean_session;
        let session_expiry_interval_update = if self.protocol_version == ProtocolVersion::V5_0 {
            self.session_expiry_interval
        } else {
            None
        };

        match state {
            ActivityState::Inactive => {
                self.stop_inflight_and_keep_alive_timer();
                if !clean_session {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("system time is before unix epoch")
                        .as_secs();
                    ctx.spawn(
                        async move {
                            let _ = session_state_service
                                .update_session_connection_state(
                                    tenant_id,
                                    client_id,
                                    Some(now),
                                    session_expiry_interval_update,
                                )
                                .await;
                        }
                        .into_actor(self),
                    );
                }
            }
            ActivityState::Active => {
                if !clean_session {
                    ctx.spawn(
                        async move {
                            let _ = session_state_service
                                .update_session_connection_state(
                                    tenant_id,
                                    client_id,
                                    None,
                                    session_expiry_interval_update,
                                )
                                .await;
                        }
                        .into_actor(self),
                    );
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

    fn should_delete_session_on_disconnect(&self) -> bool {
        self.protocol_version == ProtocolVersion::V5_0
            && self.session_expiry_interval == Some(0)
            && !self.clean_session
    }

    fn delete_session_state_and_force_stop(&mut self, ctx: &mut <SessionActor as Actor>::Context) {
        let session_state_service = self.session_state_service.clone();
        let tenant_id = self.tenant_id.clone();
        let client_id = self.client_id.clone();
        ctx.spawn(
            async move {
                let _ = session_state_service
                    .delete_session_state(tenant_id, client_id, None)
                    .await;
            }
            .into_actor(self)
            .then(|_, act, ctx| {
                act.force_stop(ctx);
                fut::ready(())
            }),
        );
    }

    fn finish_connection_disconnect(&mut self, ctx: &mut <SessionActor as Actor>::Context) {
        if self.should_delete_session_on_disconnect() {
            self.delete_session_state_and_force_stop(ctx);
        } else if !self.clean_session {
            self.set_state(ctx, ActivityState::Inactive);
            self.session_metrics.set_disconnected();
        } else {
            self.force_stop(ctx);
        }
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
        mut publish_packet: Publish,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        self.metric.increase_messages_received();
        self.session_metrics.increase_messages_received();
        mqtt_message_expiry::stamp_publish_expiry(
            &mut publish_packet,
            mqtt_message_expiry::now_unix_secs(),
        );
        let context = HandlePublishContext {
            client_info: self.get_plugin_client_info(),
            plugin_manager: self.plugin_manager.clone(),
            session_state: self.state.clone(),
            clean_session: self.clean_session,
            router_actors: self.router_actors.clone(),
            session_state_service: self.session_state_service.clone(),
            topic_service: self.topic_service.clone(),
            payload_store: self.payload_store.clone(),
        };

        ctx.spawn(
            async move { do_handle_publish(publish_packet, context).await }
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
                                let session_state_service = act.session_state_service.clone();
                                let tenant_id = act.tenant_id.clone();
                                let client_id = act.client_id.clone();

                                conn.do_send(ConnectionActorMessage::WritePacketToClient(
                                    packet.clone(),
                                ));

                                // If it was a Puback (QoS 1 Receiver completion), trigger cleanup
                                if matches!(packet, Packet::Puback(_)) {
                                    if clean_session {
                                        actix::spawn(async move {
                                            Self::cleanup_local_finished_items(
                                                state,
                                                payload_store,
                                            )
                                            .await;
                                        });
                                    } else {
                                        actix::spawn(async move {
                                            let _ = session_state_service
                                                .clean_finished_inflight_items(tenant_id, client_id)
                                                .await;
                                        });
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            match e {
                                HandlePublishError::PacketIdNotExist => {
                                    // Qos > 0 publish packet without packet id, this should not happen, force stop the session
                                    _ctx.address().do_send(SessionActorMessage::ForceDisconnect);
                                }
                                HandlePublishError::PayloadStoreError(e) => {
                                    error!(
                                        "Failed to store payload for incoming publish packet: {}",
                                        e
                                    );
                                }
                                HandlePublishError::RouteError(e) => {
                                    error!("Failed to route incoming publish packet: {}", e);
                                }
                            }
                        }
                    }
                }),
        );
    }

    fn handle_subscribe(
        &mut self,
        subscribe_packet: Subscribe,
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
        let topic_service = self.topic_service.clone();
        let session_state_service = self.session_state_service.clone();

        async move {
            let existing_subscriptions = {
                let session_state_guard = session_state.read().await;
                session_state_guard
                    .subscriptions
                    .keys()
                    .cloned()
                    .collect::<HashSet<_>>()
            };
            let res = do_handle_subscribe(
                &subscribe_packet,
                &client_info,
                plugin_manager,
                topic_service,
                existing_subscriptions,
            )
            .await;
            for retained in res.retain_messages {
                let packet = (*retained.packet).clone();
                if let Packet::Publish(mut p) = packet {
                    if !mqtt_message_expiry::prepare_publish_for_delivery(
                        &mut p,
                        retained.subscriber_protocol,
                        mqtt_message_expiry::now_unix_secs(),
                    ) {
                        continue;
                    }
                    let retained_msg_qos = p.qos;
                    let min_qos = cmp::min(retained_msg_qos, retained.requested_qos);
                    if min_qos == 0 && retained_msg_qos > 0 {
                        p.packet_identifier = None;
                    }
                    p.qos = min_qos;
                    p.retain = match retained.subscriber_protocol {
                        ProtocolVersion::V3_1_1 => true,
                        ProtocolVersion::V5_0 => retained.retain_as_published && p.retain,
                    };
                    conn.do_send(ConnectionActorMessage::WritePacketToClient(
                        Packet::Publish(p),
                    ));
                }
            }
            conn.do_send(ConnectionActorMessage::WritePacketToClient(Packet::Suback(
                res.suback_packet,
            )));

            let client_info_ref = &client_info;
            let tenant_id = &client_info_ref.tenant_id;
            let client_id = &client_info_ref.client_identifier;

            for _ in 0..res.succeed_subscriptions.len() {
                metric.increase_subscriptions_count();
            }

            if !clean_session {
                for (topic, subscription) in &res.succeed_subscriptions {
                    match session_state_service
                        .subscribe_topic_with_options(
                            tenant_id.clone(),
                            client_id.clone(),
                            topic.clone(),
                            subscription.qos.as_u8(),
                            subscription.no_local,
                            subscription.retain_as_published,
                        )
                        .await
                    {
                        Ok(()) => {
                            session_state
                                .write()
                                .await
                                .subscriptions
                                .insert(topic.clone(), subscription.clone());
                        }
                        Err(e) => {
                            warn!(
                                "persistent session raft add subscribe topic {} error, {}",
                                topic.clone(),
                                e
                            );
                        }
                    }
                }
            } else {
                for (topic, subscription) in &res.succeed_subscriptions {
                    session_state
                        .write()
                        .await
                        .subscriptions
                        .insert(topic.clone(), subscription.clone());
                }
            }
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn handle_pingreq(&mut self) {
        if let Some(recipient) = &self.conn_recipient {
            recipient.do_send(ConnectionActorMessage::WritePacketToClient(
                Packet::Pingresp,
            ));
        }
    }

    fn handle_unsubscribe(
        &mut self,
        unsubscribe_packet: Unsubscribe,
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
        let topic_service = self.topic_service.clone();
        let session_state_service = self.session_state_service.clone();
        let metric = self.metric.clone();
        async move {
            let res = do_handle_unsubscribe(unsubscribe_packet, &client_info, topic_service).await;
            for _ in 0..res.succeed_unsubscriptions.len() {
                metric.decrease_subscriptions_count();
            }
            for topic in res.succeed_unsubscriptions {
                {
                    session_state.write().await.subscriptions.remove(&topic);
                }
                if !clean_session {
                    match session_state_service
                        .unsubscribe_topic(
                            client_info.tenant_id.clone(),
                            client_info.client_identifier.clone(),
                            topic.clone(),
                        )
                        .await
                    {
                        Ok(()) => {}
                        Err(e) => {
                            error!(
                                "persistent session raft remove subscribe topic {} error, {}",
                                topic.clone(),
                                e
                            );
                        }
                    }
                }
            }
            conn.do_send(ConnectionActorMessage::WritePacketToClient(
                Packet::Unsuback(res.unsuback_packet),
            ));
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn handle_pubrel(&mut self, pubrel_packet: Ack, ctx: &mut <SessionActor as Actor>::Context) {
        let session_state = self.state.clone();
        let conn = if let Some(recipient) = &self.conn_recipient {
            recipient.clone()
        } else {
            return; // If connection recipient is none, it means the connection is already closed, no need to handle pubrel
        };
        let clean_session = self.clean_session;
        let client_info = self.get_plugin_client_info();
        let session_state_service = self.session_state_service.clone();
        let payload_store = self.payload_store.clone();

        async move {
            let mut session_state_guard = session_state.write().await;
            let packet_id = pubrel_packet.packet_identifier;

            // Generate Pubcomp response
            let pubcomp = Packet::Pubcomp(success_ack(packet_id));

            if let Err(e) = conn
                .send(ConnectionActorMessage::WritePacketToClient(pubcomp))
                .await
            {
                warn!("handle pubrel write pubcomp to client error: {}", e);
            } else if clean_session {
                session_state_guard.inflight.next_state(packet_id);
                // Trigger local cleanup
                let state = session_state.clone();
                let store = payload_store.clone();
                actix::spawn(async move {
                    Self::cleanup_local_finished_items(state, store).await;
                });
            } else {
                match session_state_service
                    .advance_inflight_state(
                        client_info.tenant_id.clone(),
                        client_info.client_identifier.clone(),
                        packet_id,
                    )
                    .await
                {
                    Ok(()) => {
                        session_state_guard.inflight.next_state(packet_id);

                        // Trigger Raft cleanup
                        let _ = session_state_service
                            .clean_finished_inflight_items(
                                client_info.tenant_id.clone(),
                                client_info.client_identifier.clone(),
                            )
                            .await;
                        session_state_guard.inflight.clean_finished_items();
                    }
                    Err(e) => {
                        error!("handle pubrel raft advance state error {}", e);
                    }
                }
            }
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn handle_pubrec(&mut self, pubrec_packet: Ack, ctx: &mut <SessionActor as Actor>::Context) {
        let session_state = self.state.clone();
        let conn = if let Some(recipient) = &self.conn_recipient {
            recipient.clone()
        } else {
            return; // If connection recipient is none, it means the connection is already closed, no need to handle pubrec
        };
        let clean_session = self.clean_session;
        let client_info = self.get_plugin_client_info();
        let session_state_service = self.session_state_service.clone();

        async move {
            let packet_id = pubrec_packet.packet_identifier;
            let is_expected = matches!(
                session_state
                    .read()
                    .await
                    .inflight
                    .get_inflight_current_state(packet_id),
                Some(InflightState::WaitPubrec)
            );
            if !is_expected {
                warn!(
                    "ignoring PUBREC for unknown packet identifier {}",
                    packet_id
                );
                return false;
            }

            // Generate Pubrel command
            let pubrel = Packet::Pubrel(success_ack(packet_id));

            if let Err(e) = conn
                .send(ConnectionActorMessage::WritePacketToClient(pubrel))
                .await
            {
                warn!("handle pubrec write pubrel to client error: {}", e);
            } else if clean_session {
                let mut session_state_guard = session_state.write().await;
                session_state_guard.inflight.next_state(packet_id);
            } else {
                match session_state_service
                    .advance_inflight_state(
                        client_info.tenant_id.clone(),
                        client_info.client_identifier.clone(),
                        packet_id,
                    )
                    .await
                {
                    Ok(()) => {
                        let mut session_state_guard = session_state.write().await;
                        session_state_guard.inflight.next_state(packet_id);
                    }
                    Err(e) => {
                        warn!("handle pubrec raft advance state error {}", e);
                        return false;
                    }
                }
            }
            true
        }
        .into_actor(self)
        .then(|release_slot, act, ctx| {
            if release_slot {
                act.release_receive_maximum_slot_and_drain(ctx);
            }
            fut::ready(())
        })
        .wait(ctx);
    }

    fn handle_puback(&mut self, puback_packet: Ack, ctx: &mut <SessionActor as Actor>::Context) {
        let session_state = self.state.clone();
        let clean_session = self.clean_session;
        let client_info = self.get_plugin_client_info();
        let session_state_service = self.session_state_service.clone();
        let payload_store = self.payload_store.clone();

        async move {
            let packet_id = puback_packet.packet_identifier;
            let is_expected = matches!(
                session_state
                    .read()
                    .await
                    .inflight
                    .get_inflight_current_state(packet_id),
                Some(InflightState::WaitPuback)
            );
            if !is_expected {
                warn!(
                    "ignoring PUBACK for unknown packet identifier {}",
                    packet_id
                );
                return false;
            }

            if clean_session {
                let mut session_state_guard = session_state.write().await;
                session_state_guard.inflight.next_state(packet_id);
                let state = session_state.clone();
                let store = payload_store.clone();
                actix::spawn(async move {
                    Self::cleanup_local_finished_items(state, store).await;
                });
            } else {
                match session_state_service
                    .advance_inflight_state(
                        client_info.tenant_id.clone(),
                        client_info.client_identifier.clone(),
                        packet_id,
                    )
                    .await
                {
                    Ok(()) => {
                        {
                            let mut session_state_guard = session_state.write().await;
                            session_state_guard.inflight.next_state(packet_id);
                        }

                        match session_state_service
                            .clean_finished_inflight_items(
                                client_info.tenant_id.clone(),
                                client_info.client_identifier.clone(),
                            )
                            .await
                        {
                            Ok(()) => {
                                let mut session_state_guard = session_state.write().await;
                                session_state_guard.inflight.clean_finished_items();
                            }
                            Err(e) => {
                                warn!("handle puback raft clean finished items error {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("handle puback raft advance state error {}", e);
                        return false;
                    }
                }
            }
            true
        }
        .into_actor(self)
        .then(|release_slot, act, ctx| {
            if release_slot {
                act.release_receive_maximum_slot_and_drain(ctx);
            }
            fut::ready(())
        })
        .wait(ctx);
    }

    fn handle_pubcomp(&mut self, pubcomp_packet: Ack, ctx: &mut <SessionActor as Actor>::Context) {
        let session_state = self.state.clone();

        let clean_session = self.clean_session;

        let client_info = self.get_plugin_client_info();

        let session_state_service = self.session_state_service.clone();

        let payload_store = self.payload_store.clone();

        async move {
            let packet_id = pubcomp_packet.packet_identifier;

            if clean_session {
                let mut session_state_guard = session_state.write().await;
                session_state_guard.inflight.next_state(packet_id);

                let state = session_state.clone();

                let store = payload_store.clone();

                actix::spawn(async move {
                    Self::cleanup_local_finished_items(state, store).await;
                });
            } else {
                match session_state_service
                    .advance_inflight_state(
                        client_info.tenant_id.clone(),
                        client_info.client_identifier.clone(),
                        packet_id,
                    )
                    .await
                {
                    Ok(()) => {
                        {
                            let mut session_state_guard = session_state.write().await;
                            session_state_guard.inflight.next_state(packet_id);
                        }

                        match session_state_service
                            .clean_finished_inflight_items(
                                client_info.tenant_id.clone(),
                                client_info.client_identifier.clone(),
                            )
                            .await
                        {
                            Ok(()) => {
                                let mut session_state_guard = session_state.write().await;
                                session_state_guard.inflight.clean_finished_items();
                            }
                            Err(e) => {
                                warn!("handle pubcomp raft clean finished items error {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("handle pubcomp raft advance state error {}", e);
                    }
                };
            }
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn handle_disconnect(
        &mut self,
        disconnect_packet: yedmq_mqtt::packet::Disconnect,
        ctx: &mut <SessionActor as Actor>::Context,
    ) {
        self.clean_will_message();

        if disconnect_packet.protocol_version == ProtocolVersion::V5_0 {
            let requested_expiry = disconnect_packet
                .session_expiry_interval
                .or(disconnect_packet.properties.session_expiry_interval);
            match updated_session_expiry_interval(self.session_expiry_interval, requested_expiry) {
                Ok(updated) => self.session_expiry_interval = updated,
                Err(reason_code) => {
                    if let Some(recipient) = &self.conn_recipient {
                        let recipient = recipient.clone();
                        async move {
                            recipient
                                .send(ConnectionActorMessage::WritePacketToClient(
                                    Packet::Disconnect(yedmq_mqtt::packet::Disconnect {
                                        protocol_version: ProtocolVersion::V5_0,
                                        reason_code,
                                        session_expiry_interval: None,
                                        properties: Properties::default(),
                                    }),
                                ))
                                .await
                        }
                        .into_actor(self)
                        .then(|result, act, ctx| {
                            match result {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => {
                                    warn!("failed to encode MQTT 5 protocol error: {}", error)
                                }
                                Err(error) => {
                                    warn!("failed to send MQTT 5 protocol error: {}", error)
                                }
                            }
                            act.finish_connection_disconnect(ctx);
                            fut::ready(())
                        })
                        .wait(ctx);
                    } else {
                        self.finish_connection_disconnect(ctx);
                    }
                    return;
                }
            }
        }
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

        self.finish_connection_disconnect(ctx);
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
                let publish_packet =
                    build_will_publish(&will_message, mqtt_message_expiry::now_unix_secs());

                let hash = publish_topic_hash(&will_message.will_topic);
                let router_actor = &router_actors[hash as usize % router_actors.len()];
                router_actor.do_send(crate::router_actor::RoutePacket {
                    tenant_id,
                    packet: Packet::Publish(publish_packet),
                    source_client_identifier: None,
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
                    Packet::Pingreq => {
                        self.handle_pingreq();
                    }
                    Packet::Subscribe(subscribe_packet) => {
                        self.handle_subscribe(subscribe_packet, ctx);
                    }
                    Packet::Unsubscribe(unsubscribe_packet) => {
                        self.handle_unsubscribe(unsubscribe_packet, ctx);
                    }
                    Packet::Publish(publish_packet) => {
                        self.handle_publish(publish_packet, ctx);
                    }
                    Packet::Puback(puback_packet) => {
                        self.handle_puback(puback_packet, ctx);
                    }
                    Packet::Pubrec(pubrec_packet) => {
                        self.handle_pubrec(pubrec_packet, ctx);
                    }
                    Packet::Pubrel(pubrel_packet) => {
                        self.handle_pubrel(pubrel_packet, ctx);
                    }
                    Packet::Pubcomp(pubcomp_packet) => {
                        self.handle_pubcomp(pubcomp_packet, ctx);
                    }
                    Packet::Disconnect(disconnect_packet) => {
                        self.handle_disconnect(disconnect_packet, ctx);
                    }
                    _ => {}
                }
            }
            SessionActorMessage::OutboundMessage(packet) => {
                if let Packet::Publish(publish_packet) = packet {
                    self.metric.increase_messages_sent();
                    self.session_metrics.increase_messages_sent();
                    self.start_outbound_publish_delivery(publish_packet, ctx);
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
                        actor.finish_connection_disconnect(ctx);
                    }
                    fut::ready(())
                });
            }
            SessionActorMessage::InflightRetry => {
                let session_state = self.state.clone();
                let payload_store = self.payload_store.clone();
                let conn = self.conn_recipient.clone();
                self.register_inflight_retry_timer(ctx);
                ctx.spawn(
                    async move {
                        let inflight_entries = {
                            let mut session_state_guard = session_state.write().await;
                            session_state_guard
                                .inflight
                                .get_all_expired_packet_keys_and_refresh_expired_time()
                                .into_iter()
                                .filter_map(|(packet_id, key)| {
                                    session_state_guard
                                        .inflight
                                        .get_inflight_current_state(packet_id)
                                        .map(|state| (packet_id, key, state))
                                })
                                .collect::<Vec<_>>()
                        };

                        let retry_packets =
                            collect_inflight_retry_packets(inflight_entries, payload_store).await;
                        if let Some(conn) = conn {
                            write_inflight_retry_packets(conn, retry_packets).await;
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
                    actor.finish_connection_disconnect(ctx);
                    fut::ready(())
                });
            }
            SessionActorMessage::Reconnect {
                conn,
                keep_alive,
                clean_session,
                protocol_version,
                session_expiry_interval,
                receive_maximum,
                username,
                will_message,
                socket_addr,
            } => {
                self.protocol_version = protocol_version;
                self.session_expiry_interval = session_expiry_interval;
                self.receive_maximum = receive_maximum;
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
                let topic_service = self.topic_service.clone();
                let payload_store = self.payload_store.clone();
                let protocol_version = self.protocol_version;
                async move {
                    let mut session_state_guard = session_state.write().await;
                    for key in session_state_guard.pending_messages.drain(..) {
                        if let Some(store) = &payload_store {
                            match store.get(&key).await {
                                Ok(Some(data)) => {
                                    if let Ok(mut packet) = deserialize_stored_packet(&data) {
                                        if mqtt_message_expiry::prepare_packet_for_delivery(
                                            &mut packet,
                                            protocol_version,
                                            mqtt_message_expiry::now_unix_secs(),
                                        ) {
                                            session_actor_addr.do_send(
                                                SessionActorMessage::OutboundMessage(packet),
                                            );
                                        }
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
                    for (topic, subscription) in session_state_guard.subscriptions.iter() {
                        if let Err(e) = topic_service
                            .subscribe_with_options(
                                tenant_id.clone(),
                                client_identifier.clone(),
                                topic.clone(),
                                subscription.qos.as_u8(),
                                subscription.no_local,
                                subscription.retain_as_published,
                            )
                            .await
                        {
                            warn!(
                                "handle subscribe topic error for session {}, topic {}, error: {}",
                                client_identifier, topic, e
                            );
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
                self.finish_connection_disconnect(ctx);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disconnect_session_expiry_cannot_exceed_connect_value() {
        assert_eq!(
            updated_session_expiry_interval(Some(0), Some(1)),
            Err(ReasonCode::ProtocolError)
        );
        assert_eq!(
            updated_session_expiry_interval(Some(30), Some(31)),
            Err(ReasonCode::ProtocolError)
        );
        assert_eq!(
            updated_session_expiry_interval(Some(30), Some(10)),
            Ok(Some(10))
        );
        assert_eq!(
            updated_session_expiry_interval(Some(30), None),
            Ok(Some(30))
        );
    }

    #[test]
    fn mqtt5_shared_subscription_filter_is_accepted() {
        assert_eq!(
            subscribe_precheck_reason(ProtocolVersion::V5_0, "$share/group/sensors/+"),
            None
        );
    }

    #[test]
    fn mqtt5_invalid_shared_subscription_filter_is_rejected() {
        assert_eq!(
            subscribe_precheck_reason(ProtocolVersion::V5_0, "$share//sensors/+"),
            Some(ReasonCode::TopicFilterInvalid)
        );
    }

    #[test]
    fn mqtt3_shared_subscription_filter_is_accepted() {
        assert_eq!(
            subscribe_precheck_reason(ProtocolVersion::V3_1_1, "$share/group/sensors/+"),
            None
        );
    }

    #[test]
    fn unauthorized_subscribe_maps_to_suback_reason() {
        let authorize_result = AuthorizeResult {
            authorized: false,
            reason: Some("denied".to_string()),
            modified_context: HashMap::new(),
        };

        assert_eq!(
            authorization_failure_reason(&authorize_result),
            Some(ReasonCode::NotAuthorized)
        );
    }

    #[test]
    fn mqtt5_authorized_publish_denial_maps_qos1_to_not_authorized_puback() {
        let publish = Publish {
            protocol_version: ProtocolVersion::V5_0,
            topic_name: "denied/topic".to_string(),
            payload: Bytes::from_static(b"payload"),
            qos: 1,
            retain: false,
            dup: false,
            packet_identifier: Some(42),
            properties: Properties::default(),
            expires_at_unix_secs: None,
        };

        let packet = inbound_publish_ack(&publish, ReasonCode::NotAuthorized).unwrap();
        match packet {
            Packet::Puback(ack) => {
                assert_eq!(ack.packet_identifier, 42);
                assert_eq!(ack.reason_code, ReasonCode::NotAuthorized);
            }
            other => panic!("expected PUBACK, got {other:?}"),
        }
    }

    #[test]
    fn mqtt5_authorized_publish_denial_maps_qos2_to_not_authorized_pubrec() {
        let publish = Publish {
            protocol_version: ProtocolVersion::V5_0,
            topic_name: "denied/topic".to_string(),
            payload: Bytes::from_static(b"payload"),
            qos: 2,
            retain: false,
            dup: false,
            packet_identifier: Some(43),
            properties: Properties::default(),
            expires_at_unix_secs: None,
        };

        let packet = inbound_publish_ack(&publish, ReasonCode::NotAuthorized).unwrap();
        match packet {
            Packet::Pubrec(ack) => {
                assert_eq!(ack.packet_identifier, 43);
                assert_eq!(ack.reason_code, ReasonCode::NotAuthorized);
            }
            other => panic!("expected PUBREC, got {other:?}"),
        }
    }

    #[test]
    fn will_publish_preserves_mqtt5_publish_properties() {
        let will_message = WillMessage {
            will_topic: "will/topic".to_string(),
            will_message: b"offline".to_vec(),
            will_qos: 1,
            will_retain: true,
            protocol_version: ProtocolVersion::V5_0,
            properties: Properties {
                content_type: Some("text/plain".to_string()),
                message_expiry_interval: Some(30),
                will_delay_interval: Some(0),
                user_properties: vec![("source".to_string(), "will".to_string())],
                ..Properties::default()
            },
        };

        let publish = build_will_publish(&will_message, 100);

        assert_eq!(publish.protocol_version, ProtocolVersion::V5_0);
        assert_eq!(publish.topic_name, "will/topic");
        assert_eq!(publish.payload, Bytes::from_static(b"offline"));
        assert_eq!(publish.qos, 1);
        assert!(publish.retain);
        assert_eq!(
            publish.properties.content_type.as_deref(),
            Some("text/plain")
        );
        assert_eq!(publish.properties.message_expiry_interval, Some(30));
        assert_eq!(
            publish.properties.user_properties,
            vec![("source".to_string(), "will".to_string())]
        );
        assert_eq!(publish.properties.will_delay_interval, None);
        assert_eq!(publish.expires_at_unix_secs, Some(130));
    }
}

impl Handler<AcceptRoutedPublish> for SessionActor {
    type Result = Result<(), SessionActorError>;

    fn handle(&mut self, msg: AcceptRoutedPublish, ctx: &mut Self::Context) -> Self::Result {
        match msg.packet {
            Packet::Publish(publish_packet) => {
                self.start_outbound_publish_delivery(publish_packet, ctx);
                Ok(())
            }
            other => Err(SessionActorError::DeliveryError(format!(
                "unsupported routed packet: {:?}",
                other
            ))),
        }
    }
}

impl Handler<AllInflightRetryImmediate> for SessionActor {
    type Result = ();

    fn handle(&mut self, _msg: AllInflightRetryImmediate, ctx: &mut Self::Context) -> Self::Result {
        let session_state = self.state.clone();
        let payload_store = self.payload_store.clone();
        let conn = self.conn_recipient.clone();
        ctx.spawn(
            async move {
                let inflight_entries = {
                    let mut session_state_guard = session_state.write().await;
                    session_state_guard
                        .inflight
                        .get_all_packet_keys_and_refresh_expired_time()
                        .into_iter()
                        .filter_map(|(packet_id, key)| {
                            session_state_guard
                                .inflight
                                .get_inflight_current_state(packet_id)
                                .map(|state| (packet_id, key, state))
                        })
                        .collect::<Vec<_>>()
                };

                let retry_packets =
                    collect_inflight_retry_packets(inflight_entries, payload_store).await;
                if let Some(conn) = conn {
                    write_inflight_retry_packets(conn, retry_packets).await;
                }
            }
            .into_actor(self),
        );
    }
}
