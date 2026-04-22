use actix::prelude::*;
use log::{info, warn};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::Request;
use yedmq_mqtt::{v3::publish::PublishPacket, MqttPacketV3};

use crate::metric::Metric;
use crate::node_resolver::NodeResolver;
use crate::raft::payload::PayloadStore;
use crate::route_store::JsonRocksDBStore;
use crate::session::session_actor::{AcceptRoutedPublish, SessionActorMessage};
use crate::session::session_actor_map_service::SessionActorMapService;
use crate::session::session_actor_map_storage::{SessionActorMapEntry, SessionActorMapStorage};
use crate::session::session_manager_actor::SessionManagerActor;
use crate::settings::Settings;
use crate::topic::topic_service::TopicService;
use crate::topic::topic_storage::TopicStorage;
use crate::{
    protobuf::cluster_service_client::ClusterServiceClient,
    raft::NodeId,
    session::{session_manager_actor, session_registry::SessionRegistry},
};

#[derive(Clone, Debug, thiserror::Error)]
pub enum RouterActorError {
    #[error("gRPC error: {0}")]
    GRPC(String),

    #[error("actor unexpected stopped")]
    ActorUnExceptedStopped(#[from] MailboxError),

    #[error("topic raft error: {0}")]
    TopicRaftError(#[from] Box<crate::raft::topic::topic_raft_actor::TopicRaftError>),

    #[error("session actor map raft error: {0}")]
    SessionActorMapRaftError(
        #[from]
        Box<
            crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError,
        >,
    ),

    #[error("get subscribers error: {0}")]
    GetSubscribersError(#[from] crate::topic::TopicStorageError),

    #[error("topic service error: {0}")]
    TopicServiceError(String),

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("payload store error: {0}")]
    PayloadStoreError(String),

    #[error("route store error: {0}")]
    RouteStoreError(String),

    #[error("routed delivery error: {0}")]
    RoutedDeliveryError(String),
}

#[derive(Clone, Debug)]
struct RouteRetryConfig {
    max_retry_count: u32,
    retry_interval_seconds: u64,
    cleanup_interval_seconds: u64,
    message_ttl_seconds: u64,
}

impl Default for RouteRetryConfig {
    fn default() -> Self {
        Self {
            max_retry_count: 3,
            retry_interval_seconds: 30,
            cleanup_interval_seconds: 300,
            message_ttl_seconds: 86400,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum RouteInboxStatus {
    Pending,
    Completed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RouteOutboxItem {
    route_id: String,
    tenant_id: String,
    source_node_id: NodeId,
    dest_node_id: NodeId,
    target_client_id: String,
    target_qos: u8,
    packet_key: String,
    retry_at: u64,
    attempts: u32,
    expiry_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RouteInboxItem {
    route_id: String,
    tenant_id: String,
    target_client_id: String,
    packet_key: String,
    retry_at: u64,
    attempts: u32,
    expiry_at: u64,
    status: RouteInboxStatus,
    completed_at: Option<u64>,
}

pub struct RouterActor {
    pub current_node_id: NodeId,
    pub settings: Arc<Settings>,
    pub node_resolver: Arc<NodeResolver>,
    pub session_manager_actor: Addr<SessionManagerActor>,
    pub topic_raft_actor: Option<Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>>,
    pub local_topic_storage: Arc<RwLock<TopicStorage>>,
    pub local_session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
    pub session_registry: SessionRegistry,
    pub payload_store: Arc<dyn PayloadStore>,
    pub route_outbox_store: Arc<JsonRocksDBStore>,
    pub route_inbox_store: Arc<JsonRocksDBStore>,
    retry_config: RouteRetryConfig,
    pub metric: Arc<Metric>,
}

pub struct RouterActorConfig {
    pub settings: Arc<Settings>,
    pub session_manager_actor: Addr<SessionManagerActor>,
    pub topic_raft_actor: Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>,
    pub node_resolver: Arc<NodeResolver>,
    pub topic_storage: Arc<RwLock<TopicStorage>>,
    pub session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
    pub session_registry: SessionRegistry,
    pub payload_store: Arc<dyn PayloadStore>,
    pub route_outbox_store: Arc<JsonRocksDBStore>,
    pub route_inbox_store: Arc<JsonRocksDBStore>,
    pub metric: Arc<Metric>,
}

struct RouteContext {
    current_node_id: NodeId,
    node_resolver: Arc<NodeResolver>,
    topic_raft_actor: Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>,
    local_topic_storage: Arc<RwLock<TopicStorage>>,
    local_session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
    session_registry: SessionRegistry,
    payload_store: Arc<dyn PayloadStore>,
    route_outbox_store: Arc<JsonRocksDBStore>,
    retry_config: RouteRetryConfig,
    metric: Arc<Metric>,
}

impl Actor for RouterActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(10000);
        self.process_route_outbox();
        self.process_route_inbox();
        self.cleanup_completed_inbox_items();

        ctx.run_interval(
            Duration::from_secs(self.retry_config.retry_interval_seconds),
            |act, _| {
                act.process_route_outbox();
                act.process_route_inbox();
            },
        );
        ctx.run_interval(
            Duration::from_secs(self.retry_config.cleanup_interval_seconds),
            |act, _| {
                act.cleanup_completed_inbox_items();
            },
        );
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("RouterActor stopped");
    }
}

impl RouterActor {
    pub fn new(config: RouterActorConfig) -> Self {
        Self {
            current_node_id: config.settings.cluster.node_id,
            settings: config.settings,
            node_resolver: config.node_resolver,
            session_manager_actor: config.session_manager_actor,
            topic_raft_actor: Some(config.topic_raft_actor),
            local_topic_storage: config.topic_storage,
            local_session_actor_map_storage: config.session_actor_map_storage,
            session_registry: config.session_registry,
            payload_store: config.payload_store,
            route_outbox_store: config.route_outbox_store,
            route_inbox_store: config.route_inbox_store,
            retry_config: RouteRetryConfig::default(),
            metric: config.metric,
        }
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is before UNIX_EPOCH")
            .as_secs()
    }

    fn build_packet_key(prefix: &str, route_id: &str) -> String {
        format!("{prefix}:{route_id}")
    }

    fn should_route_durably(packet: &MqttPacketV3) -> bool {
        matches!(
            packet,
            MqttPacketV3::Publish(publish_packet)
                if publish_packet.fix_header.qos.unwrap_or_default() >= 1
        )
    }

    fn adjust_publish_for_subscriber(
        publish_packet: &PublishPacket,
        subscriber_qos: u8,
    ) -> PublishPacket {
        let mut publish_packet = publish_packet.clone();
        if publish_packet.fix_header.qos.unwrap_or_default() >= subscriber_qos.into() {
            if subscriber_qos == 0 && publish_packet.fix_header.qos.unwrap_or_default() > 0 {
                publish_packet.fix_header.qos = Some(0);
                publish_packet.variable_header.packet_identifier = None;
                publish_packet.fix_header.remaining_length =
                    publish_packet.fix_header.remaining_length.saturating_sub(2);
            } else {
                publish_packet.fix_header.qos = Some(subscriber_qos.into());
            }
        }
        publish_packet
    }

    async fn serialize_packet(packet: &MqttPacketV3) -> Result<Vec<u8>, RouterActorError> {
        serde_json::to_vec(packet).map_err(|e| RouterActorError::SerializationError(e.to_string()))
    }

    async fn deserialize_packet(bytes: &[u8]) -> Result<MqttPacketV3, RouterActorError> {
        serde_json::from_slice(bytes)
            .map_err(|e| RouterActorError::SerializationError(e.to_string()))
    }

    async fn route_to_other_node(
        dest_addr: &str,
        request: crate::protobuf::RoutePacketRequest,
    ) -> Result<(), RouterActorError> {
        let mut cluster_client = ClusterServiceClient::connect(format!("http://{}", dest_addr))
            .await
            .map_err(|e| RouterActorError::GRPC(e.to_string()))?;
        cluster_client
            .route_packet(Request::new(request))
            .await
            .map_err(|e| RouterActorError::GRPC(e.to_string()))?;
        Ok(())
    }

    fn route_to_local_session(
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
        session_registry: SessionRegistry,
    ) -> Result<(), RouterActorError> {
        if let Some(recipient) = session_registry.get_session(tenant_id, client_id) {
            recipient.do_send(SessionActorMessage::OutboundMessage(packet));
        } else {
            warn!(
                "session {} not found locally, skip best-effort route",
                client_id
            );
        }
        Ok(())
    }

    async fn route_to_local_session_durable(
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
        session_registry: SessionRegistry,
    ) -> Result<(), RouterActorError> {
        let recipient = session_registry
            .get_accept_routed_publish(tenant_id, client_id)
            .ok_or_else(|| {
                RouterActorError::RoutedDeliveryError(format!(
                    "session {} not found locally",
                    client_id
                ))
            })?;
        recipient
            .send(AcceptRoutedPublish { packet })
            .await?
            .map_err(|e| RouterActorError::RoutedDeliveryError(e.to_string()))?;
        Ok(())
    }

    async fn publish_to_local_subscribers(
        tenant_id: &str,
        packet: &MqttPacketV3,
        topic_raft_actor_addr: Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>,
        session_registry: SessionRegistry,
    ) -> Result<(), RouterActorError> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = &publish_packet.variable_header.topic_name;
            let res = topic_raft_actor_addr
                .send(crate::raft::topic::topic_raft_actor::GetSubscriptions {
                    tenant_id: tenant_id.to_string(),
                    topic: topic.clone(),
                })
                .await?;

            match res {
                Ok(subscriptions) => {
                    for item in subscriptions.subscriptions {
                        let publish_packet =
                            Self::adjust_publish_for_subscriber(publish_packet, item.qos);
                        Self::route_to_local_session(
                            tenant_id,
                            &item.client_identifier,
                            MqttPacketV3::Publish(publish_packet),
                            session_registry.clone(),
                        )?;
                    }
                    Ok(())
                }
                Err(e) => Err(RouterActorError::TopicRaftError(Box::new(e))),
            }
        } else {
            Ok(())
        }
    }

    async fn route(
        context: RouteContext,
        tenant_id: &str,
        packet: &MqttPacketV3,
    ) -> Result<(), RouterActorError> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = &publish_packet.variable_header.topic_name;

            let mut subscriptions = {
                let local_topic_storage = context.local_topic_storage.read();
                local_topic_storage
                    .get_subscriptions(tenant_id.to_string(), topic.clone())?
                    .iter()
                    .map(|x| crate::raft::topic::topic_raft_actor::SubscriptionInfo {
                        client_identifier: x.client_identifier.clone(),
                        qos: x.qos,
                    })
                    .collect::<Vec<_>>()
            };

            if subscriptions.is_empty() {
                let subscriptions_res = TopicService::new(context.topic_raft_actor.clone())
                    .get_subscriptions_linearizable(tenant_id.to_string(), topic.clone())
                    .await
                    .map_err(|e| RouterActorError::TopicServiceError(e.to_string()))?;
                subscriptions = subscriptions_res.subscriptions;
            }

            if subscriptions.is_empty() {
                context.metric.increase_messages_dropped();
                return Ok(());
            }

            let local_results: Vec<Option<SessionActorMapEntry>> = {
                let local_session_actor_map_storage =
                    context.local_session_actor_map_storage.read();
                subscriptions
                    .iter()
                    .map(|item| {
                        local_session_actor_map_storage
                            .get_session_actor_map(tenant_id, &item.client_identifier)
                    })
                    .collect()
            };

            for (item, session_actor_map) in subscriptions.iter().zip(local_results.into_iter()) {
                let session_actor_map = match session_actor_map {
                    Some(session_actor_map) => Some(session_actor_map),
                    None => {
                        let remote = SessionActorMapService::from_registry()
                            .get_session_actor_map_linearizable(
                                tenant_id.to_string(),
                                item.client_identifier.clone(),
                            )
                            .await
                            .map_err(|e| RouterActorError::SessionActorMapRaftError(Box::new(e)))?;
                        remote
                    }
                };

                let Some(session_actor_addr) = session_actor_map else {
                    continue;
                };

                let adjusted_packet = MqttPacketV3::Publish(Self::adjust_publish_for_subscriber(
                    publish_packet,
                    item.qos,
                ));

                if session_actor_addr.node_id != context.current_node_id {
                    if Self::should_route_durably(&adjusted_packet) {
                        Self::enqueue_durable_remote_route(
                            &context,
                            tenant_id,
                            session_actor_addr.node_id,
                            item.client_identifier.clone(),
                            item.qos,
                            adjusted_packet,
                        )
                        .await?;
                    } else {
                        Self::dispatch_best_effort_remote_route(
                            context.node_resolver.clone(),
                            context.topic_raft_actor.clone(),
                            tenant_id.to_string(),
                            session_actor_addr.node_id,
                            item.client_identifier.clone(),
                            item.qos,
                            adjusted_packet,
                        );
                    }
                    continue;
                }

                Self::route_to_local_session(
                    tenant_id,
                    &item.client_identifier,
                    adjusted_packet,
                    context.session_registry.clone(),
                )?;
            }
        }

        Ok(())
    }

    async fn enqueue_durable_remote_route(
        context: &RouteContext,
        tenant_id: &str,
        dest_node_id: NodeId,
        target_client_id: String,
        target_qos: u8,
        packet: MqttPacketV3,
    ) -> Result<(), RouterActorError> {
        let route_id = uuid::Uuid::new_v4().to_string();
        let packet_key = Self::build_packet_key("route-outbox", &route_id);
        let now = Self::now_secs();
        let expiry_at = now + context.retry_config.message_ttl_seconds;
        let packet_bytes = Self::serialize_packet(&packet).await?;

        context
            .payload_store
            .put(&packet_key, bytes::Bytes::from(packet_bytes))
            .await
            .map_err(|e| RouterActorError::PayloadStoreError(e.to_string()))?;

        let item = RouteOutboxItem {
            route_id: route_id.clone(),
            tenant_id: tenant_id.to_string(),
            source_node_id: context.current_node_id,
            dest_node_id,
            target_client_id,
            target_qos,
            packet_key,
            retry_at: now,
            attempts: 0,
            expiry_at,
        };

        if let Err(e) = context.route_outbox_store.put(&route_id, &item).await {
            let _ = context.payload_store.delete(&item.packet_key).await;
            return Err(RouterActorError::RouteStoreError(e.to_string()));
        }

        Self::spawn_outbox_dispatch(
            item,
            context.node_resolver.clone(),
            context.topic_raft_actor.clone(),
            context.payload_store.clone(),
            context.route_outbox_store.clone(),
            context.metric.clone(),
            context.retry_config.clone(),
        );
        Ok(())
    }

    fn spawn_outbox_dispatch(
        item: RouteOutboxItem,
        node_resolver: Arc<NodeResolver>,
        topic_raft_actor: Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>,
        payload_store: Arc<dyn PayloadStore>,
        route_outbox_store: Arc<JsonRocksDBStore>,
        metric: Arc<Metric>,
        retry_config: RouteRetryConfig,
    ) {
        actix::spawn(async move {
            if let Err(e) = Self::dispatch_outbox_item(
                item,
                node_resolver,
                topic_raft_actor,
                payload_store,
                route_outbox_store,
                metric,
                retry_config,
            )
            .await
            {
                warn!("dispatch durable route outbox item failed: {}", e);
            }
        });
    }

    async fn dispatch_outbox_item(
        mut item: RouteOutboxItem,
        node_resolver: Arc<NodeResolver>,
        topic_raft_actor: Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>,
        payload_store: Arc<dyn PayloadStore>,
        route_outbox_store: Arc<JsonRocksDBStore>,
        metric: Arc<Metric>,
        retry_config: RouteRetryConfig,
    ) -> Result<(), RouterActorError> {
        let now = Self::now_secs();
        if item.expiry_at <= now || item.attempts >= retry_config.max_retry_count {
            metric.increase_messages_dropped();
            let _ = route_outbox_store.delete(&item.route_id).await;
            let _ = payload_store.delete(&item.packet_key).await;
            return Ok(());
        }

        if item.retry_at > now {
            return Ok(());
        }

        let Some(dest_node) = node_resolver
            .get_node(item.dest_node_id, &topic_raft_actor)
            .await
        else {
            item.attempts += 1;
            item.retry_at = now + retry_config.retry_interval_seconds;
            route_outbox_store
                .put(&item.route_id, &item)
                .await
                .map_err(|e| RouterActorError::RouteStoreError(e.to_string()))?;
            return Err(RouterActorError::GRPC(format!(
                "node {} not found",
                item.dest_node_id
            )));
        };

        let packet_bytes = payload_store
            .get(&item.packet_key)
            .await
            .map_err(|e| RouterActorError::PayloadStoreError(e.to_string()))?
            .ok_or_else(|| {
                RouterActorError::PayloadStoreError(format!(
                    "route payload {} not found",
                    item.packet_key
                ))
            })?;
        let packet = Self::deserialize_packet(&packet_bytes).await?;
        let request = crate::protobuf::RoutePacketRequest {
            tenant_id: item.tenant_id.clone(),
            payload: serde_json::to_string(&packet)
                .map_err(|e| RouterActorError::SerializationError(e.to_string()))?,
            route_id: item.route_id.clone(),
            source_node_id: item.source_node_id,
            target_client_id: item.target_client_id.clone(),
            target_qos: item.target_qos as u32,
            expiry_at: item.expiry_at,
        };

        match Self::route_to_other_node(&dest_node.rpc_addr, request).await {
            Ok(()) => {
                route_outbox_store
                    .delete(&item.route_id)
                    .await
                    .map_err(|e| RouterActorError::RouteStoreError(e.to_string()))?;
                let _ = payload_store.delete(&item.packet_key).await;
                Ok(())
            }
            Err(err) => {
                item.attempts += 1;
                item.retry_at = now + retry_config.retry_interval_seconds;
                route_outbox_store
                    .put(&item.route_id, &item)
                    .await
                    .map_err(|e| RouterActorError::RouteStoreError(e.to_string()))?;
                Err(err)
            }
        }
    }

    fn dispatch_best_effort_remote_route(
        node_resolver: Arc<NodeResolver>,
        topic_raft_actor: Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>,
        tenant_id: String,
        dest_node_id: NodeId,
        target_client_id: String,
        target_qos: u8,
        packet: MqttPacketV3,
    ) {
        actix::spawn(async move {
            let Some(dest_node) = node_resolver
                .get_node(dest_node_id, &topic_raft_actor)
                .await
            else {
                warn!("No node found with id: {}", dest_node_id);
                return;
            };

            let request = crate::protobuf::RoutePacketRequest {
                tenant_id,
                payload: match serde_json::to_string(&packet) {
                    Ok(payload) => payload,
                    Err(e) => {
                        warn!("Failed to serialize best-effort route packet: {}", e);
                        return;
                    }
                },
                route_id: String::new(),
                source_node_id: 0,
                target_client_id,
                target_qos: target_qos as u32,
                expiry_at: 0,
            };

            if let Err(e) = Self::route_to_other_node(&dest_node.rpc_addr, request).await {
                warn!("best-effort remote route failed: {}", e);
            }
        });
    }

    async fn accept_route_from_other_node(
        route_inbox_store: Arc<JsonRocksDBStore>,
        payload_store: Arc<dyn PayloadStore>,
        session_registry: SessionRegistry,
        metric: Arc<Metric>,
        retry_config: RouteRetryConfig,
        msg: RouteFromOtherNode,
    ) -> Result<(), RouterActorError> {
        if msg.route_id.is_empty() {
            return Self::route_to_local_session_durable(
                &msg.tenant_id,
                &msg.target_client_id,
                msg.packet,
                session_registry,
            )
            .await;
        }

        if route_inbox_store
            .get::<RouteInboxItem>(&msg.route_id)
            .await
            .map_err(|e| RouterActorError::RouteStoreError(e.to_string()))?
            .is_some()
        {
            return Ok(());
        }

        let packet_key = Self::build_packet_key("route-inbox", &msg.route_id);
        let packet_bytes = Self::serialize_packet(&msg.packet).await?;
        payload_store
            .put(&packet_key, bytes::Bytes::from(packet_bytes))
            .await
            .map_err(|e| RouterActorError::PayloadStoreError(e.to_string()))?;

        let now = Self::now_secs();
        let item = RouteInboxItem {
            route_id: msg.route_id.clone(),
            tenant_id: msg.tenant_id,
            target_client_id: msg.target_client_id,
            packet_key,
            retry_at: now,
            attempts: 0,
            expiry_at: if msg.expiry_at > 0 {
                msg.expiry_at
            } else {
                now + retry_config.message_ttl_seconds
            },
            status: RouteInboxStatus::Pending,
            completed_at: None,
        };

        route_inbox_store
            .put(&item.route_id, &item)
            .await
            .map_err(|e| RouterActorError::RouteStoreError(e.to_string()))?;

        Self::spawn_inbox_processing(
            item,
            payload_store,
            route_inbox_store,
            session_registry,
            metric,
            retry_config,
        );
        Ok(())
    }

    fn spawn_inbox_processing(
        item: RouteInboxItem,
        payload_store: Arc<dyn PayloadStore>,
        route_inbox_store: Arc<JsonRocksDBStore>,
        session_registry: SessionRegistry,
        metric: Arc<Metric>,
        retry_config: RouteRetryConfig,
    ) {
        actix::spawn(async move {
            if let Err(e) = Self::process_inbox_item(
                item,
                payload_store,
                route_inbox_store,
                session_registry,
                metric,
                retry_config,
            )
            .await
            {
                warn!("process route inbox item failed: {}", e);
            }
        });
    }

    async fn process_inbox_item(
        mut item: RouteInboxItem,
        payload_store: Arc<dyn PayloadStore>,
        route_inbox_store: Arc<JsonRocksDBStore>,
        session_registry: SessionRegistry,
        metric: Arc<Metric>,
        retry_config: RouteRetryConfig,
    ) -> Result<(), RouterActorError> {
        if item.status == RouteInboxStatus::Completed {
            return Ok(());
        }

        let now = Self::now_secs();
        if item.expiry_at <= now || item.attempts >= retry_config.max_retry_count {
            metric.increase_messages_dropped();
            item.status = RouteInboxStatus::Completed;
            item.completed_at = Some(now);
            route_inbox_store
                .put(&item.route_id, &item)
                .await
                .map_err(|e| RouterActorError::RouteStoreError(e.to_string()))?;
            let _ = payload_store.delete(&item.packet_key).await;
            return Ok(());
        }

        if item.retry_at > now {
            return Ok(());
        }

        let packet_bytes = payload_store
            .get(&item.packet_key)
            .await
            .map_err(|e| RouterActorError::PayloadStoreError(e.to_string()))?
            .ok_or_else(|| {
                RouterActorError::PayloadStoreError(format!(
                    "route payload {} not found",
                    item.packet_key
                ))
            })?;
        let packet = Self::deserialize_packet(&packet_bytes).await?;

        match Self::route_to_local_session_durable(
            &item.tenant_id,
            &item.target_client_id,
            packet,
            session_registry,
        )
        .await
        {
            Ok(()) => {
                item.status = RouteInboxStatus::Completed;
                item.completed_at = Some(now);
                route_inbox_store
                    .put(&item.route_id, &item)
                    .await
                    .map_err(|e| RouterActorError::RouteStoreError(e.to_string()))?;
                let _ = payload_store.delete(&item.packet_key).await;
                Ok(())
            }
            Err(err) => {
                item.attempts += 1;
                item.retry_at = now + retry_config.retry_interval_seconds;
                route_inbox_store
                    .put(&item.route_id, &item)
                    .await
                    .map_err(|e| RouterActorError::RouteStoreError(e.to_string()))?;
                Err(err)
            }
        }
    }

    fn process_route_outbox(&self) {
        let node_resolver = self.node_resolver.clone();
        let topic_raft_actor = self
            .topic_raft_actor
            .as_ref()
            .expect("topic raft actor not set")
            .clone();
        let payload_store = self.payload_store.clone();
        let route_outbox_store = self.route_outbox_store.clone();
        let metric = self.metric.clone();
        let retry_config = self.retry_config.clone();

        actix::spawn(async move {
            let rows = match route_outbox_store.scan::<RouteOutboxItem>().await {
                Ok(rows) => rows,
                Err(e) => {
                    warn!("scan route outbox failed: {}", e);
                    return;
                }
            };

            for (_, item) in rows {
                let _ = Self::dispatch_outbox_item(
                    item,
                    node_resolver.clone(),
                    topic_raft_actor.clone(),
                    payload_store.clone(),
                    route_outbox_store.clone(),
                    metric.clone(),
                    retry_config.clone(),
                )
                .await;
            }
        });
    }

    fn process_route_inbox(&self) {
        let payload_store = self.payload_store.clone();
        let route_inbox_store = self.route_inbox_store.clone();
        let session_registry = self.session_registry.clone();
        let metric = self.metric.clone();
        let retry_config = self.retry_config.clone();

        actix::spawn(async move {
            let rows = match route_inbox_store.scan::<RouteInboxItem>().await {
                Ok(rows) => rows,
                Err(e) => {
                    warn!("scan route inbox failed: {}", e);
                    return;
                }
            };

            for (_, item) in rows {
                let _ = Self::process_inbox_item(
                    item,
                    payload_store.clone(),
                    route_inbox_store.clone(),
                    session_registry.clone(),
                    metric.clone(),
                    retry_config.clone(),
                )
                .await;
            }
        });
    }

    fn cleanup_completed_inbox_items(&self) {
        let route_inbox_store = self.route_inbox_store.clone();
        let retention_secs = self.retry_config.message_ttl_seconds;

        actix::spawn(async move {
            let now = Self::now_secs();
            let rows = match route_inbox_store.scan::<RouteInboxItem>().await {
                Ok(rows) => rows,
                Err(e) => {
                    warn!("scan route inbox for cleanup failed: {}", e);
                    return;
                }
            };

            for (_, item) in rows {
                if item.status == RouteInboxStatus::Completed
                    && item
                        .completed_at
                        .map(|completed_at| now.saturating_sub(completed_at) >= retention_secs)
                        .unwrap_or(false)
                {
                    let _ = route_inbox_store.delete(&item.route_id).await;
                }
            }
        });
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), RouterActorError>")]
pub struct RoutePacket {
    pub tenant_id: String,
    pub packet: MqttPacketV3,
}

impl Handler<RoutePacket> for RouterActor {
    type Result = ResponseActFuture<Self, Result<(), RouterActorError>>;

    fn handle(&mut self, msg: RoutePacket, _ctx: &mut Self::Context) -> Self::Result {
        let context = RouteContext {
            current_node_id: self.current_node_id,
            node_resolver: self.node_resolver.clone(),
            topic_raft_actor: self
                .topic_raft_actor
                .as_ref()
                .expect("topic raft actor not set")
                .clone(),
            local_topic_storage: self.local_topic_storage.clone(),
            local_session_actor_map_storage: self.local_session_actor_map_storage.clone(),
            session_registry: self.session_registry.clone(),
            payload_store: self.payload_store.clone(),
            route_outbox_store: self.route_outbox_store.clone(),
            retry_config: self.retry_config.clone(),
            metric: self.metric.clone(),
        };

        Box::pin(
            async move {
                Self::route(context, &msg.tenant_id, &msg.packet).await?;
                Ok(())
            }
            .into_actor(self),
        )
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), RouterActorError>")]
pub struct RouteFromOtherNode {
    pub tenant_id: String,
    pub packet: MqttPacketV3,
    pub route_id: String,
    pub source_node_id: NodeId,
    pub target_client_id: String,
    pub target_qos: u8,
    pub expiry_at: u64,
}

impl Handler<RouteFromOtherNode> for RouterActor {
    type Result = ResponseActFuture<Self, Result<(), RouterActorError>>;

    fn handle(&mut self, msg: RouteFromOtherNode, _ctx: &mut Self::Context) -> Self::Result {
        if msg.target_client_id.is_empty() {
            let topic_raft_actor = self
                .topic_raft_actor
                .as_ref()
                .expect("topic raft actor not set")
                .clone();
            let session_registry = self.session_registry.clone();
            return Box::pin(
                async move {
                    Self::publish_to_local_subscribers(
                        &msg.tenant_id,
                        &msg.packet,
                        topic_raft_actor,
                        session_registry,
                    )
                    .await?;
                    Ok(())
                }
                .into_actor(self),
            );
        }

        let route_inbox_store = self.route_inbox_store.clone();
        let payload_store = self.payload_store.clone();
        let session_registry = self.session_registry.clone();
        let metric = self.metric.clone();
        let retry_config = self.retry_config.clone();

        Box::pin(
            async move {
                Self::accept_route_from_other_node(
                    route_inbox_store,
                    payload_store,
                    session_registry,
                    metric,
                    retry_config,
                    msg,
                )
                .await?;
                Ok(())
            }
            .into_actor(self),
        )
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), RouterActorError>")]
pub struct RoutePacketToAllTenants {
    pub packet: MqttPacketV3,
}

impl Handler<RoutePacketToAllTenants> for RouterActor {
    type Result = ResponseActFuture<Self, Result<(), RouterActorError>>;

    fn handle(&mut self, msg: RoutePacketToAllTenants, _ctx: &mut Self::Context) -> Self::Result {
        let session_manager_actor_addr = self.session_manager_actor.clone();
        let topic_raft_actor_addr = self
            .topic_raft_actor
            .as_ref()
            .expect("topic raft actor not set")
            .clone();
        let session_registry = self.session_registry.clone();

        Box::pin(
            async move {
                let tenant_ids = session_manager_actor_addr
                    .send(session_manager_actor::GetAllTenantIds {})
                    .await?;
                for tenant_id in tenant_ids {
                    Self::publish_to_local_subscribers(
                        &tenant_id,
                        &msg.packet,
                        topic_raft_actor_addr.clone(),
                        session_registry.clone(),
                    )
                    .await?;
                }
                Ok(())
            }
            .into_actor(self),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use dashmap::DashMap;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};
    use yedmq_mqtt::v3::publish::PublishPacketBuilder;

    use crate::metric::Metric;
    use crate::raft::payload::RocksDBPayloadStore;
    use crate::raft::topic::topic_raft_actor::TopicRaftActor;
    use crate::session::session_actor::{
        ActivityState, GetSessionInfo, SessionActorError, SessionInfo,
    };
    use crate::session::session_actor_map_storage::SessionVersion;
    use crate::session::session_registry::SessionActorRecipientWrapper;

    #[derive(Clone, Default)]
    struct DeliveredPackets {
        packets: Arc<Mutex<Vec<MqttPacketV3>>>,
    }

    impl DeliveredPackets {
        fn len(&self) -> usize {
            self.packets.lock().unwrap().len()
        }

        fn push(&self, packet: MqttPacketV3) {
            self.packets.lock().unwrap().push(packet);
        }
    }

    struct TestRouteSessionActor {
        delivered: DeliveredPackets,
    }

    impl Actor for TestRouteSessionActor {
        type Context = Context<Self>;
    }

    impl Handler<AcceptRoutedPublish> for TestRouteSessionActor {
        type Result = Result<(), SessionActorError>;

        fn handle(&mut self, msg: AcceptRoutedPublish, _ctx: &mut Self::Context) -> Self::Result {
            self.delivered.push(msg.packet);
            Ok(())
        }
    }

    impl Handler<SessionActorMessage> for TestRouteSessionActor {
        type Result = ();

        fn handle(&mut self, _msg: SessionActorMessage, _ctx: &mut Self::Context) -> Self::Result {}
    }

    impl Handler<GetSessionInfo> for TestRouteSessionActor {
        type Result = SessionInfo;

        fn handle(&mut self, _msg: GetSessionInfo, _ctx: &mut Self::Context) -> Self::Result {
            SessionInfo {
                tenant_identifier: "tenant-a".to_string(),
                client_identifier: "client-a".to_string(),
                subscription_topics: vec![],
                session_state: ActivityState::Active,
                created_at: 0,
                connected_at: None,
                disconnected_at: None,
                messages_received: 0,
                messages_sent: 0,
                connected: true,
                ip_address: None,
            }
        }
    }

    fn build_publish_packet(topic: &str, qos: u8, payload: &[u8]) -> MqttPacketV3 {
        MqttPacketV3::Publish(
            PublishPacketBuilder::new(topic.to_string(), Bytes::copy_from_slice(payload))
                .qos(qos)
                .packet_identifier(42)
                .build(),
        )
    }

    fn build_test_settings(temp_dir: &TempDir, nodes: Vec<crate::settings::Node>) -> Arc<Settings> {
        let mut settings = Settings::default();
        settings.cluster.node_id = 1001;
        settings.cluster.store_dir = temp_dir.path().join("store").to_string_lossy().to_string();
        settings.cluster.nodes = nodes;
        Arc::new(settings)
    }

    fn build_test_registry(
        tenant_id: &str,
        client_id: &str,
    ) -> (SessionRegistry, DeliveredPackets) {
        let registry = SessionRegistry::new();
        let delivered = DeliveredPackets::default();
        let actor = TestRouteSessionActor {
            delivered: delivered.clone(),
        }
        .start();

        let tenant_map = Arc::new(DashMap::new());
        tenant_map.insert(
            client_id.to_string(),
            SessionActorRecipientWrapper {
                session_actor_message_recipient: actor.clone().recipient(),
                accept_routed_publish_recipient: actor.clone().recipient(),
                get_session_info_recipient: actor.recipient(),
                session_version: SessionVersion::new(1, 1001),
            },
        );
        registry
            .get_inner()
            .insert(tenant_id.to_string(), tenant_map);

        (registry, delivered)
    }

    async fn wait_until<F>(mut predicate: F)
    where
        F: FnMut() -> bool,
    {
        for _ in 0..20 {
            if predicate() {
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
        assert!(predicate(), "condition was not met in time");
    }

    #[actix::test]
    async fn test_accept_route_from_other_node_deduplicates_route_id() {
        let temp_dir = TempDir::new().unwrap();
        let payload_store =
            Arc::new(RocksDBPayloadStore::new(temp_dir.path().join("payload")).unwrap());
        let route_inbox_store =
            Arc::new(JsonRocksDBStore::new(temp_dir.path().join("route_inbox")).unwrap());
        let (session_registry, delivered) = build_test_registry("tenant-a", "client-a");
        let metric = Arc::new(Metric::new());
        let retry_config = RouteRetryConfig::default();
        let packet = build_publish_packet("test/router/dedupe", 1, b"hello");

        let msg = RouteFromOtherNode {
            tenant_id: "tenant-a".to_string(),
            packet: packet.clone(),
            route_id: "route-1".to_string(),
            source_node_id: 2002,
            target_client_id: "client-a".to_string(),
            target_qos: 1,
            expiry_at: RouterActor::now_secs() + 60,
        };

        RouterActor::accept_route_from_other_node(
            route_inbox_store.clone(),
            payload_store.clone(),
            session_registry.clone(),
            metric.clone(),
            retry_config.clone(),
            msg,
        )
        .await
        .unwrap();

        wait_until(|| delivered.len() == 1).await;

        let stored_item = route_inbox_store
            .get::<RouteInboxItem>("route-1")
            .await
            .unwrap()
            .expect("inbox item should exist");
        assert_eq!(stored_item.status, RouteInboxStatus::Completed);

        RouterActor::accept_route_from_other_node(
            route_inbox_store.clone(),
            payload_store.clone(),
            session_registry,
            metric,
            retry_config,
            RouteFromOtherNode {
                tenant_id: "tenant-a".to_string(),
                packet,
                route_id: "route-1".to_string(),
                source_node_id: 2002,
                target_client_id: "client-a".to_string(),
                target_qos: 1,
                expiry_at: RouterActor::now_secs() + 60,
            },
        )
        .await
        .unwrap();

        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            delivered.len(),
            1,
            "duplicate route id should not re-deliver"
        );
    }

    #[actix::test]
    async fn test_process_inbox_item_recovers_pending_delivery_after_restart() {
        let temp_dir = TempDir::new().unwrap();
        let payload_store =
            Arc::new(RocksDBPayloadStore::new(temp_dir.path().join("payload")).unwrap());
        let route_inbox_store =
            Arc::new(JsonRocksDBStore::new(temp_dir.path().join("route_inbox")).unwrap());
        let (session_registry, delivered) = build_test_registry("tenant-a", "client-a");
        let metric = Arc::new(Metric::new());
        let retry_config = RouteRetryConfig::default();
        let route_id = "route-replay";
        let packet_key = RouterActor::build_packet_key("route-inbox", route_id);
        let packet = build_publish_packet("test/router/replay", 1, b"resume");

        payload_store
            .put(
                &packet_key,
                Bytes::from(RouterActor::serialize_packet(&packet).await.unwrap()),
            )
            .await
            .unwrap();

        let item = RouteInboxItem {
            route_id: route_id.to_string(),
            tenant_id: "tenant-a".to_string(),
            target_client_id: "client-a".to_string(),
            packet_key: packet_key.clone(),
            retry_at: RouterActor::now_secs(),
            attempts: 0,
            expiry_at: RouterActor::now_secs() + 60,
            status: RouteInboxStatus::Pending,
            completed_at: None,
        };
        route_inbox_store.put(route_id, &item).await.unwrap();

        RouterActor::process_inbox_item(
            item,
            payload_store.clone(),
            route_inbox_store.clone(),
            session_registry,
            metric,
            retry_config,
        )
        .await
        .unwrap();

        assert_eq!(delivered.len(), 1, "pending inbox item should be replayed");
        let stored_item = route_inbox_store
            .get::<RouteInboxItem>(route_id)
            .await
            .unwrap()
            .expect("completed inbox item should still exist");
        assert_eq!(stored_item.status, RouteInboxStatus::Completed);
        assert!(
            payload_store.get(&packet_key).await.unwrap().is_none(),
            "payload should be deleted once inbox delivery is completed"
        );
    }

    #[actix::test]
    async fn test_dispatch_outbox_item_keeps_payload_for_retry_on_failure() {
        let temp_dir = TempDir::new().unwrap();
        let payload_store =
            Arc::new(RocksDBPayloadStore::new(temp_dir.path().join("payload")).unwrap());
        let route_outbox_store =
            Arc::new(JsonRocksDBStore::new(temp_dir.path().join("route_outbox")).unwrap());
        let metric = Arc::new(Metric::new());
        let retry_config = RouteRetryConfig::default();
        let route_id = "route-outbox-retry";
        let packet_key = RouterActor::build_packet_key("route-outbox", route_id);
        let packet = build_publish_packet("test/router/outbox_retry", 1, b"retry");

        payload_store
            .put(
                &packet_key,
                Bytes::from(RouterActor::serialize_packet(&packet).await.unwrap()),
            )
            .await
            .unwrap();

        let item = RouteOutboxItem {
            route_id: route_id.to_string(),
            tenant_id: "tenant-a".to_string(),
            source_node_id: 1001,
            dest_node_id: 2002,
            target_client_id: "client-a".to_string(),
            target_qos: 1,
            packet_key: packet_key.clone(),
            retry_at: RouterActor::now_secs(),
            attempts: 0,
            expiry_at: RouterActor::now_secs() + 60,
        };
        route_outbox_store.put(route_id, &item).await.unwrap();

        let settings = build_test_settings(
            &temp_dir,
            vec![crate::settings::Node {
                id: 2002,
                rpc_address: "127.0.0.1:1".to_string(),
                api_address: "127.0.0.1:2".to_string(),
            }],
        );
        let node_resolver = Arc::new(NodeResolver::new(settings));
        let topic_raft_actor = TopicRaftActor::from_registry();

        let result = RouterActor::dispatch_outbox_item(
            item.clone(),
            node_resolver,
            topic_raft_actor,
            payload_store.clone(),
            route_outbox_store.clone(),
            metric,
            retry_config.clone(),
        )
        .await;

        assert!(
            result.is_err(),
            "failed remote route should be retried later"
        );

        let stored_item = route_outbox_store
            .get::<RouteOutboxItem>(route_id)
            .await
            .unwrap()
            .expect("outbox item should still exist");
        assert_eq!(stored_item.attempts, 1);
        assert!(
            stored_item.retry_at > item.retry_at,
            "retry_at should move forward after a failed dispatch"
        );
        assert!(
            payload_store.get(&packet_key).await.unwrap().is_some(),
            "payload must be kept while the outbox item is retryable"
        );
    }

    #[actix::test]
    async fn test_dispatch_outbox_item_drops_expired_payload() {
        let temp_dir = TempDir::new().unwrap();
        let payload_store =
            Arc::new(RocksDBPayloadStore::new(temp_dir.path().join("payload")).unwrap());
        let route_outbox_store =
            Arc::new(JsonRocksDBStore::new(temp_dir.path().join("route_outbox")).unwrap());
        let metric = Arc::new(Metric::new());
        let retry_config = RouteRetryConfig::default();
        let route_id = "route-outbox-expired";
        let packet_key = RouterActor::build_packet_key("route-outbox", route_id);
        let packet = build_publish_packet("test/router/outbox_expired", 1, b"drop");

        payload_store
            .put(
                &packet_key,
                Bytes::from(RouterActor::serialize_packet(&packet).await.unwrap()),
            )
            .await
            .unwrap();

        let item = RouteOutboxItem {
            route_id: route_id.to_string(),
            tenant_id: "tenant-a".to_string(),
            source_node_id: 1001,
            dest_node_id: 2002,
            target_client_id: "client-a".to_string(),
            target_qos: 1,
            packet_key: packet_key.clone(),
            retry_at: RouterActor::now_secs(),
            attempts: retry_config.max_retry_count,
            expiry_at: RouterActor::now_secs() - 1,
        };
        route_outbox_store.put(route_id, &item).await.unwrap();

        let settings = build_test_settings(
            &temp_dir,
            vec![crate::settings::Node {
                id: 2002,
                rpc_address: "127.0.0.1:1".to_string(),
                api_address: "127.0.0.1:2".to_string(),
            }],
        );
        let node_resolver = Arc::new(NodeResolver::new(settings));
        let topic_raft_actor = TopicRaftActor::from_registry();

        RouterActor::dispatch_outbox_item(
            item,
            node_resolver,
            topic_raft_actor,
            payload_store.clone(),
            route_outbox_store.clone(),
            metric.clone(),
            retry_config,
        )
        .await
        .unwrap();

        assert!(
            route_outbox_store
                .get::<RouteOutboxItem>(route_id)
                .await
                .unwrap()
                .is_none(),
            "expired outbox item should be removed"
        );
        assert!(
            payload_store.get(&packet_key).await.unwrap().is_none(),
            "expired outbox payload should be removed together"
        );
        assert_eq!(
            metric
                .messages_dropped
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "dropping an expired outbox item should increment the dropped metric"
        );
    }

    #[test]
    fn test_should_route_durably_only_for_qos_gt_zero() {
        let qos0_packet = build_publish_packet("test/router/qos0", 0, b"qos0");
        let qos1_packet = build_publish_packet("test/router/qos1", 1, b"qos1");

        assert!(
            !RouterActor::should_route_durably(&qos0_packet),
            "QoS 0 should stay best-effort and avoid durable outbox/inbox"
        );
        assert!(
            RouterActor::should_route_durably(&qos1_packet),
            "QoS 1 should use durable handoff"
        );
    }
}
