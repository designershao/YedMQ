use actix::prelude::*;
use log::{info, warn};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::Request;
use yedmq_mqtt::packet::{Packet, Publish};

use crate::metric::Metric;
use crate::mqtt_message_expiry;
use crate::node_resolver::NodeResolver;
use crate::raft::payload::PayloadStore;
use crate::route_store::JsonRocksDBStore;
use crate::session::session_actor::{AcceptRoutedPublish, SessionActorMessage};
use crate::session::session_actor_map_service::SessionActorMapService;
use crate::session::session_actor_map_storage::SessionActorMapStorage;
use crate::session::session_manager_actor::SessionManagerActor;
use crate::settings::Settings;
use crate::stored_packet::{
    deserialize_stored_packet, serialize_stored_packet, serialize_stored_packet_to_string,
};
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
            max_retry_count: 120,
            retry_interval_seconds: 1,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SharedSubscriptionRouteKey {
    tenant_id: String,
    share_name: String,
    topic_filter: String,
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
    shared_subscription_cursors: std::sync::Arc<
        parking_lot::Mutex<std::collections::HashMap<SharedSubscriptionRouteKey, usize>>,
    >,
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
    shared_subscription_cursors: std::sync::Arc<
        parking_lot::Mutex<std::collections::HashMap<SharedSubscriptionRouteKey, usize>>,
    >,
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
            shared_subscription_cursors: std::sync::Arc::new(parking_lot::Mutex::new(
                std::collections::HashMap::new(),
            )),
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

    fn should_route_durably(packet: &Packet) -> bool {
        matches!(
            packet,
            Packet::Publish(publish_packet) if publish_packet.qos >= 1
        )
    }

    fn adjust_publish_for_subscriber(
        publish_packet: &Publish,
        subscriber_qos: u8,
        retain_as_published: bool,
    ) -> Publish {
        let mut publish_packet = publish_packet.clone();
        if publish_packet.qos >= subscriber_qos {
            if subscriber_qos == 0 && publish_packet.qos > 0 {
                publish_packet.qos = 0;
                publish_packet.packet_identifier = None;
            } else {
                publish_packet.qos = subscriber_qos;
            }
        }
        if !retain_as_published {
            publish_packet.retain = false;
        }
        publish_packet
    }

    async fn serialize_packet(packet: &Packet) -> Result<Vec<u8>, RouterActorError> {
        serialize_stored_packet(packet)
            .map_err(|e| RouterActorError::SerializationError(e.to_string()))
    }

    async fn deserialize_packet(bytes: &[u8]) -> Result<Packet, RouterActorError> {
        deserialize_stored_packet(bytes)
            .map_err(|e| RouterActorError::SerializationError(e.to_string()))
    }

    async fn route_to_other_node(
        dest_addr: &str,
        request: crate::protobuf::RoutePacketRequest,
    ) -> Result<(), RouterActorError> {
        let mut cluster_client =
            crate::rpc::grpc_client::connected_channel(dest_addr, Duration::from_secs(1))
                .await
                .map(ClusterServiceClient::new)
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
        packet: Packet,
        session_registry: SessionRegistry,
    ) -> Result<bool, RouterActorError> {
        if let Some(recipient) = session_registry.get_session(tenant_id, client_id) {
            recipient.do_send(SessionActorMessage::OutboundMessage(packet));
            return Ok(true);
        } else {
            warn!(
                "session {} not found locally, skip best-effort route",
                client_id
            );
        }
        Ok(false)
    }

    async fn route_to_local_session_durable(
        tenant_id: &str,
        client_id: &str,
        packet: Packet,
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
        packet: &Packet,
        topic_raft_actor_addr: Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>,
        session_registry: SessionRegistry,
        shared_subscription_cursors: std::sync::Arc<
            parking_lot::Mutex<std::collections::HashMap<SharedSubscriptionRouteKey, usize>>,
        >,
    ) -> Result<(), RouterActorError> {
        if mqtt_message_expiry::is_packet_expired(packet, mqtt_message_expiry::now_unix_secs()) {
            return Ok(());
        }

        if let Packet::Publish(publish_packet) = packet {
            let topic = &publish_packet.topic_name;
            let res = topic_raft_actor_addr
                .send(crate::raft::topic::topic_raft_actor::GetSubscriptions {
                    tenant_id: tenant_id.to_string(),
                    topic: topic.clone(),
                })
                .await?;

            match res {
                Ok(subscriptions) => {
                    // Split into normal and shared
                    let mut normal_subs = Vec::new();
                    let mut shared_groups: std::collections::HashMap<
                        SharedSubscriptionRouteKey,
                        Vec<crate::raft::topic::topic_raft_actor::SubscriptionInfo>,
                    > = std::collections::HashMap::new();

                    for sub in subscriptions.subscriptions {
                        if let Some(ref share_name) = sub.shared_group {
                            let key = SharedSubscriptionRouteKey {
                                tenant_id: tenant_id.to_string(),
                                share_name: share_name.clone(),
                                topic_filter: sub.topic_filter.clone(),
                            };
                            shared_groups.entry(key).or_default().push(sub);
                        } else {
                            normal_subs.push(sub);
                        }
                    }

                    // Deliver normal subscriptions
                    for item in normal_subs {
                        let adjusted = Self::adjust_publish_for_subscriber(
                            publish_packet,
                            item.qos,
                            item.retain_as_published,
                        );
                        let _ = Self::route_to_local_session(
                            tenant_id,
                            &item.client_identifier,
                            Packet::Publish(adjusted),
                            session_registry.clone(),
                        )?;
                    }

                    // Deliver shared subscriptions (one per group)
                    for (key, candidates) in shared_groups {
                        let mut sorted = candidates;
                        sorted.sort_by(|a, b| a.client_identifier.cmp(&b.client_identifier));

                        let start = {
                            let mut cursors = shared_subscription_cursors.lock();
                            let entry = cursors.entry(key).or_insert(0);
                            let idx = *entry % sorted.len();
                            *entry = idx + 1;
                            idx
                        };

                        // Try selected member first, then fall back to next members
                        for i in 0..sorted.len() {
                            let idx = (start + i) % sorted.len();
                            let selected = &sorted[idx];
                            let adjusted = Self::adjust_publish_for_subscriber(
                                publish_packet,
                                selected.qos,
                                selected.retain_as_published,
                            );
                            if Self::route_to_local_session(
                                tenant_id,
                                &selected.client_identifier,
                                Packet::Publish(adjusted),
                                session_registry.clone(),
                            )? {
                                break;
                            }
                        }
                    }
                    Ok(())
                }
                Err(e) => Err(RouterActorError::TopicRaftError(Box::new(e))),
            }
        } else {
            Ok(())
        }
    }

    async fn deliver_to_subscriber(
        context: &RouteContext,
        tenant_id: &str,
        publish_packet: &Publish,
        item: &crate::raft::topic::topic_raft_actor::SubscriptionInfo,
    ) -> Result<bool, RouterActorError> {
        let session_actor_map = {
            let local = context
                .local_session_actor_map_storage
                .read()
                .get_session_actor_map(tenant_id, &item.client_identifier);
            match local {
                Some(entry) => Some(entry),
                None => SessionActorMapService::from_registry()
                    .get_session_actor_map_linearizable(
                        tenant_id.to_string(),
                        item.client_identifier.clone(),
                    )
                    .await
                    .map_err(|e| RouterActorError::SessionActorMapRaftError(Box::new(e)))?,
            }
        };

        let Some(session_actor_addr) = session_actor_map else {
            return Ok(false);
        };

        let adjusted_packet = Packet::Publish(Self::adjust_publish_for_subscriber(
            publish_packet,
            item.qos,
            item.retain_as_published,
        ));

        if session_actor_addr.node_id != context.current_node_id {
            if Self::should_route_durably(&adjusted_packet) {
                Self::enqueue_durable_remote_route(
                    context,
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
            return Ok(true);
        } else {
            return Self::route_to_local_session(
                tenant_id,
                &item.client_identifier,
                adjusted_packet,
                context.session_registry.clone(),
            );
        }
    }

    async fn route(
        context: RouteContext,
        tenant_id: &str,
        packet: &Packet,
        source_client_identifier: Option<&str>,
    ) -> Result<(), RouterActorError> {
        if mqtt_message_expiry::is_packet_expired(packet, Self::now_secs()) {
            context.metric.increase_messages_dropped();
            return Ok(());
        }

        if let Packet::Publish(publish_packet) = packet {
            let topic = &publish_packet.topic_name;

            let mut subscriptions = {
                let local_topic_storage = context.local_topic_storage.read();
                local_topic_storage
                    .get_subscriptions(tenant_id.to_string(), topic.clone())?
                    .iter()
                    .map(|x| crate::raft::topic::topic_raft_actor::SubscriptionInfo {
                        client_identifier: x.client_identifier.clone(),
                        qos: x.qos,
                        no_local: x.no_local,
                        retain_as_published: x.retain_as_published,
                        shared_group: x.shared_group.clone(),
                        topic_filter: x.topic_filter.clone(),
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

            // Split into normal and shared subscriptions
            let mut normal_subs = Vec::new();
            let mut shared_groups: std::collections::HashMap<
                SharedSubscriptionRouteKey,
                Vec<&crate::raft::topic::topic_raft_actor::SubscriptionInfo>,
            > = std::collections::HashMap::new();

            for sub in &subscriptions {
                if let Some(ref share_name) = sub.shared_group {
                    let key = SharedSubscriptionRouteKey {
                        tenant_id: tenant_id.to_string(),
                        share_name: share_name.clone(),
                        topic_filter: sub.topic_filter.clone(),
                    };
                    shared_groups.entry(key).or_default().push(sub);
                } else {
                    normal_subs.push(sub);
                }
            }

            // Deliver normal subscriptions (fan-out)
            for item in &normal_subs {
                if item.no_local
                    && source_client_identifier
                        .map(|source| source == item.client_identifier)
                        .unwrap_or(false)
                {
                    continue;
                }
                Self::deliver_to_subscriber(&context, tenant_id, publish_packet, item).await?;
            }

            // Deliver shared subscriptions (one per group)
            for (key, candidates) in shared_groups {
                // Filter out No Local candidates
                let eligible: Vec<&&crate::raft::topic::topic_raft_actor::SubscriptionInfo> =
                    candidates
                        .iter()
                        .filter(|item| {
                            !(item.no_local
                                && source_client_identifier
                                    .map(|source| source == item.client_identifier)
                                    .unwrap_or(false))
                        })
                        .collect();

                if eligible.is_empty() {
                    continue;
                }

                // Sort by client_identifier for deterministic selection
                let mut sorted: Vec<&&crate::raft::topic::topic_raft_actor::SubscriptionInfo> =
                    eligible;
                sorted.sort_by(|a, b| a.client_identifier.cmp(&b.client_identifier));

                // Round-robin selection
                let start = {
                    let mut cursors = context.shared_subscription_cursors.lock();
                    let entry = cursors.entry(key.clone()).or_insert(0);
                    let idx = *entry % sorted.len();
                    *entry = idx + 1;
                    idx
                };

                // Try selected member first, then fall back to next members
                let mut delivered = false;
                for i in 0..sorted.len() {
                    let idx = (start + i) % sorted.len();
                    if Self::deliver_to_subscriber(&context, tenant_id, publish_packet, sorted[idx])
                        .await?
                    {
                        delivered = true;
                        break;
                    }
                }
                if !delivered {
                    context.metric.increase_messages_dropped();
                }
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
        packet: Packet,
    ) -> Result<(), RouterActorError> {
        let route_id = uuid::Uuid::new_v4().to_string();
        let packet_key = Self::build_packet_key("route-outbox", &route_id);
        let now = Self::now_secs();
        let expiry_at = mqtt_message_expiry::route_expiry_at(
            &packet,
            context.retry_config.message_ttl_seconds,
            now,
        );
        if expiry_at <= now {
            context.metric.increase_messages_dropped();
            return Ok(());
        }
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
        if mqtt_message_expiry::is_packet_expired(&packet, now) {
            metric.increase_messages_dropped();
            let _ = route_outbox_store.delete(&item.route_id).await;
            let _ = payload_store.delete(&item.packet_key).await;
            return Ok(());
        }
        let request = crate::protobuf::RoutePacketRequest {
            tenant_id: item.tenant_id.clone(),
            payload: serialize_stored_packet_to_string(&packet)
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
                warn!(
                    "durable route outbox dispatch failed: route_id={}, dest_node_id={}, dest_rpc_addr={}, attempt={}, retry_at={}, error={}",
                    item.route_id,
                    item.dest_node_id,
                    dest_node.rpc_addr,
                    item.attempts,
                    item.retry_at,
                    err
                );
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
        packet: Packet,
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
                payload: match serialize_stored_packet_to_string(&packet) {
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
        if mqtt_message_expiry::is_packet_expired(&msg.packet, Self::now_secs()) {
            metric.increase_messages_dropped();
            return Ok(());
        }

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
        if mqtt_message_expiry::is_packet_expired(&packet, now) {
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
    pub packet: Packet,
    pub source_client_identifier: Option<String>,
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
            shared_subscription_cursors: self.shared_subscription_cursors.clone(),
        };

        Box::pin(
            async move {
                Self::route(
                    context,
                    &msg.tenant_id,
                    &msg.packet,
                    msg.source_client_identifier.as_deref(),
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
pub struct RouteFromOtherNode {
    pub tenant_id: String,
    pub packet: Packet,
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
            let cursors = self.shared_subscription_cursors.clone();
            return Box::pin(
                async move {
                    Self::publish_to_local_subscribers(
                        &msg.tenant_id,
                        &msg.packet,
                        topic_raft_actor,
                        session_registry,
                        cursors,
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
    pub packet: Packet,
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
        let cursors = self.shared_subscription_cursors.clone();

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
                        cursors.clone(),
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
    use yedmq_mqtt::{v3::publish::PublishPacketBuilder, MqttPacketV3};

    use crate::metric::Metric;
    use crate::raft::payload::RocksDBPayloadStore;
    use crate::raft::topic::topic_raft_actor::TopicRaftActor;
    use crate::session::session_actor::{
        ActivityState, GetSessionInfo, SessionActorError, SessionInfo,
    };
    use crate::session::session_actor_map_storage::{SessionActorMapStorage, SessionVersion};
    use crate::session::session_registry::SessionActorRecipientWrapper;
    use crate::topic::topic_storage::TopicStorage;

    #[derive(Clone, Default)]
    struct DeliveredPackets {
        packets: Arc<Mutex<Vec<Packet>>>,
    }

    impl DeliveredPackets {
        fn len(&self) -> usize {
            self.packets.lock().unwrap().len()
        }

        fn push(&self, packet: Packet) {
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

        fn handle(&mut self, msg: SessionActorMessage, _ctx: &mut Self::Context) -> Self::Result {
            if let SessionActorMessage::OutboundMessage(packet) = msg {
                self.delivered.push(packet);
            }
        }
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

    fn build_publish_packet(topic: &str, qos: u8, payload: &[u8]) -> Packet {
        Packet::from(MqttPacketV3::Publish(
            PublishPacketBuilder::new(topic.to_string(), Bytes::copy_from_slice(payload))
                .qos(qos)
                .packet_identifier(42)
                .build(),
        ))
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

    /// Helper: register multiple clients in one tenant, each with its own DeliveredPackets.
    fn build_multi_client_registry(
        tenant_id: &str,
        client_ids: &[&str],
    ) -> (SessionRegistry, Vec<DeliveredPackets>) {
        let registry = SessionRegistry::new();
        let tenant_map = Arc::new(DashMap::new());
        let mut all_delivered = Vec::new();

        for client_id in client_ids {
            let delivered = DeliveredPackets::default();
            let actor = TestRouteSessionActor {
                delivered: delivered.clone(),
            }
            .start();
            tenant_map.insert(
                client_id.to_string(),
                SessionActorRecipientWrapper {
                    session_actor_message_recipient: actor.clone().recipient(),
                    accept_routed_publish_recipient: actor.clone().recipient(),
                    get_session_info_recipient: actor.recipient(),
                    session_version: SessionVersion::new(1, 1001),
                },
            );
            all_delivered.push(delivered);
        }

        registry
            .get_inner()
            .insert(tenant_id.to_string(), tenant_map);

        (registry, all_delivered)
    }

    /// Helper: build a RouteContext with a pre-populated TopicStorage.
    /// Registers all client_ids in SessionActorMapStorage as local sessions.
    fn build_route_context(
        tenant_id: &str,
        session_registry: SessionRegistry,
        topic_storage: Arc<RwLock<TopicStorage>>,
        client_ids: &[&str],
    ) -> RouteContext {
        let temp_dir = TempDir::new().unwrap();
        let payload_store =
            Arc::new(RocksDBPayloadStore::new(temp_dir.path().join("payload")).unwrap());
        let route_outbox_store =
            Arc::new(JsonRocksDBStore::new(temp_dir.path().join("route_outbox")).unwrap());
        let settings = build_test_settings(&temp_dir, vec![]);
        let node_resolver = Arc::new(NodeResolver::new(settings));
        let topic_raft_actor = TopicRaftActor::from_registry();

        let mut session_map_storage = SessionActorMapStorage::new();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for client_id in client_ids {
            session_map_storage
                .register_session_actor(
                    tenant_id.to_string(),
                    client_id.to_string(),
                    1001,
                    &SessionVersion::new(1, 1001),
                    now + 3600,
                )
                .unwrap();
        }

        RouteContext {
            current_node_id: 1001,
            node_resolver,
            topic_raft_actor,
            local_topic_storage: topic_storage,
            local_session_actor_map_storage: Arc::new(RwLock::new(session_map_storage)),
            session_registry,
            payload_store,
            route_outbox_store,
            retry_config: RouteRetryConfig::default(),
            metric: Arc::new(Metric::new()),
            shared_subscription_cursors: Arc::new(parking_lot::Mutex::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    #[actix::test]
    async fn test_shared_subscription_exactly_one_delivery() {
        let (registry, delivered) =
            build_multi_client_registry("tenant-a", &["client-a", "client-b"]);
        let topic_storage = Arc::new(RwLock::new(TopicStorage::new()));
        topic_storage.read().create_tenant(&"tenant-a".to_string());
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "client-a".to_string(),
                "$share/grp/sensors/temp".to_string(),
                1,
                false,
                false,
            )
            .unwrap();
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "client-b".to_string(),
                "$share/grp/sensors/temp".to_string(),
                1,
                false,
                false,
            )
            .unwrap();

        let context = build_route_context(
            "tenant-a",
            registry,
            topic_storage,
            &["client-a", "client-b"],
        );
        let packet = build_publish_packet("sensors/temp", 1, b"25");

        RouterActor::route(context, "tenant-a", &packet, None)
            .await
            .unwrap();

        // Wait for async delivery
        wait_until(|| delivered.iter().map(|d| d.len()).sum::<usize>() == 1).await;

        let total: usize = delivered.iter().map(|d| d.len()).sum();
        assert_eq!(
            total, 1,
            "shared group should deliver to exactly one subscriber"
        );
    }

    #[actix::test]
    async fn test_shared_subscription_round_robin() {
        let (registry, delivered) =
            build_multi_client_registry("tenant-a", &["client-a", "client-b"]);
        let topic_storage = Arc::new(RwLock::new(TopicStorage::new()));
        topic_storage.read().create_tenant(&"tenant-a".to_string());
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "client-a".to_string(),
                "$share/grp/sensors/temp".to_string(),
                1,
                false,
                false,
            )
            .unwrap();
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "client-b".to_string(),
                "$share/grp/sensors/temp".to_string(),
                1,
                false,
                false,
            )
            .unwrap();

        let shared_cursors = Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        // Send 4 publishes; round-robin should distribute them evenly.
        for _ in 0..4 {
            let mut ctx = build_route_context(
                "tenant-a",
                registry.clone(),
                topic_storage.clone(),
                &["client-a", "client-b"],
            );
            ctx.shared_subscription_cursors = shared_cursors.clone();
            let packet = build_publish_packet("sensors/temp", 1, b"v");
            RouterActor::route(ctx, "tenant-a", &packet, None)
                .await
                .unwrap();
        }

        // Wait for async delivery
        wait_until(|| delivered.iter().map(|d| d.len()).sum::<usize>() == 4).await;

        // Sorted by client_identifier: client-a=0, client-b=1.
        // Cycles: 0,1,0,1 → each receives 2.
        assert_eq!(delivered[0].len(), 2, "client-a should receive 2");
        assert_eq!(delivered[1].len(), 2, "client-b should receive 2");
    }

    #[actix::test]
    async fn test_shared_and_normal_subscription_coexist() {
        let (registry, delivered) =
            build_multi_client_registry("tenant-a", &["normal-client", "shared-a", "shared-b"]);
        let topic_storage = Arc::new(RwLock::new(TopicStorage::new()));
        topic_storage.read().create_tenant(&"tenant-a".to_string());
        // Normal subscription
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "normal-client".to_string(),
                "sensors/temp".to_string(),
                1,
                false,
                false,
            )
            .unwrap();
        // Shared subscriptions
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "shared-a".to_string(),
                "$share/grp/sensors/temp".to_string(),
                1,
                false,
                false,
            )
            .unwrap();
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "shared-b".to_string(),
                "$share/grp/sensors/temp".to_string(),
                1,
                false,
                false,
            )
            .unwrap();

        let context = build_route_context(
            "tenant-a",
            registry,
            topic_storage,
            &["normal-client", "shared-a", "shared-b"],
        );
        let packet = build_publish_packet("sensors/temp", 1, b"25");

        RouterActor::route(context, "tenant-a", &packet, None)
            .await
            .unwrap();

        // Wait for async delivery (1 normal + 1 shared = 2 total)
        wait_until(|| delivered.iter().map(|d| d.len()).sum::<usize>() == 2).await;

        // Normal client always receives (fan-out), shared group delivers to exactly one.
        assert_eq!(delivered[0].len(), 1, "normal-client should receive 1");
        let shared_total: usize = delivered[1].len() + delivered[2].len();
        assert_eq!(
            shared_total, 1,
            "shared group should deliver to exactly one of shared-a/shared-b"
        );
    }

    #[actix::test]
    async fn test_shared_subscription_no_local_exclusion() {
        let (registry, delivered) =
            build_multi_client_registry("tenant-a", &["publisher", "other-member"]);
        let topic_storage = Arc::new(RwLock::new(TopicStorage::new()));
        topic_storage.read().create_tenant(&"tenant-a".to_string());
        // Both members subscribe with no_local = true
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "publisher".to_string(),
                "$share/grp/sensors/temp".to_string(),
                1,
                true,
                false,
            )
            .unwrap();
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "other-member".to_string(),
                "$share/grp/sensors/temp".to_string(),
                1,
                true,
                false,
            )
            .unwrap();

        let context = build_route_context(
            "tenant-a",
            registry,
            topic_storage,
            &["publisher", "other-member"],
        );
        let packet = build_publish_packet("sensors/temp", 1, b"25");

        // Publish from "publisher" — should be excluded by No Local,
        // so only "other-member" is eligible.
        RouterActor::route(context, "tenant-a", &packet, Some("publisher"))
            .await
            .unwrap();

        // Wait for async delivery
        wait_until(|| delivered[1].len() == 1).await;

        assert_eq!(
            delivered[0].len(),
            0,
            "publisher should not receive its own message (No Local)"
        );
        assert_eq!(
            delivered[1].len(),
            1,
            "other-member should receive the message"
        );
    }

    #[actix::test]
    async fn test_shared_subscription_falls_back_when_selected_local_session_missing() {
        let (registry, delivered) = build_multi_client_registry("tenant-a", &["client-b"]);
        let topic_storage = Arc::new(RwLock::new(TopicStorage::new()));
        topic_storage.read().create_tenant(&"tenant-a".to_string());
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "client-a".to_string(),
                "$share/grp/sensors/temp".to_string(),
                1,
                false,
                false,
            )
            .unwrap();
        topic_storage
            .read()
            .subscribe_with_options(
                "tenant-a".to_string(),
                "client-b".to_string(),
                "$share/grp/sensors/temp".to_string(),
                1,
                false,
                false,
            )
            .unwrap();

        let context = build_route_context(
            "tenant-a",
            registry,
            topic_storage,
            &["client-a", "client-b"],
        );
        let packet = build_publish_packet("sensors/temp", 1, b"25");

        RouterActor::route(context, "tenant-a", &packet, None)
            .await
            .unwrap();

        wait_until(|| delivered[0].len() == 1).await;

        assert_eq!(
            delivered[0].len(),
            1,
            "shared delivery should fall back to the next routable member"
        );
    }
}
