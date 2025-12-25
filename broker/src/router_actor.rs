use actix::dev::MessageResponse;
use actix::prelude::*;
use log::{info, warn};
use tonic::Request;
use yedmq_mqtt::MqttPacketV3;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{protobuf::cluster_service_client::ClusterServiceClient, raft::NodeId, session::{session_registry::SessionRegistry, session_manager_actor::{self}}, settings::Node};
use crate::session::session_actor_map_storage::SessionActorMapStorage;
use crate::session::session_manager_actor::SessionManagerActor;
use crate::settings::Settings;
use crate::topic::topic_storage::TopicStorage;

#[derive(Clone, Debug, thiserror::Error)]
pub enum RouterActorError {
    #[error("gRPC error: {0}")]
    GRPC(String),

    #[error("Actor unexpected stopped")]
    ActorUnExceptedStopped(#[from] MailboxError),

    #[error("Topic raft error: {0}")]
    TopicRaftError(#[from] crate::raft::topic::topic_raft_actor::TopicRaftError),

    #[error("Get subscribers error: {0}")]
    GetSubscribersError(#[from] crate::topic::TopicError),

    #[error("Dead letter queue is full")]
    DeadLetterQueueFull,

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

#[derive(Clone, Debug)]
pub struct DeadLetterItem {
    pub tenant_id: String,
    pub packet: MqttPacketV3,
    pub dest_addr: String,
    pub retry_count: u32,
    pub created_at: u64,
    pub last_retry_at: u64,
}

#[derive(Clone, Debug)]
pub struct DeadLetterConfig {
    pub max_queue_size: usize,
    pub max_retry_count: u32,
    pub retry_interval_seconds: u64,
    pub cleanup_interval_seconds: u64,
    pub message_ttl_seconds: u64,
}

impl Default for DeadLetterConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 10000,
            max_retry_count: 3,
            retry_interval_seconds: 30,
            cleanup_interval_seconds: 300, // 5 minutes
            message_ttl_seconds: 86400,    // 24 hours
        }
    }
}

pub struct RouterActor {
    pub current_node_id: NodeId,
    pub settings: Arc<crate::settings::Settings>,
    pub dead_letter_queue: VecDeque<DeadLetterItem>,
    pub dead_letter_config: DeadLetterConfig,
    pub session_manager_actor: Addr<SessionManagerActor>,
    pub topic_raft_actor: Option<Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>>,
    pub local_topic_storage: Arc<RwLock<TopicStorage>>,
    pub local_session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
    pub session_registry: SessionRegistry,
}

impl Actor for RouterActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(10000);
        log::info!("RouterActor started with node id: {}", self.current_node_id);
        
        // Start dead letter queue retry timer
        ctx.run_interval(
            Duration::from_secs(self.dead_letter_config.retry_interval_seconds),
            |act, _ctx| {
                act.process_dead_letter_queue(_ctx.address());
            },
        );

        // Start dead letter queue cleanup timer
        ctx.run_interval(
            Duration::from_secs(self.dead_letter_config.cleanup_interval_seconds),
            |act, _ctx| {
                act.cleanup_expired_dead_letters();
            },
        );
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("RouterActor stopped");
    }
}

impl RouterActor {

    pub fn new(
        settings: Arc<Settings>,
        session_manager_actor: Addr<SessionManagerActor>,
        topic_raft_actor: Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>,
        topic_storage: Arc<RwLock<TopicStorage>>,
        session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
        session_registry: SessionRegistry,
    ) -> Self {
        RouterActor {
            current_node_id: settings.cluster.node_id,
            settings,
            dead_letter_queue: VecDeque::new(),
            dead_letter_config: DeadLetterConfig::default(),
            session_manager_actor,
            topic_raft_actor: Some(topic_raft_actor),
            local_topic_storage: topic_storage,
            local_session_actor_map_storage: session_actor_map_storage,
            session_registry,
        }
    }

    async fn route(
        session_manager_actor: Addr<SessionManagerActor>,
        router_actor: Addr<RouterActor>,
        current_node_id: &NodeId,
        cluster_nodes: &Vec<Node>,
        tenant_id: &String,
        packet: &MqttPacketV3,
        local_topic_storage: Arc<RwLock<TopicStorage>>,
        local_session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
        session_registry: SessionRegistry
    ) -> Result<(), RouterActorError> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = &publish_packet.variable_header.topic_name;

            let local_topic_storage = local_topic_storage.read().await;

            let subscriptions = local_topic_storage.get_subscriptions(
                tenant_id.clone(),
                topic.clone(),
            )?;

            for item in subscriptions {
                let session_actor_map = local_session_actor_map_storage.read().await.get_session_actor_map(tenant_id, &item.client_identifier);
                if let Some(session_actor_addr) = session_actor_map {
                    if session_actor_addr.node_id != *current_node_id {
                        // Route to other nodes
                        let dest_node_id = session_actor_addr.node_id;
                        let nodes = cluster_nodes.iter().filter(|n| n.id == dest_node_id).collect::<Vec<_>>();
                        info!("route packet to node {}", dest_node_id);
                        if nodes.is_empty() {
                            warn!("No node found with id: {}", dest_node_id);
                            continue;
                        }
                        let dest_addr = nodes[0].rpc_address.clone();
                        
                        let tenant_id_clone = tenant_id.clone();
                        let packet_clone = packet.clone();
                        let router_actor_clone = router_actor.clone();

                        tokio::spawn(async move {
                            if let Err(e) = Self::route_to_other_nodes(&dest_addr, &tenant_id_clone, &packet_clone).await {
                                warn!("Failed to route packet to {}: {}", dest_addr, e);

                                // Check if the packet should be added to the dead letter queue
                                if Self::should_add_to_dead_letter_queue(&packet_clone) {
                                    router_actor_clone.do_send(AddToDeadLetterQueue {
                                        tenant_id: tenant_id_clone,
                                        packet: packet_clone,
                                        dest_addr,
                                    });
                                }
                            }
                        });
                        
                        continue;
                    } else {
                        let mut publish_packet = publish_packet.clone();
                        if publish_packet.fix_header.qos.unwrap_or_default() >= item.qos.into() {
                            if item.qos == 0 && publish_packet.fix_header.qos.unwrap_or_default() > 0 {
                                publish_packet.fix_header.qos = Some(0);
                                publish_packet.variable_header.packet_identifier = None;
                                publish_packet.fix_header.remaining_length -= 2; // Remove 2 bytes for packet identifier
                            } else {
                                publish_packet.fix_header.qos = Some(item.qos.into());
                            }
                        }
                        if let Err(e) = Self::route_to_local_session(
                            tenant_id,
                            &item.client_identifier,
                            MqttPacketV3::Publish(publish_packet),
                            session_manager_actor.clone(),
                            session_registry.clone()
                        )
                        {
                            warn!("Failed to route packet in local node for tenant {}: {}", tenant_id, e);
                        }
                    }
                }
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    async fn route_to_other_nodes(dest_addr: &String, tenant_id: &String, packet: &MqttPacketV3) -> Result<(), RouterActorError> {
        let mut cluster_client = ClusterServiceClient::connect(format!("http://{}", dest_addr.clone())).await.map_err(|e| {
            warn!("Failed to connect to cluster service at {}: {}", dest_addr, e);
            RouterActorError::GRPC(e.to_string())
        })?;
        let request = crate::protobuf::RoutePacketRequest {
            tenant_id: tenant_id.clone(),
            payload: serde_json::to_string(packet).map_err(|e| {
                warn!("Failed to serialize packet: {}", e);
                RouterActorError::SerializationError(e.to_string())
            })?,
        };
        cluster_client.route_packet(Request::new(request)).await.map_err(|e| {
            warn!("Failed to route packet to {}: {}", dest_addr, e);
            RouterActorError::GRPC(e.to_string())
        })?;
        Ok(())
    }

    fn route_to_local_session(tenant_id: &String, client_id: &String, packet: MqttPacketV3, _session_manager_actor_addr: Addr<SessionManagerActor>, session_registry: SessionRegistry) -> Result<(), RouterActorError> {
        log::debug!("Route to  local node session {} {} : {:?}",tenant_id, client_id, packet);
        if let MqttPacketV3::Publish(publish_packet) = packet {
            if let Some(recipient) = session_registry.get_session(&tenant_id, &client_id) {
                recipient.do_send(crate::session::session_actor::SessionActorMessage::OutboundMessage(MqttPacketV3::Publish(publish_packet)));
            }
        }
        Ok(())
    }

    // Called by rpc service when receiving packet from other nodes
    async fn publish_to_local_subscribers(
        tenant_id: &String,
        packet: &MqttPacketV3,
        session_manager_actor_addr: Addr<SessionManagerActor>,
        topic_raft_actor_addr: Addr<crate::raft::topic::topic_raft_actor::TopicRaftActor>,
        session_registry: SessionRegistry,
    ) -> Result<(), RouterActorError> {
        log::debug!("In route in local node: Routing packet for tenant {}: {:?}", tenant_id, packet);
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = &publish_packet.variable_header.topic_name;

            let res = topic_raft_actor_addr.send(crate::raft::topic::topic_raft_actor::GetSubscriptions {
                tenant_id: tenant_id.clone(),
                topic: topic.clone(),
            }).await?;

            match res {
                Ok(subscriptions) => {
                    for item in subscriptions.subscriptions {
                        let mut publish_packet = publish_packet.clone();
                        if publish_packet.fix_header.qos.unwrap_or_default() >= item.qos.into() {
                            if item.qos == 0 && publish_packet.fix_header.qos.unwrap_or_default() > 0 {
                                publish_packet.fix_header.qos = Some(0);
                                publish_packet.variable_header.packet_identifier = None;
                                publish_packet.fix_header.remaining_length -= 2; // Remove 2 bytes for packet identifier
                            } else {
                                publish_packet.fix_header.qos = Some(item.qos.into());
                            }
                        }

                        Self::route_to_local_session(
                            tenant_id,
                            &item.client_identifier,
                            MqttPacketV3::Publish(publish_packet),
                            session_manager_actor_addr.clone(),
                            session_registry.clone()
                        )?;
                    }
                    Ok(())
                },
                Err(e) => {
                    log::error!("Failed to get subscriptions for topic {}: {}", topic, e);
                    Err(RouterActorError::TopicRaftError(e))
                }
            }
        } else {
            warn!("Only Publish packets are supported for routing in local node, got: {:?}, drop it.", packet);
            Ok(())
        }
    }

    // Check if the packet should be added to the dead letter queue
    fn should_add_to_dead_letter_queue(packet: &MqttPacketV3) -> bool {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            if let Some(qos) = publish_packet.fix_header.qos {
                return qos >= 1; // QoS 1 or QoS 2
            }
        }
        false
    }

    // Add the packet to the dead letter queue
    fn add_to_dead_letter_queue(&mut self, tenant_id: String, packet: MqttPacketV3, dest_addr: String) -> Result<(), RouterActorError> {
        // check if the dead letter queue is full
        if self.dead_letter_queue.len() >= self.dead_letter_config.max_queue_size {
            warn!("Dead letter queue is full, dropping oldest message");
            self.dead_letter_queue.pop_front();
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let dead_letter_item = DeadLetterItem {
            tenant_id,
            packet,
            dest_addr,
            retry_count: 0,
            created_at: now,
            last_retry_at: now,
        };

        self.dead_letter_queue.push_back(dead_letter_item);
        log::debug!("Added message to dead letter queue, current queue size: {}", self.dead_letter_queue.len());
        
        Ok(())
    }

    // Process the dead letter queue
    fn process_dead_letter_queue(&mut self, router_actor: Addr<RouterActor>) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut items_to_retry = Vec::new();
        let mut items_to_keep = VecDeque::new();

        // Split the messages that need to be retried and the messages that need to be kept
        while let Some(mut item) = self.dead_letter_queue.pop_front() {
            if item.retry_count >= self.dead_letter_config.max_retry_count {
                warn!("Message exceeded max retry count, dropping: tenant={}, dest={}", 
                      item.tenant_id, item.dest_addr);
                continue;
            }

            if now - item.last_retry_at >= self.dead_letter_config.retry_interval_seconds {
                item.retry_count += 1;
                item.last_retry_at = now;
                items_to_retry.push(item);
            } else {
                items_to_keep.push_back(item);
            }
        }

        self.dead_letter_queue = items_to_keep;

        for item in items_to_retry {
            let item_clone = item.clone();
            let dest_addr = item.dest_addr.clone();
            let tenant_id = item.tenant_id.clone();
            let packet = item.packet.clone();
            let router_actor = router_actor.clone();

            actix::spawn(async move {
                match Self::route_to_other_nodes(&dest_addr, &tenant_id, &packet).await {
                    Ok(_) => {
                        log::debug!("Successfully retried message from dead letter queue: tenant={}, dest={}", 
                                  tenant_id, dest_addr);
                    }
                    Err(e) => {
                        warn!("Failed to retry message from dead letter queue: tenant={}, dest={}, error={}", 
                              tenant_id, dest_addr, e);
                        
                        // Readd message to dead letter queue
                        if let Err(dlq_err) = router_actor.send(ReaddToDeadLetterQueue {
                            item: item_clone,
                        }).await {
                            warn!("Failed to readd message to dead letter queue: {}", dlq_err);
                        }
                    }
                }
            });
        }
    }

    // Clean up expired messages from the dead letter queue
    fn cleanup_expired_dead_letters(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let initial_size = self.dead_letter_queue.len();
        self.dead_letter_queue.retain(|item| {
            now - item.created_at < self.dead_letter_config.message_ttl_seconds
        });

        let removed_count = initial_size - self.dead_letter_queue.len();
        if removed_count > 0 {
            log::info!("Cleaned up {} expired messages from dead letter queue", removed_count);
        }
    }

    // Get stats for the dead letter queue
    fn get_dead_letter_stats(&self) -> DeadLetterStats {
        DeadLetterStats {
            queue_size: self.dead_letter_queue.len(),
            max_queue_size: self.dead_letter_config.max_queue_size,
        }
    }

}

#[derive(Message)]
#[rtype(result = "Result<(), RouterActorError>")]
pub struct RoutePacket {
    pub tenant_id: String,
    pub packet: MqttPacketV3
}

impl Handler<RoutePacket> for RouterActor {
    type Result = ResponseActFuture<Self, Result<(),RouterActorError>>;

    fn handle(&mut self, msg: RoutePacket, _ctx: &mut Self::Context) -> Self::Result {
        log::debug!("In handler Routing packet for tenant {}: {:?}", msg.tenant_id, msg.packet);
        let current_node_id = self.current_node_id;
        let cluster_nodes = self.settings.cluster.nodes.clone();
        let router_actor = _ctx.address();
        let session_manager_actor = self.session_manager_actor.clone();
        let topic_storage = self.local_topic_storage.clone();
        let session_actor_map_storage = self.local_session_actor_map_storage.clone();
        let session_registry = self.session_registry.clone();
        Box::pin(async move {
            Self::route(session_manager_actor, router_actor, &current_node_id, &cluster_nodes,&msg.tenant_id, &msg.packet, topic_storage, session_actor_map_storage, session_registry).await?;
            Ok(())
        }.into_actor(self))
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), RouterActorError>")]
pub struct RouteFromOtherNode {
    pub tenant_id: String,
    pub packet: MqttPacketV3
}

impl Handler<RouteFromOtherNode> for RouterActor {
    type Result = ResponseActFuture<Self, Result<(), RouterActorError>>;

    fn handle(&mut self, msg: RouteFromOtherNode, _ctx: &mut Self::Context) -> Self::Result {
        log::debug!("In handler from other nodeRouting packet for tenant {}: {:?}", msg.tenant_id, msg.packet);
        let session_manager_actor = self.session_manager_actor.clone();
        let topic_raft_actor = self.topic_raft_actor.as_ref().expect("topic raft actor not set").clone();
        let session_registry = self.session_registry.clone();
        Box::pin(async move {
            Self::publish_to_local_subscribers(
                &msg.tenant_id,
                &msg.packet,
                session_manager_actor,
                topic_raft_actor,
                session_registry
            ).await?;
            Ok(())
        }.into_actor(self))
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), RouterActorError>")]
pub struct RoutePacketToAllTenants {
    pub packet: MqttPacketV3
}

impl Handler<RoutePacketToAllTenants> for RouterActor {
    type Result = ResponseActFuture<Self, Result<(), RouterActorError>>;

    fn handle(&mut self, msg: RoutePacketToAllTenants, _ctx: &mut Self::Context) -> Self::Result {
        let session_manager_actor_addr = self.session_manager_actor.clone();
        let topic_raft_actor_addr = self.topic_raft_actor.as_ref().expect("topic raft actor not set").clone();
        let session_registry = self.session_registry.clone();

        Box::pin(async move {
            let tenant_ids = session_manager_actor_addr.send(session_manager_actor::GetAllTenantIds {}).await.unwrap();
            for tenant_id in tenant_ids {
                Self::publish_to_local_subscribers(&tenant_id, &msg.packet, session_manager_actor_addr.clone(), topic_raft_actor_addr.clone(), session_registry.clone()).await?;
            }
            Ok(())
        }.into_actor(self))
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), RouterActorError>")]
pub struct AddToDeadLetterQueue {
    pub tenant_id: String,
    pub packet: MqttPacketV3,
    pub dest_addr: String,
}

impl Handler<AddToDeadLetterQueue> for RouterActor {
    type Result = Result<(), RouterActorError>;

    fn handle(&mut self, msg: AddToDeadLetterQueue, _ctx: &mut Self::Context) -> Self::Result {
        self.add_to_dead_letter_queue(msg.tenant_id, msg.packet, msg.dest_addr)
    }
}

#[derive(Message)]
#[rtype(result = "Result<(), RouterActorError>")]
pub struct ReaddToDeadLetterQueue {
    pub item: DeadLetterItem,
}

impl Handler<ReaddToDeadLetterQueue> for RouterActor {
    type Result = Result<(), RouterActorError>;

    fn handle(&mut self, msg: ReaddToDeadLetterQueue, _ctx: &mut Self::Context) -> Self::Result {
        if self.dead_letter_queue.len() >= self.dead_letter_config.max_queue_size {
            return Err(RouterActorError::DeadLetterQueueFull);
        }
        
        self.dead_letter_queue.push_back(msg.item);
        Ok(())
    }
}

#[derive(Message)]
#[rtype(result = "DeadLetterStats")]
pub struct GetDeadLetterStats;

#[derive(Debug, Clone)]
pub struct DeadLetterStats {
    pub queue_size: usize,
    pub max_queue_size: usize,
}

impl<A, M> MessageResponse<A, M> for DeadLetterStats
where
    A: Actor,
    M: Message<Result = DeadLetterStats>,
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

impl Handler<GetDeadLetterStats> for RouterActor {
    type Result = DeadLetterStats;

    fn handle(&mut self, _msg: GetDeadLetterStats, _ctx: &mut Self::Context) -> Self::Result {
        self.get_dead_letter_stats()
    }
}