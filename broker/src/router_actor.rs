use actix::prelude::*;
use log::warn;
use tonic::Request;
use yedmq_mqtt::MqttPacketV3;

use crate::{protobuf::cluster_service_client::ClusterServiceClient, raft::NodeId, session::session_manager_actor::{self, SendMessageToSession}, settings::Node};

#[derive(Clone,Debug,thiserror::Error)]
pub enum RouterActorError {

    #[error("gRPC error: {0}")]
    GRPC(String),

    #[error("Actor unexpected stopped")]
    ActorUnExceptedStopped(#[from] MailboxError),

    #[error("Topic raft error: {0}")]
    TopicRaftError(#[from] crate::raft::topic::topic_raft_actor::TopicRaftError),

}

pub struct RouterActor {
    pub current_node_id: NodeId,
    pub settings: crate::settings::Settings,
}

impl Default for RouterActor {
    fn default() -> Self {
        let settings = crate::settings::Settings::default();
        RouterActor {
            current_node_id: settings.cluster.node_id,
            settings: settings,
        }
    }
}

impl SystemService for RouterActor {}

impl Supervised for RouterActor {}


impl Actor for RouterActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        log::info!("RouterActor started with node id: {}", self.current_node_id);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("RouterActor stopped");
    }
}

impl RouterActor {

    async fn route(current_node_id: &NodeId, cluster_nodes: &Vec<Node>, tenant_id: &String, packet: &MqttPacketV3) -> Result<(), RouterActorError> {
        log::info!("Routing packet for tenant {}: {:?}", tenant_id, packet);
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = &publish_packet.variable_header.topic_name;
            let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();

            let res = topic_raft_actor_addr.send(crate::raft::topic::topic_raft_actor::GetSubscriptions {
                tenant_id: tenant_id.clone(),
                topic: topic.clone(),
            }).await?;

            match res {
                Ok(subscriptions) => {
                    let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
                    for item in subscriptions.subscriptions {
                        let session_actor_map_res = session_actor_map_raft_actor_addr.send(crate::raft::session_actor_map::session_actor_map_raft_actor::GetSessionActorMap {
                            tenant_id: tenant_id.clone(),
                            client_id: item.client_identifier.clone(),
                        }).await?;
                        if let Ok(session_actor_map) = session_actor_map_res {
                            if let Some(session_actor_addr) = session_actor_map {
                                if session_actor_addr.node_id != *current_node_id {
                                    // Route to other nodes
                                    let dest_node_id = session_actor_addr.node_id;
                                    let nodes = cluster_nodes.iter().filter(|n| n.id == dest_node_id).collect::<Vec<_>>();
                                    if nodes.is_empty() {
                                        warn!("No node found with id: {}", dest_node_id);
                                        continue;
                                    }
                                    let dest_addr = nodes[0].rpc_address.clone();
                                    if let Err(e) = Self::route_to_other_nodes(&dest_addr,tenant_id, packet).await {
                                        warn!("Failed to route packet to {}: {}", dest_addr, e);
                                    }
                                    continue;
                                } else {
                                    if let Err(e) = Self::route_in_local_node(tenant_id, packet).await {
                                        warn!("Failed to route packet in local node for tenant {}: {}", tenant_id, e);
                                    }
                                }
                            }
                        } else {
                            warn!("Failed to get session actor map for tenant {} and client {}", tenant_id, item.client_identifier);
                        }
                    }
                    Ok(())
                }
                Err(e) => {
                    warn!("Failed to get subscriptions for topic {}: {}", topic, e);
                    Err(RouterActorError::TopicRaftError(e))
                },
            }
        } else {
            Ok(())
        }
    }

    async fn route_to_other_nodes(dest_addr: &String, tenant_id: &String, packet: &MqttPacketV3) -> Result<(), RouterActorError> {
        let mut cluster_client = ClusterServiceClient::connect(dest_addr.clone()).await.map_err(|e| {
            warn!("Failed to connect to cluster service at {}: {}", dest_addr, e);
            RouterActorError::GRPC(e.to_string())
        })?;
        let request = crate::protobuf::RoutePacketRequest {
            tenant_id: tenant_id.clone(),
            payload: serde_json::to_string(packet).map_err(|e| {
                warn!("Failed to serialize packet: {}", e);
                RouterActorError::GRPC(e.to_string())
            })?,
        };
        cluster_client.route_packet(Request::new(request)).await.map_err(|e| {
            warn!("Failed to route packet to {}: {}", dest_addr, e);
            RouterActorError::GRPC(e.to_string())
        })?;
        Ok(())
    }

    async fn route_in_local_node(tenant_id: &String, packet: &MqttPacketV3) -> Result<(), RouterActorError> {
        // Implement the logic to route the packet within the local node
        log::info!("Routing packet for tenant {}: {:?}", tenant_id, packet);
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = &publish_packet.variable_header.topic_name;
            let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();

            let res = topic_raft_actor_addr.send(crate::raft::topic::topic_raft_actor::GetSubscriptions {
                tenant_id: tenant_id.clone(),
                topic: topic.clone(),
            }).await.unwrap();

            match res {
                Ok(subscriptions) => {
                    let session_manager_actor_addr = crate::session::session_manager_actor::SessionManagerActor::from_registry();
                    for item in subscriptions.subscriptions {
                        let mut publish_packet = publish_packet.clone();
                        if publish_packet.fix_header.qos.unwrap() >= item.qos.into() {
                            if item.qos == 0 && publish_packet.fix_header.qos.unwrap() > 0 {
                                publish_packet.fix_header.qos = Some(0);
                                publish_packet.variable_header.packet_identifier = None;
                                publish_packet.fix_header.remaining_length =
                                    publish_packet.fix_header.remaining_length - 2;
                            } else {
                                publish_packet.fix_header.qos = Some(item.qos.into());
                            }
                        }

                        if let Err(err) = session_manager_actor_addr
                            .send(SendMessageToSession {
                                tenant_id: tenant_id.clone(),
                                client_id: item.client_identifier.clone(),
                                packet: MqttPacketV3::Publish(publish_packet),
                            })
                            .await
                        {
                            warn!(
                                "tenant {} session {} send packet error, details: {}",
                                tenant_id,
                                item.client_identifier.clone(),
                                err
                            );
                        }
                    }
                },
                Err(e) => {
                    log::error!("Failed to get subscriptions for topic {}: {}", topic, e);
                }
            }
        }
        Ok(())
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
        log::info!("Routing packet for tenant {}: {:?}", msg.tenant_id, msg.packet);
        let current_node_id = self.current_node_id;
        let cluster_nodes = self.settings.cluster.nodes.clone();
        Box::pin(async move {
            Self::route(&current_node_id, &cluster_nodes,&msg.tenant_id, &msg.packet).await?;
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
        log::info!("Routing packet for tenant {}: {:?}", msg.tenant_id, msg.packet);
        Box::pin(async move {
            Self::route_in_local_node(&msg.tenant_id, &msg.packet).await?;
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
        return Box::pin(async move {
            let session_manager_actor_addr = session_manager_actor::SessionManagerActor::from_registry();
            let tenant_ids = session_manager_actor_addr.send(session_manager_actor::GetAllTenantIds {}).await.unwrap();
            for tenant_id in tenant_ids {
                Self::route_in_local_node(&tenant_id, &msg.packet);
            }
            Ok(())
        }.into_actor(self));
        
    }
}