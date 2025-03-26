use std::sync::Arc;

use actix::Addr;
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use tokio::{select, sync::RwLock};

use crate::{session::session_manager_actor::{SendMessageToSession, SessionManagerActor}, topic::topic_manager::TopicManagerTrait};
use anyhow::Result;
use yedmq_mqtt::MqttPacketV3;
use crate::protobuf::raft_service_client::RaftServiceClient;

#[derive(Serialize, Deserialize)]
// Represent router command
pub enum RouterCmd {
    // Route publish packet to the subscribtion session
    RoutePacket{ tenant_identifier: String, packet: MqttPacketV3 },

    // Route packet to all tenants
    RoutePacketToAllTenants(MqttPacketV3),
}

pub struct Router {
    pub session_manager: Addr<SessionManagerActor>,
    pub topic_manager: Arc<RwLock<dyn TopicManagerTrait>>,
    pub router_receiver: tokio::sync::mpsc::Receiver<RouterCmd>,
    pub raft_manager: Arc<crate::raft::raft_manager::RaftManager>,
}

impl Router {
    pub async fn run(&mut self) {
        loop {
            select! {
                cmd = self.router_receiver.recv() => {
                    match cmd {
                        Some(RouterCmd::RoutePacket{tenant_identifier,packet }) => {
                            match self.route(&tenant_identifier, &packet).await {
                                Ok(_) => {
                                },
                                Err(e) => {
                                    warn!("teanant {} route packet to session error: {}", tenant_identifier, e);
                                }
                            }
                        },
                        Some(RouterCmd::RoutePacketToAllTenants(packet_)) => {
                            match self.route_to_all_tenants(&packet_).await {
                                Ok(_) => {
                                }
                                Err(e) => {
                                    warn!("route packet to all tenants error: {}", e);
                                }
                            }
                        }
                        None => todo!()
                    }
                }
            }
        }
    }

    pub async fn route_to_all_tenants(&self, packet: &MqttPacketV3) -> Result<()> {
        let tenants = self.topic_manager.read().await.get_tenant_names().await;
        for tenant in tenants.iter() {
            self.route(tenant, packet).await?;
        }
        Ok(())
    }

    async fn route_to_other_nodes(&self,addr: &String, cmd: RouterCmd) -> Result<()> {
        let addr = format!("http://{}", addr);

        let mut client = RaftServiceClient::connect(addr.clone()).await.unwrap();

        let route_request = crate::protobuf::RoutePacketRequest {
            data: serde_json::to_string(&cmd).unwrap(),
        };

        let _ = client.route_packet(route_request).await;

        Ok(())
    }


    pub async fn route(&self, tenant_identifier: &String,  packet: &MqttPacketV3) -> Result<()> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = publish_packet.variable_header.topic_name.clone();
            let topic_manager = self.topic_manager.read().await;
            let subscriptions = topic_manager
                .get_subscribers(tenant_identifier.clone(), topic).await
                .unwrap();
            for item in subscriptions.iter() {
                if item.node_id != self.raft_manager.topic_raft().current_node_id() {
                    // not the current node, send to other node
                    let router_cmd = RouterCmd::RoutePacket{
                        tenant_identifier: tenant_identifier.clone(),
                        packet: packet.clone(),
                    };
                    let node = self.raft_manager.topic_raft().get_node_by_id(item.node_id).await;
                    if let Some(node) = node {
                        if let Err(e) = self.route_to_other_nodes(&node.rpc_addr, router_cmd).await {
                            warn!("route packet to node {} error: {}", node.rpc_addr, e);
                        }
                    }
                    
                } else {
                    let client_identifier = item.client_identifier.clone();
                    let packet = packet.clone();
                    if let MqttPacketV3::Publish(mut publish_packet) = packet {
                        debug!(
                            "tenant {} session {} send packet max qos {} , body is {:?}",
                            tenant_identifier,
                            client_identifier.clone(),
                            item.qos,
                            publish_packet.payload.payload
                        );
                        if publish_packet.fix_header.qos.unwrap() >= item.qos as i32 {
                            if item.qos == 0 && publish_packet.fix_header.qos.unwrap() > 0 {
                                publish_packet.fix_header.qos = Some(0);
                                publish_packet.variable_header.packet_identifier = None;
                                publish_packet.fix_header.remaining_length =
                                    publish_packet.fix_header.remaining_length - 2;
                            } else {
                                publish_packet.fix_header.qos = Some(item.qos.into());
                            }
                        }
                        if let Err(err) = self.session_manager.send(SendMessageToSession {
                            tenant_id: tenant_identifier.clone(),
                            client_id: tenant_identifier.clone(),
                            packet: MqttPacketV3::Publish(publish_packet),
                        }).await
                        {
                            warn!(
                                "tenant {} session {} send packet error, details: {}",
                                tenant_identifier,
                                client_identifier.clone(),
                                err
                            );
                        }
                    }
                }

            }
        }

        Ok(())
    }
}
