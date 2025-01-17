use std::sync::Arc;

use log::{debug, warn};
use tokio::{select, sync::RwLock};

use crate::{session::session_manager::SessionManager, topic::TopicManager};
use anyhow::Result;
use yedmq_mqtt::MqttPacketV3;

// Represent router command
pub enum RouterCmd {
    // Route publish packet to the subscribtion session
    // struct:
    // tenant_identifier: String, packet: MqttPacketV3
    RoutePacket(String,MqttPacketV3),

    // Route packet to all tenants
    RoutePacketToAllTenants(MqttPacketV3),
}

pub struct Router {
    pub session_manager: Arc<RwLock<SessionManager>>,
    pub topic_manager: Arc<RwLock<TopicManager>>,
    pub router_receiver: tokio::sync::mpsc::Receiver<RouterCmd>,
}

impl Router {
    pub async fn run(&mut self) {
        loop {
            select! {
                cmd = self.router_receiver.recv() => {
                    match cmd {
                        Some(RouterCmd::RoutePacket(tenant_identifier,  packet_)) => {
                            match self.route(&tenant_identifier, &packet_).await {
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
        let tenants = self.topic_manager.read().await.get_tenant_names();
        for tenant in tenants.iter() {
            self.route(tenant, packet).await?;
        }
        Ok(())
    }

    pub async fn route(&self, tenant_identifier: &String,  packet: &MqttPacketV3) -> Result<()> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = publish_packet.variable_header.topic_name.clone();
            let topic_manager = self.topic_manager.read().await;
            let subscriptions = topic_manager
                .get_subscriptions(tenant_identifier.clone(), topic)
                .unwrap();
            let session_manager = self.session_manager.read().await;
            for item in subscriptions.iter() {
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
                    if let Err(error) = session_manager
                        .send_packet(
                            tenant_identifier.clone(),
                            client_identifier.clone(),
                            &MqttPacketV3::Publish(publish_packet),
                        )
                        .await
                    {
                        warn!(
                            "tenant {} session {} send packet error, details: {}",
                            tenant_identifier,
                            client_identifier.clone(),
                            error
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
