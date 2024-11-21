use std::sync::Arc;

use log::warn;
use tokio::{select, sync::RwLock};

use crate::{session::session_manager::SessionManager, topic::TopicManager};
use anyhow::Result;
use samoye_mqtt::MqttPacketV3;

// Represent router command
pub enum RouterCmd {
    // Route publish packet to the subscribtion session
    // struct:
    // tenant_identifier: String, packet: MqttPacketV3
    RoutePacket(String, MqttPacketV3),
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
                        Some(RouterCmd::RoutePacket(tenant_identifier, packet_)) => {
                            match self.route(&tenant_identifier, &packet_).await {
                                Ok(_) => {
                                },
                                Err(e) => {
                                    warn!("teanant {} route packet to session error: {}", tenant_identifier, e);
                                }
                            }
                        },
                        None => todo!()
                    }
                }
            }
        }
    }

    pub async fn route(&self, tenant_identifier: &String, packet: &MqttPacketV3) -> Result<()> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = publish_packet.variable_header.topic_name.clone();
            if publish_packet.fix_header.retain == Some(true) {
                // register retain publish packet
                let mut topic_manager = self.topic_manager.write().await;
                let _ =
                    topic_manager.register_retain_publish_packet(tenant_identifier.clone(), packet);
            }
            let topic_manager = self.topic_manager.read().await;
            let subscriptions = topic_manager
                .get_subscriptions(tenant_identifier.clone(), topic)
                .unwrap();
            let session_manager = self.session_manager.read().await;
            for item in subscriptions.iter() {
                let client_identifier = item.client_identifier.clone();
                let packet = packet.clone();
                if let MqttPacketV3::Publish(mut publish_packet) = packet {
                    publish_packet.fix_header.qos = Some(item.qos.into());
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
