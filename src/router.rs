use std::{collections::HashMap, sync::Arc, borrow::BorrowMut};

use log::warn;
use tokio::{sync::RwLock, select};

use crate::{session::{Session, SessionManager}, protocol::MqttPacketV3, topic::TopicManager};
use anyhow::Result;

// Represent router command
pub enum RouterCmd {

    // Route publish packet to the subscribtion session
    // struct:
    // tenant_identifier: String, packet: MqttPacketV3
    RoutePacket(String,MqttPacketV3), 

}

pub struct Router {
    session_manager: Arc<SessionManager>,
    topic_manager: Arc<RwLock<TopicManager>>,
    router_receiver: tokio::sync::mpsc::Receiver<RouterCmd>,
}

impl Router {

    pub async fn run(&mut self) {
        loop {
            select! {
                cmd = self.router_receiver.recv() => {
                    match cmd {
                        Some(RouterCmd::RoutePacket(tenant_identifier, packet_)) => {
                            match self.route(&tenant_identifier, &packet_).await {
                                Ok(_) => (),
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
                let _ = topic_manager.register_retain_publish_packet(tenant_identifier.clone(),packet);
            }
            let topic_manager = self.topic_manager.read().await;
            let subscriptions = topic_manager.get_subscriptions(
                tenant_identifier.clone(),
                topic).unwrap();
            for item in subscriptions.iter() {
                let client_identifier = item.client_identifier.clone();
                self.session_manager.send_packet(client_identifier, packet).await;
            }
        }
        
        Ok(())
    }
}