use std::{collections::HashMap, sync::Arc, borrow::BorrowMut};

use tokio::{sync::RwLock, select};

use crate::{session::{Session, SessionManager}, protocol::MqttPacketV3, topic::TopicManager};
use anyhow::Result;

pub enum RouterCmd {
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
                            self.route(&tenant_identifier, packet_).await;
                        },
                        None => todo!()
                    }
                }
            }
        }
    }

    pub async fn route(&self, tenant_identifier: &String, packet: MqttPacketV3) -> Result<()> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic_manager = self.topic_manager.read().await;
            let subscriptions = topic_manager.get_subscriptions(
                tenant_identifier.clone(),
                publish_packet.variable_header.topic_name.clone()).unwrap();
            let publish_packet = MqttPacketV3::Publish(publish_packet);
            for item in subscriptions.iter() {
                let client_identifier = item.client_identifier.clone();
                let session = self.session_manager.get(client_identifier).await;
                if let Some(session) = session {
                    let mut session = session.write().await;
                    session.process_route_packet(&publish_packet).await?;
                }
            }
        }
        
        Ok(())
    }
}