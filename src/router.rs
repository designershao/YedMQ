use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::{session::{Session, SessionManager}, protocol::MqttPacketV3, topic::TopicManager};
use anyhow::Result;

pub struct Router {
    session_manager: Arc<SessionManager>,
    topic_manager: Arc<RwLock<TopicManager>>,
}

impl Router {
    pub async fn route(&self, tenant_identifier: &String, packet: MqttPacketV3) -> Result<()> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic_manager = self.topic_manager.read().await;
            let subscriptions = topic_manager.get_subscriptions(
                tenant_identifier.clone(),
                publish_packet.variable_header.topic_name.clone()).unwrap();
            let publish_packet = MqttPacketV3::Publish(publish_packet);
            for item in subscriptions.iter() {
                let client_identifier = item.client_identifier.clone();
                let session = self.session_manager.get(client_identifier);
                if let Some(session) = session {
                    session.write_packet(&publish_packet).await?;
                }
            }
        }
        
        Ok(())
    }
}