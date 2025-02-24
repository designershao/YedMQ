use std::sync::Arc;

use tokio::sync::RwLock;
use yedmq_mqtt::MqttPacketV3;

use crate::raft::{store::Request, NodeId};

use super::topic_storage::{Error, Subscription, TopicStorage};

pub struct TopicManager {
    storage: Arc<RwLock<TopicStorage>>,
    raft_manager: Arc<crate::raft::raft_manager::RaftManager>,
    current_node_id: NodeId,
}

impl TopicManager {
    pub fn new(
        storage: Arc<RwLock<TopicStorage>>,
        raft_manager: Arc<crate::raft::raft_manager::RaftManager>,
        current_node_id: NodeId,
    ) -> Self {
        Self {
            storage,
            raft_manager,
            current_node_id,
        }
    }

    pub async fn get_retain_message_list_with_pagination(
        &self, 
        tenant_identifier: &str,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(u64,Vec<(String, String,u8)>)> {
        let storage = self.storage.read().await;
        storage.get_retain_message_list_with_pagination(tenant_identifier, offset, limit)
    }

    pub async fn get_topic_list_with_pagination(
        &self,
        tenant_id: &String,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(u64, Vec<(String, String, u8)>)> {
        let storage = self.storage.read().await;
        storage.get_topic_list_with_pagination(tenant_id, offset, limit)
    }

    pub async fn handle_subscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic_filter: String,
        qos: u8,
    ) -> Result<(), Error> {
        let _ = self.raft_manager.execute_command(Request::SubscribeTopic {
            node_id: self.current_node_id,
            tenant_id,
            client_identifier,
            topic: topic_filter,
            qos,
        });
        Ok(())
    }

    pub async fn handle_unsubscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic_filter: String,
    ) -> Result<(), Error> {
        let _ = self
            .raft_manager
            .execute_command(Request::UnsubscribeTopic {
                node_id: self.current_node_id,
                tenant_id,
                client_identifier,
                topic: topic_filter,
            });
        Ok(())
    }

    pub async fn get_subscribers(
        &self,
        tenant_id: String,
        msg_topic: String,
    ) -> Result<Vec<Arc<Subscription>>, Error> {
        let inner_storage = self.storage.read().await;
        inner_storage.get_subscriptions(tenant_id, msg_topic)
    }

    pub async fn clean_retain_publish_packet(
        &mut self,
        tenant_id: String,
        topic_filter: &String,
    ) -> Result<(), Error> {
        let _ = self
            .raft_manager
            .execute_command(Request::CleanRetainPublishPacket {
                tenant_id,
                topic_filter: topic_filter.to_string(),
            }).await.unwrap();
        Ok(())
    }

    pub async fn get_tenant_names(&self) -> Vec<String> {
        let inner_storage = self.storage.read().await;
        inner_storage.get_tenant_names()
    }

    pub async fn register_retain_publish_packet(
        &mut self,
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: &MqttPacketV3,
    ) -> Result<(), Error> {
        self
            .raft_manager
            .execute_command(Request::RegisterRetainPublishPacket {
                tenant_id,
                source_client_identifier,
                publish_packet: publish_packet.clone(),
            }).await.unwrap();
        Ok(())
    }

    pub async fn get_retain_publish_packet(
        &mut self,
        tenant_id: String,
        topic_filter: String,
    ) -> Result<Vec<Arc<MqttPacketV3>>, Error> {
        let topic_storage = self.storage.read().await;
        topic_storage.get_retain_publish_packet(tenant_id, topic_filter)
    }

    pub async fn create_tenant(&mut self, tenant_id: String) -> Result<(), Error> {
        self
            .raft_manager
            .execute_command(Request::CreateTenant { tenant_id }).await.unwrap();
        Ok(())
    }
}
