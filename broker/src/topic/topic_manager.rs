use std::sync::Arc;

use log::info;
use mockall::automock;
use tokio::sync::RwLock;
use yedmq_mqtt::MqttPacketV3;

use crate::raft::raft_manager;
use crate::raft::{
        topic::TopicRaftManagerTrait,
        NodeId,
    };

use super::topic_storage::{Subscription, TopicStorage};
use super::TopicError;
use async_trait::async_trait;

#[async_trait]
#[automock]
pub trait TopicManagerTrait: Sync + Send {
    async fn get_retain_message_list_with_pagination(
        &self,
        tenant_identifier: &str,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(u64, Vec<(String, String, u8)>)>;

    async fn get_topic_list_with_pagination(
        &self,
        tenant_id: &String,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(u64, Vec<(String, String, u8)>)>;

    async fn handle_subscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic_filter: String,
        qos: u8,
    ) -> Result<(), TopicError>;

    async fn handle_unsubscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic_filter: String,
    ) -> Result<(), TopicError>;

    async fn get_subscribers(
        &self,
        tenant_id: String,
        msg_topic: String,
    ) -> Result<Vec<Arc<Subscription>>, TopicError>;

    async fn clean_retain_publish_packet(
        &mut self,
        tenant_id: String,
        topic_filter: &String,
    ) -> Result<(), TopicError>;

    async fn get_retain_publish_packet(
        &mut self,
        tenant_id: String,
        topic_filter: String,
    ) -> Result<Vec<Arc<MqttPacketV3>>, TopicError>;

    async fn get_tenant_names(&self) -> Vec<String>;

    async fn register_retain_publish_packet(
        &mut self,
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: &MqttPacketV3,
    ) -> Result<(), TopicError>;

    async fn create_tenant(&mut self, tenant_id: String) -> Result<(), TopicError>;
}

pub struct TopicManager {
    storage: Arc<RwLock<TopicStorage>>,
    topic_raft_client: Arc<dyn crate::raft::client::topic::TopicRaftClientTrait>,
    current_node_id: NodeId,
}

#[async_trait]
impl TopicManagerTrait for TopicManager {
    async fn get_retain_message_list_with_pagination(
        &self,
        tenant_identifier: &str,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(u64, Vec<(String, String, u8)>)> {
        let storage = self.storage.read().await;
        storage.get_retain_message_list_with_pagination(tenant_identifier, offset, limit)
    }

    async fn get_topic_list_with_pagination(
        &self,
        tenant_id: &String,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(u64, Vec<(String, String, u8)>)> {
        let storage = self.storage.read().await;
        storage.get_topic_list_with_pagination(tenant_id, offset, limit)
    }

    async fn handle_subscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic_filter: String,
        qos: u8,
    ) -> Result<(), TopicError> {
        info!(
            "with raft cluster subscribe tenant_id: {} topic: {}, qos: {}, node_id: {}",
            tenant_id, topic_filter, qos, self.current_node_id
        );
        let res = self
            .topic_raft_client
            .handle_subscribe(tenant_id, client_identifier, topic_filter, qos).await;

        res.map_err(|e| {
            match e {
                crate::raft::client::base::RaftClientError::ServiceError { code, message, node } => {
                    match code {
                        crate::protobuf::ErrorCode::Unknown => todo!(),
                        crate::protobuf::ErrorCode::InternalError => todo!(),
                        crate::protobuf::ErrorCode::NotLeader => todo!(),
                    }
                },
                _ => {
                    TopicError::InternalError(e.to_string())
                }
            }
        })
    }

    async fn handle_unsubscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic_filter: String,
    ) -> Result<(), TopicError> {
        self.topic_raft_client.handle_unsubscribe(tenant_id, client_identifier, topic_filter).await;
        Ok(())
    }

    async fn get_subscribers(
        &self,
        tenant_id: String,
        msg_topic: String,
    ) -> Result<Vec<Arc<Subscription>>, TopicError> {
        let r = self.topic_raft_client.get_subscribers(tenant_id, msg_topic).await.unwrap();
        Ok(r)
    }

    async fn clean_retain_publish_packet(
        &mut self,
        tenant_id: String,
        topic_filter: &String,
    ) -> Result<(), TopicError> {
        self.topic_raft_client.clean_retain_publish_packet(tenant_id, topic_filter.to_string()).await.unwrap();
        Ok(())
    }

    async fn get_tenant_names(&self) -> Vec<String> {
        let inner_storage = self.storage.read().await;
        inner_storage.get_tenant_names()
    }

    async fn register_retain_publish_packet(
        &mut self,
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: &MqttPacketV3,
    ) -> Result<(), TopicError> {
        self.topic_raft_client.register_retain_publish_packet(tenant_id, source_client_identifier, publish_packet.clone()).await;
        Ok(())
    }

    async fn get_retain_publish_packet(
        &mut self,
        tenant_id: String,
        topic_filter: String,
    ) -> Result<Vec<Arc<MqttPacketV3>>, TopicError> {
        let topic_storage = self.storage.read().await;
        topic_storage.get_retain_publish_packet(tenant_id, topic_filter)
    }

    async fn create_tenant(&mut self, tenant_id: String) -> Result<(), TopicError> {
        self.topic_raft_client.create_tenant(tenant_id).await.unwrap();
        Ok(())
    }
}

impl TopicManager {
    pub fn new(
        storage: Arc<RwLock<TopicStorage>>,
        current_node_id: NodeId,
        topic_raft_client: Arc<dyn crate::raft::client::topic::TopicRaftClientTrait>,
    ) -> Self {
        Self {
            storage,
            current_node_id,
            topic_raft_client
        }
    }
}