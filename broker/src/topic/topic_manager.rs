use std::{sync::Arc, time::Duration};

use backoff::{backoff::Backoff, ExponentialBackoff};
use log::{info, warn};
use mockall::automock;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use yedmq_mqtt::MqttPacketV3;

use crate::{protobuf::raft_service_client::RaftServiceClient, raft::{topic::{types::Request, TopicRaftManagerTrait}, NodeId}, topic::NetworkError};

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
    raft_manager: Arc<crate::raft::raft_manager::RaftManager>,
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
        info!("with raft cluster subscribe tenant_id: {} topic: {}, qos: {}, node_id: {}", tenant_id, topic_filter, qos, self.current_node_id);
        self.raft_manager.topic_raft()
            .execute_command(Request::SubscribeTopic {
                node_id: self.current_node_id,
                tenant_id,
                client_identifier,
                topic: topic_filter,
                qos,
            })
            .await
            .unwrap();
        Ok(())
    }

    async fn handle_unsubscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic_filter: String,
    ) -> Result<(), TopicError> {
        self.raft_manager.topic_raft()
            .execute_command(Request::UnsubscribeTopic {
                node_id: self.current_node_id,
                tenant_id,
                client_identifier,
                topic: topic_filter,
            })
            .await
            .unwrap();
        Ok(())
    }

    async fn get_subscribers(
        &self,
        tenant_id: String,
        msg_topic: String,
    ) -> Result<Vec<Arc<Subscription>>, TopicError> {
        let current_leader_node_id = self.raft_manager.topic_raft().get_leader_node_id().await;
        if current_leader_node_id.is_none() {
            warn!("raft get session state leader not found");
            return Ok(vec![]);
        }
        let client = self.get_grpc_client(current_leader_node_id.unwrap()).await;
        if client.is_err() {
            warn!("raft get session state grpc error: {}", client.err().unwrap());
            return Err(TopicError::NetworkError(NetworkError::Timeout));
        }
        let mut client = client.unwrap();
        let request = crate::protobuf::GetSubscriptionRequest {
            tenant_id: tenant_id.to_string(),
            topic: msg_topic.to_string(),
        };
        let res = client.get_subscriptions(request).await;
        if let Ok(res) = res {
            let response = res.into_inner();
            if response.success {
                let r = response.subscriptions.iter().map(|data| {
                    Arc::new(Subscription {
                        node_id: data.node_id,
                        client_identifier: data.client_id.clone(),
                        qos: data.qos as u8,
                    })
                }).collect();
                Ok(r)
            } else {
                Err(TopicError::InternalError(response.error.unwrap().message))
            }
        } else {
            let r = res.unwrap_err();
            Err(TopicError::NetworkError(NetworkError::GrpcError(r)))
        }
    }

    async fn clean_retain_publish_packet(
        &mut self,
        tenant_id: String,
        topic_filter: &String,
    ) -> Result<(), TopicError> {
        let _ = self
            .raft_manager.topic_raft()
            .execute_command(Request::CleanRetainPublishPacket {
                tenant_id,
                topic_filter: topic_filter.to_string(),
            })
            .await
            .unwrap();
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
        self.raft_manager.topic_raft()
            .execute_command(Request::RegisterRetainPublishPacket {
                tenant_id,
                source_client_identifier,
                publish_packet: publish_packet.clone(),
            })
            .await
            .unwrap();
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
        self.raft_manager
            .topic_raft()
            .execute_command(Request::CreateTenant { tenant_id })
            .await
            .unwrap();
        Ok(())
    }
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

    async fn get_grpc_client(&self, node_id: NodeId) -> anyhow::Result<RaftServiceClient<tonic::transport::Channel>> {
        let node = self.raft_manager.topic_raft().get_node_by_id(node_id).await.unwrap();
        let addr = format!("http://{}", node.rpc_addr);
        let client = create_rpc_client_with_retry(addr.clone()).await;
        if client.is_ok() {
            return client;
        }
        return Err(anyhow::anyhow!("Failed to connect after retries"));
    }

}

   async fn create_rpc_client_with_retry(addr: String) -> anyhow::Result<RaftServiceClient<Channel>> {
        let mut backoff = ExponentialBackoff {
            initial_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(10),
            multiplier: 2.0,
            max_elapsed_time: Some(Duration::from_secs(30)),
            ..ExponentialBackoff::default()
        };

        let channel = loop {
            match tonic::transport::Endpoint::from_shared(addr.clone())?
                .connect()
                .await
            {
                Ok(channel) => break channel,
                Err(e) => {
                    if let Some(duration) = backoff.next_backoff() {
                        warn!("RPC client connection failed: {}. Retrying in {:?}...", e, duration);
                        tokio::time::sleep(duration).await;
                    } else {
                        return Err(anyhow::anyhow!(format!("Failed to connect after retries: {}", e)));
                    }
                }
            }
        };

        Ok(RaftServiceClient::new(channel))
    }
