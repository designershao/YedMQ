use std::sync::Arc;

use mockall::automock;
use tonic::transport::Channel;
use yedmq_mqtt::MqttPacketV3;

use crate::{
    protobuf::{raft_service_client::RaftServiceClient, RaftType},
    topic::topic_storage::Subscription,
};

use super::base::{BaseRaftClient, RaftClient, RaftClientError};

pub struct TopicRaftClient {
    base_client: BaseRaftClient<crate::raft::topic::types::TypeConfig>,
}

#[async_trait::async_trait]
#[automock]
pub trait TopicRaftClientTrait: Sync + Send {
    async fn handle_subscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic: String,
        qos: u8,
    ) -> Result<(), RaftClientError>;

    async fn handle_unsubscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic: String,
    ) -> Result<(), RaftClientError>;

    async fn get_subscribers(
        &self,
        tenant_id: String,
        msg_topic: String,
    ) -> Result<Vec<Arc<Subscription>>, RaftClientError>;

    async fn register_retain_publish_packet(
        &self,
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: MqttPacketV3,
    ) -> Result<(), RaftClientError>;

    async fn clean_retain_publish_packet(
        &self,
        tenant_id: String,
        topic_filter: String,
    ) -> Result<(), RaftClientError>;

    async fn create_tenant(&self, tenant_id: String) -> Result<(), RaftClientError>;
}

#[async_trait::async_trait]
impl RaftClient<crate::raft::topic::types::TypeConfig> for TopicRaftClient {
    fn get_raft_type(&self) -> RaftType {
        RaftType::Topic
    }

    async fn get_client(&self) -> super::base::Result<RaftServiceClient<Channel>> {
        self.base_client.get_client().await
    }

    async fn append_entries(&self, data: String) -> super::base::Result<String> {
        self.base_client.append_entries(data).await
    }

    async fn vote(&self, data: String) -> super::base::Result<String> {
        self.base_client.vote(data).await
    }

    async fn install_snapshot(&self, data: String) -> super::base::Result<String> {
        self.base_client.install_snapshot(data).await
    }

    async fn reconnect(&self) -> super::base::Result<()> {
        self.base_client.reconnect().await
    }

    async fn get_leader(&self) -> Option<crate::raft::Node> {
        self.base_client.get_leader().await
    }

    async fn get_current_node_id(&self) -> crate::raft::NodeId {
        self.base_client.get_current_node_id().await
    }
}

impl TopicRaftClient {
    pub fn new(raft: Arc<crate::raft::topic::types::TopicRaft>) -> Self {
        Self {
            base_client: BaseRaftClient::new(RaftType::Topic, raft),
        }
    }
}

#[async_trait::async_trait]
impl TopicRaftClientTrait for TopicRaftClient {
    async fn handle_subscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic_filter: String,
        qos: u8,
    ) -> Result<(), RaftClientError> {
        let request = crate::raft::topic::types::Request::SubscribeTopic {
            node_id: self.get_current_node_id().await,
            tenant_id,
            client_identifier,
            topic: topic_filter,
            qos,
        };

        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }

    async fn handle_unsubscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic_filter: String,
    ) -> Result<(), RaftClientError> {
        let request = crate::raft::topic::types::Request::UnsubscribeTopic {
            node_id: self.get_current_node_id().await,
            tenant_id,
            client_identifier,
            topic: topic_filter,
        };

        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }

    async fn get_subscribers(
        &self,
        tenant_id: String,
        msg_topic: String,
    ) -> Result<Vec<Arc<Subscription>>, RaftClientError> {
        let mut client = self.get_client().await.unwrap();

        let request = crate::protobuf::GetSubscriptionRequest {
            tenant_id: tenant_id.to_string(),
            topic: msg_topic.to_string(),
        };
        let res = client.get_subscriptions(request).await;
        if let Ok(res) = res {
            let response = res.into_inner();
            if response.success {
                let r = response
                    .subscriptions
                    .iter()
                    .map(|data| {
                        Arc::new(Subscription {
                            node_id: data.node_id,
                            client_identifier: data.client_id.clone(),
                            qos: data.qos as u8,
                        })
                    })
                    .collect();
                Ok(r)
            } else {
                let err_detail = response.error.unwrap();
                let error = RaftClientError::ServiceError {
                    code: err_detail.code(),
                    message: err_detail.message.clone(),
                    node: err_detail.node,
                };
                std::result::Result::Err(error)
            }
        } else {
            std::result::Result::Err(RaftClientError::GrpcError(res.unwrap_err()))
        }
    }

    async fn register_retain_publish_packet(
        &self,
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: MqttPacketV3,
    ) -> Result<(), RaftClientError> {
        let request = crate::raft::topic::types::Request::RegisterRetainPublishPacket {
            tenant_id,
            source_client_identifier,
            publish_packet,
        };

        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }

    async fn clean_retain_publish_packet(
        &self,
        tenant_id: String,
        topic_filter: String,
    ) -> Result<(), RaftClientError> {
        let request = crate::raft::topic::types::Request::CleanRetainPublishPacket {
            tenant_id,
            topic_filter,
        };
        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }

    async fn create_tenant(&self, tenant_id: String) -> Result<(), RaftClientError> {
        let request = crate::raft::topic::types::Request::CreateTenant { tenant_id };
        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }
}
