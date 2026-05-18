use std::sync::Arc;

use actix::{Addr, MailboxError, SystemService};
use thiserror::Error;
use tonic::transport::Channel;
use yedmq_mqtt::packet::Packet;

use crate::protobuf::cluster_service_client::ClusterServiceClient;
use crate::protobuf::{
    CleanRetainPublishMessageRequest, GetRetainPublishMessageRequest, GetSubscribersByTopicRequest,
    RegisterRetainPublishMessageRequest, SubscribeTopicRequest, UnsubscribeTopicRequest,
};
use crate::raft::topic::topic_raft_actor::{
    CleanRetainPublishPacket, GetRetainPublishPacketEnsureLinearizable,
    GetSubscriptionsEnsureLinearizable, GetSubscriptionsResponse, RegisterRetainPublishPacket,
    Subscribe, TopicRaftActor, TopicRaftError, Unsubscribe,
};
use crate::stored_packet::{
    deserialize_stored_packet_list_from_str, serialize_stored_packet_to_string,
};

#[derive(Debug, Error)]
pub enum TopicServiceError {
    #[error("topic actor mailbox error: {0}")]
    Mailbox(String),

    #[error("topic raft error: {0}")]
    TopicRaft(#[from] TopicRaftError),

    #[error("gRPC status error: {0}")]
    GRPC(#[from] tonic::Status),

    #[error("gRPC connect error: {0}")]
    GRPCConnect(String),

    #[error("serialization error: {0}")]
    Serialize(String),
}

impl From<MailboxError> for TopicServiceError {
    fn from(value: MailboxError) -> Self {
        Self::Mailbox(value.to_string())
    }
}

#[derive(Clone)]
pub struct TopicService {
    topic_raft_actor: Addr<TopicRaftActor>,
}

impl TopicService {
    pub fn new(topic_raft_actor: Addr<TopicRaftActor>) -> Self {
        Self { topic_raft_actor }
    }

    pub fn from_registry() -> Self {
        Self::new(TopicRaftActor::from_registry())
    }

    async fn connect_cluster_client(
        leader_addr: &str,
    ) -> Result<ClusterServiceClient<Channel>, TopicServiceError> {
        crate::rpc::grpc_client::lazy_channel(leader_addr)
            .map(ClusterServiceClient::new)
            .map_err(|e| TopicServiceError::GRPCConnect(e.to_string()))
    }

    pub async fn subscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic: String,
        qos: u8,
    ) -> Result<(), TopicServiceError> {
        self.subscribe_with_options(tenant_id, client_identifier, topic, qos, false, false)
            .await
    }

    pub async fn subscribe_with_options(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic: String,
        qos: u8,
        no_local: bool,
        retain_as_published: bool,
    ) -> Result<(), TopicServiceError> {
        let result = self
            .topic_raft_actor
            .send(Subscribe {
                tenant_id: tenant_id.clone(),
                client_identifier: client_identifier.clone(),
                topic: topic.clone(),
                qos,
                no_local,
                retain_as_published,
            })
            .await?;

        match result {
            Ok(()) => Ok(()),
            Err(TopicRaftError::NotLeader {
                leader: Some(leader),
            }) => {
                let mut client = Self::connect_cluster_client(&leader.rpc_addr).await?;
                client
                    .subscribe_topic(SubscribeTopicRequest {
                        node_id: 0,
                        tenant_id,
                        client_id: client_identifier,
                        topic,
                        qos: qos as u32,
                        no_local,
                        retain_as_published,
                    })
                    .await?;
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn unsubscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic: String,
    ) -> Result<(), TopicServiceError> {
        let result = self
            .topic_raft_actor
            .send(Unsubscribe {
                tenant_id: tenant_id.clone(),
                client_identifier: client_identifier.clone(),
                topic: topic.clone(),
            })
            .await?;

        match result {
            Ok(()) => Ok(()),
            Err(TopicRaftError::NotLeader {
                leader: Some(leader),
            }) => {
                let mut client = Self::connect_cluster_client(&leader.rpc_addr).await?;
                client
                    .unsubscribe_topic(UnsubscribeTopicRequest {
                        tenant_id,
                        client_id: client_identifier,
                        topic,
                    })
                    .await?;
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn register_retain_publish_packet(
        &self,
        tenant_id: String,
        client_id: String,
        publish_packet: Packet,
    ) -> Result<(), TopicServiceError> {
        let result = self
            .topic_raft_actor
            .send(RegisterRetainPublishPacket {
                tenant_id: tenant_id.clone(),
                client_id: client_id.clone(),
                publish_packet: publish_packet.clone(),
            })
            .await?;

        match result {
            Ok(()) => Ok(()),
            Err(TopicRaftError::NotLeader {
                leader: Some(leader),
            }) => {
                let payload = serialize_stored_packet_to_string(&publish_packet)
                    .map_err(|e| TopicServiceError::Serialize(e.to_string()))?;
                let mut client = Self::connect_cluster_client(&leader.rpc_addr).await?;
                client
                    .register_retain_publish_message(RegisterRetainPublishMessageRequest {
                        tenant_id,
                        client_id,
                        payload,
                    })
                    .await?;
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn clean_retain_publish_packet(
        &self,
        tenant_id: String,
        topic_filter: String,
    ) -> Result<(), TopicServiceError> {
        let result = self
            .topic_raft_actor
            .send(CleanRetainPublishPacket {
                tenant_id: tenant_id.clone(),
                topic_filter: topic_filter.clone(),
            })
            .await?;

        match result {
            Ok(()) => Ok(()),
            Err(TopicRaftError::NotLeader {
                leader: Some(leader),
            }) => {
                let mut client = Self::connect_cluster_client(&leader.rpc_addr).await?;
                client
                    .clean_retain_publish_message(CleanRetainPublishMessageRequest {
                        tenant_id,
                        topic: topic_filter,
                    })
                    .await?;
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn get_subscriptions_linearizable(
        &self,
        tenant_id: String,
        topic: String,
    ) -> Result<GetSubscriptionsResponse, TopicServiceError> {
        let result = self
            .topic_raft_actor
            .send(GetSubscriptionsEnsureLinearizable {
                tenant_id: tenant_id.clone(),
                topic: topic.clone(),
            })
            .await?;

        match result {
            Ok(response) => Ok(response),
            Err(TopicRaftError::NotLeader {
                leader: Some(leader),
            }) => {
                let mut client = Self::connect_cluster_client(&leader.rpc_addr).await?;
                let response = client
                    .get_subscribers_by_topic(GetSubscribersByTopicRequest { tenant_id, topic })
                    .await?;
                let subscriptions = response
                    .into_inner()
                    .payload
                    .into_iter()
                    .map(
                        |subscriber| crate::raft::topic::topic_raft_actor::SubscriptionInfo {
                            client_identifier: subscriber.client_id,
                            qos: subscriber.qos as u8,
                            no_local: subscriber.no_local,
                            retain_as_published: subscriber.retain_as_published,
                        },
                    )
                    .collect();

                Ok(GetSubscriptionsResponse { subscriptions })
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn get_retain_publish_packets_linearizable(
        &self,
        tenant_id: String,
        topic: String,
    ) -> Result<Vec<Arc<Packet>>, TopicServiceError> {
        let result = self
            .topic_raft_actor
            .send(GetRetainPublishPacketEnsureLinearizable {
                tenant_id: tenant_id.clone(),
                topic: topic.clone(),
            })
            .await?;

        match result {
            Ok(response) => Ok(response),
            Err(TopicRaftError::NotLeader {
                leader: Some(leader),
            }) => {
                let mut client = Self::connect_cluster_client(&leader.rpc_addr).await?;
                let response = client
                    .get_retain_publish_message(GetRetainPublishMessageRequest { tenant_id, topic })
                    .await?;
                match response.into_inner().payload {
                    Some(payload) => deserialize_stored_packet_list_from_str(&payload)
                        .map_err(|e| TopicServiceError::Serialize(e.to_string())),
                    None => Ok(vec![]),
                }
            }
            Err(err) => Err(err.into()),
        }
    }
}
