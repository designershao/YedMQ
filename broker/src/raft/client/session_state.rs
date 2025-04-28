use std::sync::Arc;

use mockall::automock;
use tonic::transport::Channel;
use yedmq_mqtt::MqttPacketV3;

use crate::{
    protobuf::{raft_service_client::RaftServiceClient, RaftType},
    raft::session_state::types::SessionStateResponse,
    session::session_state_storage::SessionState,
};

use super::base::{BaseRaftClient, RaftClient};

pub struct SessionStateRaftClient {
    base_client: BaseRaftClient<crate::raft::session_state::types::SessionStateTypeConfig>,
}

impl SessionStateRaftClient {
    pub fn new(raft: Arc<crate::raft::session_state::SessionStateRaft>) -> Self {
        Self {
            base_client: BaseRaftClient::new(RaftType::SessionState, raft),
        }
    }
}

#[async_trait::async_trait]
impl RaftClient<crate::raft::session_state::types::SessionStateTypeConfig>
    for SessionStateRaftClient
{
    fn get_raft_type(&self) -> RaftType {
        RaftType::SessionState
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

#[async_trait::async_trait]
#[automock]
pub trait SessionStateRaftClientTrait {
    async fn pop_from_pending_queue(
        &self,
        tenant_id: String,
        client_id: String,
    ) -> super::base::Result<Option<MqttPacketV3>>;

    async fn session_state_exists(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> super::base::Result<bool>;

    async fn get_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> super::base::Result<Option<SessionState>>;

    async fn create_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
        inflight_duration: u64,
    ) -> super::base::Result<()>;


    async fn delete_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> super::base::Result<()>;

    async fn inflight_register_rx_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> super::base::Result<()>;

    async fn inflight_register_tx_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> super::base::Result<()>;

    async fn inflight_get_current_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet_identifier: u16,
    ) -> super::base::Result<Option<MqttPacketV3>>;


    async fn inflight_next_state(&self, tenant_id: &str, client_id: &str, packet_identifier: u16) -> super::base::Result<()>;

    async fn inflight_clean_finished_items(&self, tenant_id: &str, client_id: &str) -> super::base::Result<()>;

    async fn append_to_pending_queue(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> super::base::Result<()>;

    async fn subscribe_topic(&self, tenant_id: String, client_id: String, topic: String, qos: u8) -> super::base::Result<()>;

    async fn unsubscribe_topic(&self, tenant_id: String, client_id: String, topic: String) -> super::base::Result<()>;

}

#[async_trait::async_trait]
impl SessionStateRaftClientTrait for SessionStateRaftClient {
    async fn pop_from_pending_queue(
        &self,
        tenant_id: String,
        client_id: String,
    ) -> super::base::Result<Option<MqttPacketV3>> {
        let request = crate::raft::session_state::types::SessionStateRequest::PopFromPendingQueue {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        };

        let data = serde_json::to_string(&request).unwrap();

        let r = self.append_entries(data).await?;

        let res: SessionStateResponse = serde_json::from_str(&r.as_str()).unwrap();

        match res {
            SessionStateResponse::PopFromPendingQueueResult(packet) => Ok(packet),
            _ => Err(super::base::RaftClientError::InternalError(
                "pop from pending queue error, unexpected response".to_string(),
            )),
        }
    }

    async fn session_state_exists(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> super::base::Result<bool> {
        let mut client = self.get_client().await.unwrap();

        let request = crate::protobuf::SessionExistedRequest {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        };

        let res = client.session_state_existed(request).await;
        if let Ok(res) = res {
            let response = res.into_inner();
            if response.success {
                let r = response.session_existed;
                Ok(r)
            } else {
                let err_detail = response.error.unwrap();
                let error = super::base::RaftClientError::ServiceError {
                    code: err_detail.code(),
                    message: err_detail.message.clone(),
                    node: err_detail.node,
                };
                std::result::Result::Err(error)
            }
        } else {
            std::result::Result::Err(super::base::RaftClientError::GrpcError(res.unwrap_err()))
        }
    }

    async fn get_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> super::base::Result<Option<SessionState>> {
        let mut client = self.get_client().await.unwrap();
        let res = client
            .get_session_state(crate::protobuf::GetSessionStateRequest {
                tenant_id: tenant_id.to_string(),
                client_id: client_id.to_string(),
            })
            .await;

        if let Ok(res) = res {
            let response = res.into_inner();
            if response.success {
                let r = response.session_state_data;
                let session_state: SessionState = serde_json::from_str(&r.unwrap()).unwrap();
                Ok(Some(session_state))
            } else {
                let err_detail = response.error.unwrap();
                let error = super::base::RaftClientError::ServiceError {
                    code: err_detail.code(),
                    message: err_detail.message.clone(),
                    node: err_detail.node,
                };
                std::result::Result::Err(error)
            }
        } else {
            std::result::Result::Err(super::base::RaftClientError::GrpcError(res.unwrap_err()))
        }
    }

    async fn create_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
        inflight_duration: u64,
    ) -> super::base::Result<()> {
        let request = crate::raft::session_state::types::SessionStateRequest::CreateSessionState {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            inflight_duration_secs: inflight_duration,
        };

        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }

    async fn delete_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> super::base::Result<()> {
        let request = crate::raft::session_state::types::SessionStateRequest::DeleteSessionState {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        };

        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }

    async fn inflight_register_rx_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> super::base::Result<()> {
        let request = crate::raft::session_state::types::SessionStateRequest::InflightRegisterRxPacket {
                tenant_id: tenant_id.to_string(),
                client_id: client_id.to_string(),
                packet,
            };

        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }

    async fn inflight_register_tx_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> super::base::Result<()> {
        let request = crate::raft::session_state::types::SessionStateRequest::InflightRegisterTxPacket {
                tenant_id: tenant_id.to_string(),
                client_id: client_id.to_string(),
                packet,
            };

        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }


    async fn inflight_get_current_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet_identifier: u16,
    ) -> super::base::Result<Option<MqttPacketV3>> {
        let mut client = self.get_client().await.unwrap();
        let res = client
            .inflight_get_current_packet(crate::protobuf::InflightGetCurrentPacketRequest {
                tenant_id: tenant_id.to_string(),
                client_id: client_id.to_string(),
                packet_id: packet_identifier.into(),
            })
            .await;
        if let Ok(res) = res {
            let response = res.into_inner();
            if response.success {
                Ok(response.packet.and_then(|packet| {
                    let packet: MqttPacketV3 = serde_json::from_str(&packet).unwrap();
                    Some(packet)
                }))
            } else {
                let err_detail = response.error.unwrap();
                let error = super::base::RaftClientError::ServiceError {
                    code: err_detail.code(),
                    message: err_detail.message.clone(),
                    node: err_detail.node,
                };
                std::result::Result::Err(error)
            }
        } else {
            std::result::Result::Err(super::base::RaftClientError::GrpcError(res.unwrap_err()))
        }        

    }

    async fn inflight_next_state(&self, tenant_id: &str, client_id: &str, packet_identifier: u16) -> super::base::Result<()> {
        let request = crate::raft::session_state::types::SessionStateRequest::InflightNextState {
                tenant_id: tenant_id.to_string(),
                client_id: client_id.to_string(),
                packet_identifier: packet_identifier.into(),
            };

        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())

    }
   async fn inflight_clean_finished_items(&self, tenant_id: &str, client_id: &str) -> super::base::Result<()> {
        let request = crate::raft::session_state::types::SessionStateRequest::InflightCleanFinishItems {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        };

        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }

    async fn append_to_pending_queue(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> super::base::Result<()> {
        let request = crate::raft::session_state::types::SessionStateRequest::AppendToPendingQueue {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            packet,
        };

        let data = serde_json::to_string(&request).unwrap();

        self.append_entries(data).await?;

        Ok(())
    }

    async fn subscribe_topic(&self, tenant_id: String, client_id: String, topic: String, qos: u8) -> super::base::Result<()> {
        let request = crate::raft::session_state::types::SessionStateRequest::SubscribeTopic {
            tenant_id,
            client_id,
            topic,
            qos,
        };

        self.append_entries(serde_json::to_string(&request).unwrap()).await?;
        Ok(())
    }

    async fn unsubscribe_topic(&self, tenant_id: String, client_id: String, topic: String) -> super::base::Result<()> {
        let request = crate::raft::session_state::types::SessionStateRequest::UnsubscribeTopic {
            tenant_id,
            client_id,
            topic,
        };

        self.append_entries(serde_json::to_string(&request).unwrap()).await?;
        Ok(())
    }

}
