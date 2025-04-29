use std::sync::Arc;

use tonic::transport::Channel;

use crate::{protobuf::{raft_service_client::RaftServiceClient, RaftType}, raft::{session_actor_map::types::SessionActorMapResponse, NodeId}, session::session_actor_map_storage::SessionVersion};

use super::base::{BaseRaftClient, RaftClient, RaftClientError};

pub struct SessionActorMapRaftClient {
    base_client: BaseRaftClient<crate::raft::session_actor_map::types::SessionActorMapTypeConfig>,
}

impl SessionActorMapRaftClient {
    pub fn new(raft: Arc<crate::raft::session_actor_map::SessionActorMapRaft>) -> Self {
        Self {
            base_client: BaseRaftClient::new(RaftType::SessionActorMap, raft),
        }
    }
}

#[async_trait::async_trait]
pub trait SessionActorMapRaftClientTrait: Sync + Send {

    async fn register_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftClientError>;

    async fn unregister_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftClientError>;
}

#[async_trait::async_trait]
impl RaftClient<crate::raft::session_actor_map::types::SessionActorMapTypeConfig> for SessionActorMapRaftClient  {

    fn get_raft_type(&self) -> RaftType {
        RaftType::SessionActorMap
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

    async fn install_snapshot(&self, data: String) -> super::base::Result<String>  {
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
impl SessionActorMapRaftClientTrait for SessionActorMapRaftClient {

    async fn register_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftClientError> {

        let request = crate::raft::session_actor_map::types::SessionActorMapRequest::RegisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            node_id,
            version,
        };

        let data = serde_json::to_string(&request).unwrap();

        let r = self.append_entries(data).await?;

        Ok(serde_json::from_str(&r.as_str()).unwrap())
    }

    async fn unregister_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftClientError> {
         let request = crate::raft::session_actor_map::types::SessionActorMapRequest::UnregisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            session_version: version,
        };

        let data = serde_json::to_string(&request).unwrap();

        let r = self.append_entries(data).await?;

        Ok(serde_json::from_str(&r.as_str()).unwrap())
    }
}