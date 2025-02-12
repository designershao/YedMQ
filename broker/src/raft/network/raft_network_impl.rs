use std::sync::Arc;

use axum::async_trait;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{AnyError, RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use tonic::transport::Channel;

use crate::protobuf::raft_service_client::RaftServiceClient;

use crate::raft::{Node, NodeId, TypeConfig};

pub struct Network {}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        let addr = format!("http://{}", node.rpc_addr);

        let client = RaftServiceClient::connect(addr.clone()).await.unwrap();

        NetworkConnection {
            rpc_addr: addr.clone(),
            client: Some(client),
        }
    }
}

pub struct NetworkConnection {
    rpc_addr: String,
    client: Option<RaftServiceClient<Channel>>,
}

impl NetworkConnection {
    async fn c<E: std::error::Error + DeserializeOwned>(
        &mut self,
    ) -> Result<&mut RaftServiceClient<Channel>, RPCError<NodeId, Node, E>> {
        if self.client.is_none() {
            self.client = Some(
                RaftServiceClient::connect(self.rpc_addr.clone())
                    .await
                    .unwrap(),
            );
        }
        self.client
            .as_mut()
            .ok_or_else(|| RPCError::Network(NetworkError::from(AnyError::default())))
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let c = self.c().await?;

        let resp = c.append_entries(req).await;
        let resp = resp.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let c = self.c().await?;
        let resp = c.install_snapshot(req).await;

        let resp = resp.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(resp)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let c = self.c().await?;
        let resp = c.vote(req).await;

        let resp = resp.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }
}
