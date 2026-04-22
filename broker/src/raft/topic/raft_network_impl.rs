use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use tonic::transport::Channel;

use crate::protobuf::raft_service_client::RaftServiceClient;

use crate::raft::{Node, NodeId};

use super::types::TypeConfig;

pub struct Network {}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        NetworkConnection::new(node)
    }
}

pub struct NetworkConnection {
    node: Node,
}

impl NetworkConnection {
    pub fn new(node: &Node) -> Self {
        NetworkConnection { node: node.clone() }
    }

    async fn c<E: std::error::Error + DeserializeOwned>(
        &mut self,
    ) -> Result<RaftServiceClient<Channel>, RPCError<NodeId, Node, E>> {
        let channel = crate::rpc::grpc_client::lazy_channel(&self.node.rpc_addr)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        Ok(RaftServiceClient::new(channel))
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let mut c = self.c().await?;

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
        let mut c = self.c().await?;
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
        let mut c = self.c().await?;

        let data =
            serde_json::to_string(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mes = crate::protobuf::VoteRequest {
            data,
            raft_type: crate::protobuf::RaftType::Topic.into(),
        };
        let request = tonic::Request::new(mes);
        let resp = c.vote(request).await;

        let resp = resp.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }
}
