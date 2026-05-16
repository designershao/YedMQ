use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};
use std::time::Duration;
use tonic::transport::Channel;

use crate::protobuf::raft_service_client::RaftServiceClient;

use crate::raft::{Node, NodeId};

use super::types::SessionActorMapTypeConfig;

pub struct Network {}

impl RaftNetworkFactory<SessionActorMapTypeConfig> for Network {
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

    fn c<E: std::error::Error>(
        &mut self,
    ) -> Result<RaftServiceClient<Channel>, RPCError<NodeId, Node, E>> {
        let channel = crate::rpc::grpc_client::lazy_channel_with_connect_timeout(
            &self.node.rpc_addr,
            Duration::from_secs(1),
        )
        .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        Ok(RaftServiceClient::new(channel))
    }
}

fn map_grpc_status<E: std::error::Error>(status: tonic::Status) -> RPCError<NodeId, Node, E> {
    if status.code() == tonic::Code::Unavailable {
        RPCError::Unreachable(Unreachable::new(&status))
    } else {
        RPCError::Network(NetworkError::new(&status))
    }
}

fn map_grpc_response<T, E: std::error::Error>(
    resp: Result<tonic::Response<T>, tonic::Status>,
) -> Result<T, RPCError<NodeId, Node, E>> {
    resp.map(|resp| resp.into_inner()).map_err(map_grpc_status)
}

impl RaftNetwork<SessionActorMapTypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<SessionActorMapTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let mut c = self.c()?;

        let resp = c.append_entries(req).await;
        let mes = map_grpc_response(resp)?;
        let resp = serde_json::from_str(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<SessionActorMapTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let mut c = self.c()?;
        let resp = c.install_snapshot(req).await;

        let mes = map_grpc_response(resp)?;
        let resp = serde_json::from_str(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(resp)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let mut c = self.c()?;

        let data =
            serde_json::to_string(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mes = crate::protobuf::VoteRequest {
            data,
            raft_type: crate::protobuf::RaftType::SessionActorMap.into(),
        };
        let request = tonic::Request::new(mes);
        let resp = c.vote(request).await;

        let mes = map_grpc_response(resp)?;
        let resp = serde_json::from_str(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }
}
