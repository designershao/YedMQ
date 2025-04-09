use std::time::Duration;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{AnyError, RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use tonic::transport::{Channel, Endpoint};

use crate::protobuf::raft_service_client::RaftServiceClient;

use crate::raft::{Node, NodeId};

use super::types::TypeConfig;

pub struct Network {}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        let addr = format!("http://{}", node.rpc_addr);

        let channel = Endpoint::from_shared(addr.clone()).unwrap()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(5))
            .concurrency_limit(8)
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true)
            .connect_lazy();

        NetworkConnection {
            channel,
        }
    }
}

pub struct NetworkConnection {
    channel: Channel,
}

impl NetworkConnection {
    async fn c<E: std::error::Error + DeserializeOwned>(
        &mut self,
    ) -> Result<RaftServiceClient<Channel>, RPCError<NodeId, Node, E>> {
        Ok(RaftServiceClient::new(self.channel.clone()))
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

        let mes = crate::protobuf::VoteRequest {
            data: serde_json::to_string(&req).expect("fail to serialize"),
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
