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

use super::types::SessionStateTypeConfig;

use crate::raft::payload::PayloadStore;
use std::sync::Arc;

pub struct Network {
    payload_store: Arc<dyn PayloadStore>,
}

impl Network {
    pub fn new(payload_store: Arc<dyn PayloadStore>) -> Self {
        Self { payload_store }
    }
}

impl RaftNetworkFactory<SessionStateTypeConfig> for Network {
    type Network = NetworkConnection;
    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        NetworkConnection::new(node, self.payload_store.clone())
    }
}

pub struct NetworkConnection {
    node: Node,
    payload_store: Arc<dyn PayloadStore>,
}

impl NetworkConnection {
    pub fn new(node: &Node, payload_store: Arc<dyn PayloadStore>) -> Self {
        NetworkConnection {
            node: node.clone(),
            payload_store,
        }
    }

    async fn c<E: std::error::Error + DeserializeOwned>(
        &mut self,
    ) -> Result<RaftServiceClient<Channel>, RPCError<NodeId, Node, E>> {
        let channel = crate::rpc::grpc_client::lazy_channel(&self.node.rpc_addr)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        Ok(RaftServiceClient::new(channel))
    }
}

use crate::protobuf::raft_payload::payload_service_client::PayloadServiceClient;
use crate::protobuf::raft_payload::{ReplicateBatchRequest, ReplicateRequest};

impl RaftNetwork<SessionStateTypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<SessionStateTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        // 1. Heartbeat Fast-Path: Non-blocking
        if req.entries.is_empty() {
            let mut c = self.c().await?;
            let resp = c.append_entries(req).await;
            let resp = resp.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            let mes = resp.into_inner();
            let resp = serde_json::from_str(&mes.data)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            return Ok(resp);
        }

        // 2. Prepare Batch Payload
        let mut batch_entries = Vec::new();
        let mut total_size: usize = 0;

        for entry in &req.entries {
            if let openraft::EntryPayload::Normal(data) = &entry.payload {
                let key = match data {
                    crate::raft::session_state::types::SessionStateRequest::InflightRegisterRxPacket { packet_key, .. } => Some(packet_key),
                    crate::raft::session_state::types::SessionStateRequest::InflightRegisterTxPacket { packet_key, .. } => Some(packet_key),
                    crate::raft::session_state::types::SessionStateRequest::AppendToPendingQueue { packet_key, .. } => Some(packet_key),
                    _ => None,
                };

                if let Some(k) = key {
                    match self.payload_store.get(k).await {
                        Ok(Some(data)) => {
                            total_size += data.len();
                            // log::debug!("Preparing to replicate payload key: {}", k);
                            batch_entries.push(ReplicateRequest {
                                key: k.clone(),
                                data: data.to_vec(),
                                term: entry.log_id.leader_id.term,
                            });
                        }
                        Ok(None) => {
                            let msg = format!("Critical: Leader missing payload for key: {}", k);
                            log::error!("{}", msg);
                            return Err(RPCError::Network(NetworkError::new(
                                &std::io::Error::new(std::io::ErrorKind::NotFound, msg),
                            )));
                        }
                        Err(e) => {
                            log::error!("Error reading payload store: {}", e);
                            return Err(RPCError::Network(NetworkError::new(
                                &std::io::Error::other(e.to_string()),
                            )));
                        }
                    }
                }
            }
        }

        // 3. Synchronous Batching: Payload Push First -> Then Log Append
        // This prevents the "Payload missing" race condition at the Follower.
        if !batch_entries.is_empty() {
            // Connect and Replicate
            let payload_result = async {
                let mut client =
                    PayloadServiceClient::new(crate::rpc::grpc_client::lazy_channel(
                        &self.node.rpc_addr,
                    )
                    .map_err(|e| NetworkError::new(&e))?);

                log::info!(
                    "Sending ReplicateBatchRequest with {} entries to {}",
                    batch_entries.len(),
                    self.node.rpc_addr
                );

                let batch_req = ReplicateBatchRequest {
                    entries: batch_entries,
                };

                // Dynamic Timeout
                let size_mb = (total_size as f64) / (1024.0 * 1024.0);
                let additional_timeout = (size_mb * 100.0) as u64;
                let timeout_duration = std::time::Duration::from_millis(100 + additional_timeout);

                tokio::time::timeout(timeout_duration, client.replicate_batch(batch_req))
                    .await
                    .map_err(|_| {
                        NetworkError::new(&std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("Payload push timed out (size: {} bytes)", total_size),
                        ))
                    })?
                    .map_err(|status| NetworkError::new(&status))
            }
            .await;

            if let Err(e) = payload_result {
                log::error!(
                    "Payload push failed: {}. Aborting Log Append to maintain consistency.",
                    e
                );
                return Err(RPCError::Network(e));
            }
        }

        // 4. Log Append
        let mut c = self.c().await?;
        let resp = c.append_entries(req).await;
        let resp = resp.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str::<AppendEntriesResponse<NodeId>>(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<SessionStateTypeConfig>,
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
            raft_type: crate::protobuf::RaftType::SessionState.into(),
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
