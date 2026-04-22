use crate::protobuf::raft_service_server::RaftService;
use crate::protobuf::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse, WriteRequest, WriteResponse,
};
use crate::raft::payload::PayloadStore;
use crate::rpc::grpc_status;
use actix::SystemService;
use log::{error, warn};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct RustServiceImpl {
    pub store: Arc<dyn PayloadStore>,
}

fn map_topic_raft_error(
    action: &str,
    err: crate::raft::topic::topic_raft_actor::TopicRaftError,
) -> Status {
    match err {
        crate::raft::topic::topic_raft_actor::TopicRaftError::NotLeader {
            leader: Some(leader),
        } => grpc_status::leader_redirect_status(
            format!("{} requires leader handling at {}", action, leader.rpc_addr),
            leader.rpc_addr,
            leader.node_id,
        ),
        crate::raft::topic::topic_raft_actor::TopicRaftError::NotLeader { leader: None }
        | crate::raft::topic::topic_raft_actor::TopicRaftError::NoLeaderAvailable => {
            grpc_status::no_leader_status(format!("{} failed: no leader available", action))
        }
        crate::raft::topic::topic_raft_actor::TopicRaftError::NotInitialized => {
            grpc_status::not_ready_status(format!("{} failed: topic raft not initialized", action))
        }
        crate::raft::topic::topic_raft_actor::TopicRaftError::NotReady(message) => {
            grpc_status::not_ready_status(format!("{} failed: {}", action, message))
        }
        crate::raft::topic::topic_raft_actor::TopicRaftError::InvalidTopicName { topic } => {
            grpc_status::business_status(
                tonic::Code::InvalidArgument,
                grpc_status::business_detail(
                    crate::protobuf::ErrorCode::TopicInvalidName,
                    format!("invalid topic name: {}", topic),
                    "topic_raft".to_string(),
                ),
            )
        }
        other => grpc_status::fatal_status(
            tonic::Code::Internal,
            format!("{} failed: {}", action, other),
        ),
    }
}

fn map_session_actor_map_raft_error(
    action: &str,
    err: crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError,
) -> Status {
    match err {
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NotLeader {
            leader: Some(leader),
        } => grpc_status::leader_redirect_status(
            format!("{} requires leader handling at {}", action, leader.rpc_addr),
            leader.rpc_addr,
            leader.node_id,
        ),
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NotLeader {
            leader: None,
        }
        | crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NoLeaderAvailable => {
            grpc_status::no_leader_status(format!("{} failed: no leader available", action))
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NotInitialized => {
            grpc_status::not_ready_status(format!("{} failed: session actor map raft not initialized", action))
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NotReady(message) => {
            grpc_status::not_ready_status(format!("{} failed: {}", action, message))
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::ServiceUnavailable(message)
        | crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::GRPCConnect(message) => {
            Status::unavailable(format!("{} failed: {}", action, message))
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::SessionVersionRejected {
            current_version,
            existing_version,
        } => grpc_status::business_status(
            tonic::Code::FailedPrecondition,
            grpc_status::business_detail(
                crate::protobuf::ErrorCode::SessionVersionRejected,
                format!(
                    "Session version rejected, current: {}-{}, existing: {}-{}",
                    current_version.counter,
                    current_version.node_id,
                    existing_version.counter,
                    existing_version.node_id
                ),
                "session_actor_map_raft".to_string(),
            ),
        ),
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::TenantNotFound {
            tenant_id,
        } => grpc_status::business_status(
            tonic::Code::NotFound,
            grpc_status::business_detail(
                crate::protobuf::ErrorCode::SessionTenantNotFound,
                format!("tenant not found: {}", tenant_id),
                "session_actor_map_raft".to_string(),
            ),
        ),
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::GRPCBusiness(err) => {
            grpc_status::business_status(
                err.grpc_code(),
                grpc_status::business_detail(err.code(), err.message(), err.node()),
            )
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::GRPC(status) => {
            status
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::Serialize(message) => {
            Status::internal(format!("{} serialization failed: {}", action, message))
        }
        other => grpc_status::fatal_status(
            tonic::Code::Internal,
            format!("{} failed: {}", action, other),
        ),
    }
}

fn map_session_state_raft_error(
    action: &str,
    err: crate::raft::session_state::session_state_raft_actor::SessionStateRaftError,
) -> Status {
    match err {
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NotLeader {
            leader: Some(leader),
        } => grpc_status::leader_redirect_status(
            format!("{} requires leader handling at {}", action, leader.rpc_addr),
            leader.rpc_addr,
            leader.node_id,
        ),
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NotLeader {
            leader: None,
        }
        | crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NoLeaderAvailable => {
            grpc_status::no_leader_status(format!("{} failed: no leader available", action))
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NotInitialized => {
            grpc_status::not_ready_status(format!("{} failed: session state raft not initialized", action))
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NotReady(message) => {
            grpc_status::not_ready_status(format!("{} failed: {}", action, message))
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::ServiceUnavailable(message)
        | crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::GRPCConnect(message) => {
            Status::unavailable(format!("{} failed: {}", action, message))
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::SessionStateNotExisted(message) => {
            grpc_status::business_status(
                tonic::Code::NotFound,
                grpc_status::business_detail(
                    crate::protobuf::ErrorCode::SessionStateNotFound,
                    message,
                    "session_state_raft".to_string(),
                ),
            )
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::InflightError(
            crate::inflight::InflightError::PacketIdentifierHasExisted,
        ) => grpc_status::business_status(
            tonic::Code::AlreadyExists,
            grpc_status::business_detail(
                crate::protobuf::ErrorCode::PacketIdentifierAlreadyExists,
                "packet identifier has existed",
                "session_state_raft".to_string(),
            ),
        ),
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::GRPCBusiness(err) => {
            grpc_status::business_status(
                err.grpc_code(),
                grpc_status::business_detail(err.code(), err.message(), err.node()),
            )
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::GRPC(status) => {
            status
        }
        other => grpc_status::fatal_status(
            tonic::Code::Internal,
            format!("{} failed: {}", action, other),
        ),
    }
}

#[tonic::async_trait]
impl RaftService for RustServiceImpl {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let inner = request.into_inner();

        match inner.raft_type() {
            crate::protobuf::RaftType::Topic => {
                let topic_raft_actor_addr =
                    crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
                let command: crate::raft::topic::types::Request = serde_json::from_str(&inner.data)
                    .map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let res = topic_raft_actor_addr
                    .send(crate::raft::topic::topic_raft_actor::DirectWriteToRaft { command })
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res =
                    res.map_err(|e| Status::internal(format!("Write to topic raft error {}", e)))?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::WriteResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("WriteResponse serialization error: {}", e))
                        })?,
                    })),
                    Err(e) => Err(map_topic_raft_error("topic raft write", e)),
                }
            }
            crate::protobuf::RaftType::SessionActorMap => {
                let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
                let command: crate::raft::session_actor_map::types::SessionActorMapRequest =
                    serde_json::from_str(&inner.data).map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let res = session_actor_map_raft_actor_addr
                    .send(crate::raft::session_actor_map::session_actor_map_raft_actor::DirectWriteToRaft {
                        command
                    })
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| {
                    Status::internal(format!("Write to session actor map raft error {}", e))
                })?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::WriteResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("WriteResponse serialization error: {}", e))
                        })?,
                    })),
                    Err(e) => Err(map_session_actor_map_raft_error(
                        "session actor map write",
                        e,
                    )),
                }
            }
            crate::protobuf::RaftType::SessionState => {
                let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();
                let command: crate::raft::session_state::types::SessionStateRequest =
                    serde_json::from_str(&inner.data).map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let res = session_state_raft_actor_addr
                    .send(
                        crate::raft::session_state::session_state_raft_actor::DirectWriteToRaft {
                            command,
                        },
                    )
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| {
                    Status::internal(format!("Write to session state raft error {}", e))
                })?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::WriteResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("WriteResponse serialization error: {}", e))
                        })?,
                    })),
                    Err(e) => Err(map_session_state_raft_error("session state write", e)),
                }
            }
        }
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let inner = request.into_inner();
        match inner.raft_type() {
            crate::protobuf::RaftType::SessionActorMap => {
                let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let append_entries_message = crate::raft::session_actor_map::session_actor_map_raft_actor::AppendEntriesRequestMessage {
                    payload
                };
                let res = session_actor_map_raft_actor_addr
                    .send(append_entries_message)
                    .await;
                if let Err(e) = res {
                    error!("append_entries SessionActorMap error: {}", e);
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| {
                    Status::internal(format!("Write to session actor map raft error {}", e))
                })?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::AppendEntriesResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("Failed to serialize response: {}", e))
                        })?,
                    })),
                    Err(e) => {
                        warn!("append_entries SessionActorMap error: {}", e);
                        Err(Status::internal(e.to_string()))
                    }
                }
            }
            crate::protobuf::RaftType::Topic => {
                let topic_raft_actor_addr =
                    crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let append_entries_message =
                    crate::raft::topic::topic_raft_actor::AppendEntriesRequestMessage { payload };
                let res = topic_raft_actor_addr.send(append_entries_message).await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res =
                    res.map_err(|e| Status::internal(format!("Write to topic raft error {}", e)))?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::AppendEntriesResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("Failed to serialize response: {}", e))
                        })?,
                    })),
                    Err(e) => Err(Status::internal(e.to_string())),
                }
            }
            crate::protobuf::RaftType::SessionState => {
                let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

                let payload: openraft::raft::AppendEntriesRequest<
                    crate::raft::session_state::types::SessionStateTypeConfig,
                > = serde_json::from_str(&inner.data)
                    .map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;

                // Intercept and ensure payloads are present before passing to RaftCore
                // This resolves the race condition between side-channel replication and log replication.
                for entry in &payload.entries {
                    if let openraft::EntryPayload::Normal(req) = &entry.payload {
                        let key = match req {
                            crate::raft::session_state::types::SessionStateRequest::InflightRegisterRxPacket { packet_key, .. } => Some(packet_key),
                            crate::raft::session_state::types::SessionStateRequest::InflightRegisterTxPacket { packet_key, .. } => Some(packet_key),
                            crate::raft::session_state::types::SessionStateRequest::AppendToPendingQueue { packet_key, .. } => Some(packet_key),
                            _ => None,
                        };

                        if let Some(k) = key {
                            // log::debug!("Interceptor checking payload for key: {}", k);
                            let mut found = false;
                            // Retry for up to 500ms (50 * 10ms) to allow for OS visibility/scheduling gap
                            for _ in 0..50 {
                                match self.store.contains(k).await {
                                    Ok(true) => {
                                        found = true;
                                        break;
                                    }
                                    _ => {
                                        tokio::time::sleep(std::time::Duration::from_millis(10))
                                            .await;
                                    }
                                }
                            }
                            if !found {
                                let msg = format!("Payload missing for key: {} after retry (AppendEntries). RPC aborted.", k);
                                error!("{}", msg);
                                return Err(grpc_status::not_ready_status(msg));
                            } else {
                                // log::debug!("Interceptor payload check PASSED for key: {}", k);
                            }
                        }
                    }
                }

                let append_entries_message = crate::raft::session_state::session_state_raft_actor::AppendEntriesRequestMessage {
                    payload
                };
                let res = session_state_raft_actor_addr
                    .send(append_entries_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| {
                    Status::internal(format!("Write to session state raft error {}", e))
                })?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::AppendEntriesResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("Failed to serialize response: {}", e))
                        })?,
                    })),
                    Err(e) => Err(Status::internal(e.to_string())),
                }
            }
        }
    }

    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let inner = request.into_inner();
        match inner.raft_type() {
            crate::protobuf::RaftType::Topic => {
                let topic_raft_actor_addr =
                    crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let vote_message =
                    crate::raft::topic::topic_raft_actor::VoteRequestMessage { payload };
                let res = topic_raft_actor_addr.send(vote_message).await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res =
                    res.map_err(|e| Status::internal(format!("Write to topic raft error {}", e)))?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::VoteResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("Failed to serialize response: {}", e))
                        })?,
                    })),
                    Err(e) => Err(Status::internal(e.to_string())),
                }
            }
            crate::protobuf::RaftType::SessionActorMap => {
                let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let vote_message = crate::raft::session_actor_map::session_actor_map_raft_actor::VoteRequestMessage {
                    payload
                };
                let res = session_actor_map_raft_actor_addr.send(vote_message).await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| {
                    Status::internal(format!("Write to session actor map raft error {}", e))
                })?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::VoteResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("Failed to serialize response: {}", e))
                        })?,
                    })),
                    Err(e) => Err(Status::internal(e.to_string())),
                }
            }
            crate::protobuf::RaftType::SessionState => {
                let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let vote_message =
                    crate::raft::session_state::session_state_raft_actor::VoteRequestMessage {
                        payload,
                    };
                let res = session_state_raft_actor_addr.send(vote_message).await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| {
                    Status::internal(format!("Write to session state raft error {}", e))
                })?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::VoteResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("Failed to serialize response: {}", e))
                        })?,
                    })),
                    Err(e) => Err(Status::internal(e.to_string())),
                }
            }
        }
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let inner = request.into_inner();
        match inner.raft_type() {
            crate::protobuf::RaftType::Topic => {
                let topic_raft_actor_addr =
                    crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let install_snapshot_message =
                    crate::raft::topic::topic_raft_actor::InstallSnapshotRequestMessage { payload };
                let res = topic_raft_actor_addr.send(install_snapshot_message).await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res =
                    res.map_err(|e| Status::internal(format!("Write to topic raft error {}", e)))?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::InstallSnapshotResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("Failed to serialize response: {}", e))
                        })?,
                    })),
                    Err(e) => Err(Status::internal(e.to_string())),
                }
            }
            crate::protobuf::RaftType::SessionActorMap => {
                let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let install_snapshot_message = crate::raft::session_actor_map::session_actor_map_raft_actor::InstallSnapshotRequestMessage {
                    payload
                };
                let res = session_actor_map_raft_actor_addr
                    .send(install_snapshot_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| {
                    Status::internal(format!("Write to session actor map raft error {}", e))
                })?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::InstallSnapshotResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("Failed to serialize response: {}", e))
                        })?,
                    })),
                    Err(e) => Err(Status::internal(e.to_string())),
                }
            }
            crate::protobuf::RaftType::SessionState => {
                let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| {
                        grpc_status::invalid_argument_status(
                            format!("Invalid JSON data: {}", e),
                            "raft_service",
                        )
                    })?;
                let install_snapshot_message = crate::raft::session_state::session_state_raft_actor::InstallSnapshotRequestMessage {
                    payload
                };
                let res = session_state_raft_actor_addr
                    .send(install_snapshot_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| {
                    Status::internal(format!("Write to session state raft error {}", e))
                })?;
                match res {
                    Ok(res) => Ok(Response::new(crate::protobuf::InstallSnapshotResponse {
                        data: serde_json::to_string(&res).map_err(|e| {
                            Status::internal(format!("Failed to serialize response: {}", e))
                        })?,
                    })),
                    Err(e) => Err(Status::internal(e.to_string())),
                }
            }
        }
    }
}
