use actix::SystemService;
use log::{error, warn};
use tonic::{Request, Response, Status};
use crate::protobuf::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse, WriteRequest, WriteResponse};
use crate::protobuf::raft_service_server::RaftService;
use std::sync::Arc;
use crate::raft::payload::PayloadStore;

pub struct RustServiceImpl {
    pub store: Arc<dyn PayloadStore>,
}

#[tonic::async_trait]
impl RaftService for RustServiceImpl {

    async fn write(&self, request: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        let inner = request.into_inner();

        match inner.raft_type() {
            crate::protobuf::RaftType::Topic => {
                let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
                let command: crate::raft::topic::types::Request = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                let res = topic_raft_actor_addr
                    .send(crate::raft::topic::topic_raft_actor::DirectWriteToRaft {
                        command
                    })
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| Status::internal(format!("Write to topic raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::WriteResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("WriteResponse serialization error: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            }
            crate::protobuf::RaftType::SessionActorMap => {
                let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
                let command: crate::raft::session_actor_map::types::SessionActorMapRequest = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                let res = session_actor_map_raft_actor_addr
                    .send(crate::raft::session_actor_map::session_actor_map_raft_actor::DirectWriteToRaft {
                        command
                    })
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| Status::internal(format!("Write to session actor map raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::WriteResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("WriteResponse serialization error: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            },
            crate::protobuf::RaftType::SessionState => {
                let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();
                let command: crate::raft::session_state::types::SessionStateRequest = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                let res = session_state_raft_actor_addr
                    .send(crate::raft::session_state::session_state_raft_actor::DirectWriteToRaft {
                        command
                    })
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| Status::internal(format!("Write to session state raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::WriteResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("WriteResponse serialization error: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            },
        }
    }

    async fn append_entries(&self, request: Request<AppendEntriesRequest>) -> Result<Response<AppendEntriesResponse>, Status> {
        let inner = request.into_inner();
        match inner.raft_type() {
            crate::protobuf::RaftType::SessionActorMap => {
                let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
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
                let res = res.map_err(|e| Status::internal(format!("Write to session actor map raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::AppendEntriesResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        warn!("append_entries SessionActorMap error: {}", e);
                        Err(Status::internal(e.to_string()))
                    }
                }
            }
            crate::protobuf::RaftType::Topic => {
                let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                let append_entries_message = crate::raft::topic::topic_raft_actor::AppendEntriesRequestMessage {
                    payload
                };
                let res = topic_raft_actor_addr
                    .send(append_entries_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| Status::internal(format!("Write to topic raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::AppendEntriesResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            }
            crate::protobuf::RaftType::SessionState => {
                let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

                let payload: openraft::raft::AppendEntriesRequest<crate::raft::session_state::types::SessionStateTypeConfig> = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                
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
                                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                    }
                                }
                            }
                            if !found {
                                let msg = format!("Payload missing for key: {} after retry (AppendEntries). RPC aborted.", k);
                                error!("{}", msg);
                                return Err(Status::failed_precondition(msg));
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
                let res = res.map_err(|e| Status::internal(format!("Write to session state raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::AppendEntriesResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            }
        }
    }

    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let inner = request.into_inner();
        match inner.raft_type() {
            crate::protobuf::RaftType::Topic => {
                let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                let vote_message = crate::raft::topic::topic_raft_actor::VoteRequestMessage {
                    payload
                };
                let res = topic_raft_actor_addr
                    .send(vote_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| Status::internal(format!("Write to topic raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::VoteResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            },
            crate::protobuf::RaftType::SessionActorMap => {
                let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                let vote_message = crate::raft::session_actor_map::session_actor_map_raft_actor::VoteRequestMessage {
                    payload
                };
                let res = session_actor_map_raft_actor_addr
                    .send(vote_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| Status::internal(format!("Write to session actor map raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::VoteResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            },
            crate::protobuf::RaftType::SessionState => {
                let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                let vote_message = crate::raft::session_state::session_state_raft_actor::VoteRequestMessage {
                    payload
                };
                let res = session_state_raft_actor_addr
                    .send(vote_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| Status::internal(format!("Write to session state raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::VoteResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            },
        }
    }

    async fn install_snapshot(&self, request: Request<InstallSnapshotRequest>) -> Result<Response<InstallSnapshotResponse>, Status> {
        let inner = request.into_inner();
        match inner.raft_type() {
            crate::protobuf::RaftType::Topic => {
                let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                let install_snapshot_message = crate::raft::topic::topic_raft_actor::InstallSnapshotRequestMessage {
                    payload
                };
                let res = topic_raft_actor_addr
                    .send(install_snapshot_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| Status::internal(format!("Write to topic raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::InstallSnapshotResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            },
            crate::protobuf::RaftType::SessionActorMap => {
                let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                let install_snapshot_message = crate::raft::session_actor_map::session_actor_map_raft_actor::InstallSnapshotRequestMessage {
                    payload
                };
                let res = session_actor_map_raft_actor_addr
                    .send(install_snapshot_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| Status::internal(format!("Write to session actor map raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::InstallSnapshotResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            },
            crate::protobuf::RaftType::SessionState => {
                let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

                let payload = serde_json::from_str(&inner.data)
                    .map_err(|e| Status::invalid_argument(format!("Invalid JSON data: {}", e)))?;
                let install_snapshot_message = crate::raft::session_state::session_state_raft_actor::InstallSnapshotRequestMessage {
                    payload
                };
                let res = session_state_raft_actor_addr
                    .send(install_snapshot_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.map_err(|e| Status::internal(format!("Write to session state raft error {}", e)))?;
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::InstallSnapshotResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?,
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            },
        }
    }
}
