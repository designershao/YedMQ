use actix::SystemService;
use tonic::{Request, Response, Status};
use crate::protobuf::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use crate::protobuf::raft_service_server::RaftService;
use crate::raft::session_state::session_state_raft_actor;
use crate::raft::topic::topic_raft_actor;

struct RustServiceImpl;

#[tonic::async_trait]
impl RaftService for RustServiceImpl {
    async fn append_entries(&self, request: Request<AppendEntriesRequest>) -> Result<Response<AppendEntriesResponse>, Status> {
        let inner = request.into_inner();
        match inner.raft_type() {
            crate::protobuf::RaftType::SessionActorMap => {
                let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data).unwrap();
                let append_entries_message = crate::raft::session_actor_map::session_actor_map_raft_actor::AppendEntriesRequestMessage {
                    payload
                };
                let res = session_actor_map_raft_actor_addr
                    .send(append_entries_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.unwrap();
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::AppendEntriesResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).unwrap(),
                        };
                        Ok(Response::new(res))
                    }
                    Err(e) => {
                        Err(Status::internal(e.to_string()))
                    }
                }
            }
            crate::protobuf::RaftType::Topic => {
                let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
                let payload = serde_json::from_str(&inner.data).unwrap();
                let append_entries_message = crate::raft::topic::topic_raft_actor::AppendEntriesRequestMessage {
                    payload
                };
                let res = topic_raft_actor_addr
                    .send(append_entries_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.unwrap();
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::AppendEntriesResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).unwrap(),
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

                let payload = serde_json::from_str(&inner.data).unwrap();
                let append_entries_message = crate::raft::session_state::session_state_raft_actor::AppendEntriesRequestMessage {
                    payload
                };
                let res = session_state_raft_actor_addr
                    .send(append_entries_message)
                    .await;
                if let Err(e) = res {
                    return Err(Status::internal(e.to_string()));
                }
                let res = res.unwrap();
                match res {
                    Ok(res) => {
                        let res = crate::protobuf::AppendEntriesResponse {
                            success: true,
                            error: None,
                            data: serde_json::to_string(&res).unwrap(),
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
        todo!()
    }

    async fn install_snapshot(&self, request: Request<InstallSnapshotRequest>) -> Result<Response<InstallSnapshotResponse>, Status> {
        todo!()
    }
}
