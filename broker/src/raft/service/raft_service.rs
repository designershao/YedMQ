use crate::protobuf::raft_service_server::RaftService;
use crate::protobuf::{
    AppendEntriesRequest, AppendEntriesResponse, ErrorCode, ErrorDetail,
    ForceSessionDisconnectRequest, ForceSessionDisconnectResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, RaftType, RoutePacketRequest, RoutePacketResponse, VoteRequest,
    VoteResponse,
};
use crate::raft::raft_manager::RaftManager;
use crate::router::RouterCmd;
use actix::Recipient;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct RaftServiceImpl {
    pub raft_manager: Arc<RaftManager>,

    pub router_sender: Sender<RouterCmd>,

    pub session_manager_actor_recipient:
        Recipient<crate::session::session_manager_actor::ForceDisconnect>,
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {

    // Consistent get the session state
    async fn get_session_state(
        &self,
        request: tonic::Request<crate::protobuf::GetSessionStateRequest>,
    ) -> Result<tonic::Response<crate::protobuf::GetSessionStateResponse>, tonic::Status> {
        let ret = self
            .raft_manager
            .session_actor_map_raft
            .raft
            .ensure_linearizable()
            .await;
        match ret {
            Ok(_) => {
                let request = request.into_inner();
                let client_id = request.client_id;
                let tenant_id = request.tenant_id;


                let session_state = self
                    .raft_manager
                    .session_state_raft
                    .get_session_state(&tenant_id, &client_id)
                    .await;
                match session_state {
                    Some(session_state) => {
                        let session_state = session_state.read().await.clone();
                        let data = serde_json::to_string(&session_state).unwrap();
                        let res = crate::protobuf::GetSessionStateResponse {
                            success: true,
                            error: None,
                            session_state_data: Some(data),
                        };
                        Ok(tonic::Response::new(res))
                    },
                    None => {
                        let res = crate::protobuf::GetSessionStateResponse {
                            success: true,
                            error: None,
                            session_state_data: None
                        };
                        Ok(tonic::Response::new(res))
                    }
                }
            }
            Err(e) => {
                let res = crate::protobuf::GetSessionStateResponse {
                    success: false,
                    error: Some(crate::protobuf::ErrorDetail {
                        code: 500,
                        message: e.to_string(),
                        node: self
                            .raft_manager
                            .session_actor_map_raft
                            .current_node_id()
                            .to_string(),
                    }),
                    session_state_data: None
                };

                Ok(tonic::Response::new(res))
            }
        }
    }

    // Consistent get the session actor map
    async fn get_session_actor_map(
        &self,
        request: tonic::Request<crate::protobuf::GetSessionActorMapRequest>,
    ) -> Result<tonic::Response<crate::protobuf::GetSessionActorMapResponse>, tonic::Status> {
        let ret = self
            .raft_manager
            .session_actor_map_raft
            .raft
            .ensure_linearizable()
            .await;
        match ret {
            Ok(_) => {
                let request = request.into_inner();
                let client_id = request.client_id;
                let tenant_id = request.tenant_id;
                let node_id_option = self
                    .raft_manager
                    .session_actor_map_raft
                    .get_session_actor_map_node_id(&tenant_id, &client_id)
                    .await;
                let res = crate::protobuf::GetSessionActorMapResponse {
                    success: true,
                    error: None,
                    node_id: node_id_option,
                };
                Ok(tonic::Response::new(res))
            }
            Err(e) => {
                let res = crate::protobuf::GetSessionActorMapResponse {
                    success: false,
                    error: Some(crate::protobuf::ErrorDetail {
                        code: 500,
                        message: e.to_string(),
                        node: self
                            .raft_manager
                            .session_actor_map_raft
                            .current_node_id()
                            .to_string(),
                    }),
                    node_id: None,
                };

                Ok(tonic::Response::new(res))
            }
        }
    }

    // Foce session disconnect
    async fn session_force_disconnect(
        &self,
        request: tonic::Request<ForceSessionDisconnectRequest>,
    ) -> Result<tonic::Response<ForceSessionDisconnectResponse>, tonic::Status> {
        let req = request.into_inner();
        let client_id = req.client_id;
        let tenant_id = req.tenant_id;

        let res = self
            .session_manager_actor_recipient
            .send(crate::session::session_manager_actor::ForceDisconnect {
                client_id,
                tenant_id,
            })
            .await;

        if let Err(e) = res {
            let res = ForceSessionDisconnectResponse {
                success: false,
                error: Some(ErrorDetail {
                    code: ErrorCode::InternalError.into(),
                    message: e.to_string(),
                    node: self
                        .raft_manager
                        .session_actor_map_raft
                        .current_node_id()
                        .to_string(),
                }),
            };
            Ok(tonic::Response::new(res))
        } else {
            let res = ForceSessionDisconnectResponse {
                success: true,
                error: None,
            };
            Ok(tonic::Response::new(res))
        }
    }

    // Receive route packet from other node
    async fn route_packet(
        &self,
        request: tonic::Request<RoutePacketRequest>,
    ) -> Result<tonic::Response<RoutePacketResponse>, tonic::Status> {
        let req = request.into_inner();

        let router_cmd: RouterCmd =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;
        let res = self.router_sender.send(router_cmd).await;

        if let Err(e) = res {
            let res = RoutePacketResponse {
                success: false,
                data: "".to_string(),
                error: Some(ErrorDetail {
                    code: ErrorCode::InternalError.into(),
                    message: e.to_string(),
                    node: self
                        .raft_manager
                        .session_actor_map_raft
                        .current_node_id()
                        .to_string(),
                }),
            };
            Ok(tonic::Response::new(res))
        } else {
            let res = RoutePacketResponse {
                success: true,
                data: "".to_string(),
                error: None,
            };
            Ok(tonic::Response::new(res))
        }
    }

    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        let req = request.into_inner();
        let resp = match req.raft_type() {
            RaftType::Topic => {
                let append_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .topic_raft
                    .raft
                    .append_entries(append_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                resp
            }
            RaftType::SessionActorMap => {
                let append_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .session_actor_map_raft
                    .raft
                    .append_entries(append_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                resp
            }
        };

        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = AppendEntriesResponse {
            success: true,
            data,
            error: None,
        };

        Ok(tonic::Response::new(mes))
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<InstallSnapshotRequest>,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        let req = request.into_inner();

        let resp = match req.raft_type() {
            RaftType::Topic => {
                let install_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .topic_raft
                    .raft
                    .install_snapshot(install_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;
                resp
            }
            RaftType::SessionActorMap => {
                let install_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .session_actor_map_raft
                    .raft
                    .install_snapshot(install_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;
                resp
            }
        };

        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = InstallSnapshotResponse {
            success: true,
            data,
            error: None,
        };

        Ok(tonic::Response::new(mes))
    }

    async fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        let req = request.into_inner();

        let resp = match req.raft_type() {
            RaftType::Topic => {
                let vote_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .topic_raft
                    .raft
                    .vote(vote_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;
                resp
            }
            RaftType::SessionActorMap => {
                let vote_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .session_actor_map_raft
                    .raft
                    .vote(vote_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;
                resp
            }
        };

        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = VoteResponse {
            success: true,
            data,
            error: None,
        };

        Ok(tonic::Response::new(mes))
    }
}
