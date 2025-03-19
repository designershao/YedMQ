use crate::protobuf::raft_service_server::RaftService;
use crate::protobuf::{
    AppendEntriesRequest, AppendEntriesResponse, ErrorCode, ErrorDetail, InstallSnapshotRequest,
    InstallSnapshotResponse, RaftType, RoutePacketRequest, RoutePacketResponse, VoteRequest,
    VoteResponse,
};
use crate::raft::raft_manager::RaftManager;
use crate::router::RouterCmd;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct RaftServiceImpl {
    pub raft_manager: Arc<RaftManager>,

    pub router_sender: Sender<RouterCmd>,
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
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
                let install_req =
                    serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

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
                let install_req =
                    serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

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
