use std::sync::Arc;
use crate::protobuf::raft_service_server::RaftService;
use crate::protobuf::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use crate::raft::raft_manager::RaftManager;

pub struct RaftServiceImpl {
    pub raft_manager: Arc<RaftManager>
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        let req = request.into_inner();

        let append_req = serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self.raft_manager.raft.append_entries(append_req).await.map_err(|x| tonic::Status::internal(x.to_string()))?;

        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = AppendEntriesResponse {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }
    
    async fn install_snapshot(
        &self,
        request: tonic::Request<InstallSnapshotRequest>,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        let req = request.into_inner();

        let install_req = serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self.raft_manager.raft.install_snapshot(install_req).await.map_err(|x| tonic::Status::internal(x.to_string()))?;

        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = InstallSnapshotResponse {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }

    async fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        let req = request.into_inner();

        let vote_req = serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self.raft_manager.raft.vote(vote_req).await.map_err(|x| tonic::Status::internal(x.to_string()))?;

        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = VoteResponse {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }

}