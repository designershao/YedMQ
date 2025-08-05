use tonic::{Request, Response, Status};
use crate::protobuf::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use crate::protobuf::raft_service_server::RaftService;

struct RustServiceImpl;

#[tonic::async_trait]
impl RaftService for RustServiceImpl {
    async fn append_entries(&self, request: Request<AppendEntriesRequest>) -> Result<Response<AppendEntriesResponse>, Status> {
        todo!()
    }

    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        todo!()
    }

    async fn install_snapshot(&self, request: Request<InstallSnapshotRequest>) -> Result<Response<InstallSnapshotResponse>, Status> {
        todo!()
    }
}
