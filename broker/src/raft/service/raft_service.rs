use tokio::sync::mpsc::Sender;
use yedmq_mqtt::MqttPacketV3;

use crate::protobuf::raft_service_server::RaftService;
use crate::protobuf::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RoutePacketRequest, RoutePacketResponse, VoteRequest, VoteResponse,
};
use crate::raft::raft_manager::RaftManager;
use crate::router::RouterCmd;
use std::sync::Arc;

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

        let tenant_id = req.tenant_id;
        let client_id = req.client_id;
        let packet:MqttPacketV3 = serde_json::from_str(&req.packet).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let route_cmd = RouterCmd::RoutePacket {tenant_identifier: tenant_id, packet };
        let _ = self.router_sender.send(route_cmd).await;

        let res = RoutePacketResponse {
            error: "".to_string(),
        };

        Ok(tonic::Response::new(res))
    }

    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        let req = request.into_inner();

        let append_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .raft_manager
            .raft
            .append_entries(append_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;

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

        let install_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .raft_manager
            .raft
            .install_snapshot(install_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;

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

        let vote_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .raft_manager
            .raft
            .vote(vote_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;

        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = VoteResponse {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }
}
