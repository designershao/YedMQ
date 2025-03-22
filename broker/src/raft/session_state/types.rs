use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use crate::raft::Node;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SessionStateRequest {
    
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SessionStateResponse {
    None
}

openraft::declare_raft_types!(
    pub SessionStateTypeConfig:
        D = SessionStateRequest,
        R = SessionStateResponse,
        Node = Node
);

pub type Entry = openraft::Entry<SessionStateTypeConfig>;

impl tonic::IntoRequest<crate::protobuf::AppendEntriesRequest>
    for AppendEntriesRequest<SessionStateTypeConfig>
{
    fn into_request(self) -> tonic::Request<crate::protobuf::AppendEntriesRequest> {
        let mes = crate::protobuf::AppendEntriesRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            raft_type: crate::protobuf::RaftType::SessionActorMap.into(),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<crate::protobuf::InstallSnapshotRequest>
    for InstallSnapshotRequest<SessionStateTypeConfig>
{
    fn into_request(self) -> tonic::Request<crate::protobuf::InstallSnapshotRequest> {
        let mes = crate::protobuf::InstallSnapshotRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            raft_type: crate::protobuf::RaftType::SessionActorMap.into(),
        };
        tonic::Request::new(mes)
    }
}
