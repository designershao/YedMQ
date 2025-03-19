use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use crate::raft::Node;
use super::NodeId;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SessionActorMapRequest {
    CreateSession {
        tenant_id: String,
        session_id: String,
        node_id: NodeId,
    },
    DeleteSession {
        tenant_id: String,
        session_id: String,
        node_id: NodeId,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SessionActorMapResponse {
    None
}


openraft::declare_raft_types!(
    pub SessionActorMapTypeConfig:
        D = SessionActorMapRequest,
        R = SessionActorMapResponse,
        Node = Node
);


pub type Entry = openraft::Entry<SessionActorMapTypeConfig>;

impl tonic::IntoRequest<crate::protobuf::AppendEntriesRequest>
    for AppendEntriesRequest<SessionActorMapTypeConfig>
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
    for InstallSnapshotRequest<SessionActorMapTypeConfig>
{
    fn into_request(self) -> tonic::Request<crate::protobuf::InstallSnapshotRequest> {
        let mes = crate::protobuf::InstallSnapshotRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            raft_type: crate::protobuf::RaftType::SessionActorMap.into(),
        };
        tonic::Request::new(mes)
    }
}
