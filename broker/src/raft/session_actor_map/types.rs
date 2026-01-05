use super::NodeId;
use crate::{raft::Node, session::session_actor_map_storage::SessionVersion};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SessionActorMapRequest {
    RegisterSession {
        tenant_id: String,
        session_id: String,
        node_id: NodeId,
        version: SessionVersion,
    },
    UnregisterSession {
        tenant_id: String,
        session_id: String,
        session_version: SessionVersion,
    },
    CleanExpiredSessions {
        sessions: Vec<ExpiredSession>,
    },
    SessionLeaseRenewRequest {
        sessions: Vec<RenewSession>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExpiredSession {
    pub tenant_id: String,
    pub session_id: String,
    pub node_id: NodeId,
    pub session_version: SessionVersion,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RenewSession {
    pub tenant_id: String,
    pub session_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SessionActorMapResponse {
    None,
    Rejected {
        current_version: SessionVersion,
        existing_version: SessionVersion,
    },
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
