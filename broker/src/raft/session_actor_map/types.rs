use serde::{Deserialize, Serialize};
use std::io::Cursor;
use crate::raft::{Node, NodeId};

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