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