use std::{fmt::Display, io::Cursor};
use serde::{Deserialize, Serialize};

pub mod service;
pub mod topic;
pub mod raft_manager;
pub mod session_actor_map;

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Node {
    pub rpc_addr: String,
    pub api_addr: String,
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node {{ rpc_addr: {}, api_addr: {} }}",
            self.rpc_addr, self.api_addr
        )
    }
}

pub type SnapshotData = Cursor<Vec<u8>>;

pub mod typ {

    use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

    use super::{topic::types::TypeConfig, NodeId};


}

pub enum RaftManagerError {
    Raft(openraft::AnyError),
    
}


