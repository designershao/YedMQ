use std::{fmt::Display, io::Cursor};

use serde::{Deserialize, Serialize};
use store::{Request, Response};

pub mod store;

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Node {
    pub rpc_addr: String,
    pub api_addr: String,
}


impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node {{ rpc_addr: {}, api_addr: {} }}", self.rpc_addr, self.api_addr)
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        Node = Node
);

pub type SnapshotData = Cursor<Vec<u8>>;

pub mod typ {
    use openraft::error::Infallible;

    use super::TypeConfig;

    pub type Entry = openraft::Entry<TypeConfig>; 
}