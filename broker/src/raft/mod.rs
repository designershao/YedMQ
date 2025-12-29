use serde::{Deserialize, Serialize};
use std::{fmt::Display, io::Cursor};

pub mod session_actor_map;
pub mod session_state;
pub mod topic;
pub mod payload;

pub type NodeId = u64;

pub trait NodeTrait {

    fn rpc_addr(&self) -> &String;

    fn api_addr(&self) -> &String;

}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Node {
    pub rpc_addr: String,
    pub api_addr: String,
}

impl NodeTrait for Node {
    fn rpc_addr(&self) -> &String {
        &self.rpc_addr
    }

    fn api_addr(&self) -> &String {
        &self.api_addr
    }
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