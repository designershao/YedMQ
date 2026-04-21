use serde::{Deserialize, Serialize};
use std::{fmt::Display, io::Cursor};

pub mod payload;
pub mod session_actor_map;
pub mod session_state;
pub mod topic;

pub type NodeId = u64;

#[derive(Clone, Debug)]
pub struct GRPCBusinessError {
    code: crate::protobuf::ErrorCode,

    message: String,

    node: String,
}

impl std::fmt::Display for GRPCBusinessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node: {}, code: {:?}, message: {}",
            self.node, self.code, self.message
        )
    }
}

impl From<crate::protobuf::ErrorDetail> for GRPCBusinessError {
    fn from(value: crate::protobuf::ErrorDetail) -> Self {
        Self {
            code: value.code(),
            message: value.message,
            node: value.node,
        }
    }
}

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
