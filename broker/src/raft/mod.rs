use std::{fmt::Display, io::Cursor};

use serde::{Deserialize, Serialize};
use store::{Request, Response};


pub mod network;
pub mod service;
pub mod store;
pub mod raft_manager;

pub type NodeId = u64;

pub type YedMQRaft = openraft::Raft<TypeConfig>;

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

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        Node = Node
);

pub type SnapshotData = Cursor<Vec<u8>>;

pub mod typ {

    use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

    use super::{NodeId, TypeConfig};

    pub type Entry = openraft::Entry<TypeConfig>;

    impl tonic::IntoRequest<crate::protobuf::AppendEntriesRequest>
        for AppendEntriesRequest<TypeConfig>
    {
        fn into_request(self) -> tonic::Request<crate::protobuf::AppendEntriesRequest> {
            let mes = crate::protobuf::AppendEntriesRequest {
                data: serde_json::to_string(&self).expect("fail to serialize"),
            };
            tonic::Request::new(mes)
        }
    }

    impl tonic::IntoRequest<crate::protobuf::InstallSnapshotRequest>
        for InstallSnapshotRequest<TypeConfig>
    {
        fn into_request(self) -> tonic::Request<crate::protobuf::InstallSnapshotRequest> {
            let mes = crate::protobuf::InstallSnapshotRequest {
                data: serde_json::to_string(&self).expect("fail to serialize"),
            };
            tonic::Request::new(mes)
        }
    }

    impl tonic::IntoRequest<crate::protobuf::VoteRequest> for VoteRequest<NodeId> {
        fn into_request(self) -> tonic::Request<crate::protobuf::VoteRequest> {
            let mes = crate::protobuf::VoteRequest {
                data: serde_json::to_string(&self).expect("fail to serialize"),
            };
            tonic::Request::new(mes)
        }
    }
}

pub enum RaftManagerError {
    Raft(openraft::AnyError),
    
}


