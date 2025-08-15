use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use yedmq_mqtt::MqttPacketV3;

use crate::raft::{Node, NodeId};

pub type TopicRaft = openraft::Raft<TypeConfig>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    // Subscribe topic
    SubscribeTopic {
        tenant_id: String,
        client_identifier: String,
        topic: String,
        qos: u8,
    },
    // Unsubscribe topic
    UnsubscribeTopic {
        node_id: NodeId,
        tenant_id: String,
        client_identifier: String,
        topic: String,
    },
    RegisterRetainPublishPacket {
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: MqttPacketV3,
    },
    CleanRetainPublishPacket {
        tenant_id: String,
        topic_filter: String,
    },
    CreateTenant {
        tenant_id: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    None,
}

pub type Entry = openraft::Entry<TypeConfig>;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        Node = Node
);

impl tonic::IntoRequest<crate::protobuf::AppendEntriesRequest>
    for AppendEntriesRequest<TypeConfig>
{
    fn into_request(self) -> tonic::Request<crate::protobuf::AppendEntriesRequest> {
        let mes = crate::protobuf::AppendEntriesRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            raft_type: crate::protobuf::RaftType::Topic.into(),
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
            raft_type: crate::protobuf::RaftType::Topic.into(),
        };
        tonic::Request::new(mes)
    }
}

