use crate::{raft::Node, session::session_state_storage::SessionStateStorageError};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use yedmq_mqtt::MqttPacketV3;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SessionStateRequest {
    CreateSessionState {
        tenant_id: String,
        client_id: String,
        inflight_duration_secs: u64,
    },
    DeleteSessionState {
        tenant_id: String,
        client_id: String,
    },
    InflightRegisterRxPacket {
        tenant_id: String,
        client_id: String,
        packet_id: u16,
        qos: u8,
        packet_key: String,
    },
    InflightRegisterTxPacket {
        tenant_id: String,
        client_id: String,
        packet_id: u16,
        qos: u8,
        packet_key: String,
    },
    InflightGetCurrentPacket {
        tenant_id: String,
        client_id: String,
        packet_identifier: u64,
    },
    InflightNextState {
        tenant_id: String,
        client_id: String,
        packet_identifier: u64,
    },
    InflightCleanFinishItems {
        tenant_id: String,
        client_id: String,
    },
    AppendToPendingQueue {
        tenant_id: String,
        client_id: String,
        packet_key: String,
    },
    PopFromPendingQueue {
        tenant_id: String,
        client_id: String,
    },
    SubscribeTopic {
        tenant_id: String,
        client_id: String,
        topic: String,
        qos: u8,
    },
    UnsubscribeTopic {
        tenant_id: String,
        client_id: String,
        topic: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SessionStateResponse {
    InflightGetCurrentPacketResult(Option<String>), // Return Key instead of Packet
    InflightRegisterTxPacketResponse(Result<(), SessionStateStorageError>),
    InflightRegisterRxPacketResponse(Result<(), SessionStateStorageError>),
    PopFromPendingQueueResult(Option<String>), // Return Key instead of Packet
    None,
}

openraft::declare_raft_types!(
    pub SessionStateTypeConfig:
        D = SessionStateRequest,
        R = SessionStateResponse,
        Node = Node
);

pub type Entry = openraft::Entry<SessionStateTypeConfig>;

impl tonic::IntoRequest<crate::protobuf::AppendEntriesRequest>
    for AppendEntriesRequest<SessionStateTypeConfig>
{
    fn into_request(self) -> tonic::Request<crate::protobuf::AppendEntriesRequest> {
        let mes = crate::protobuf::AppendEntriesRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            raft_type: crate::protobuf::RaftType::SessionState.into(),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<crate::protobuf::InstallSnapshotRequest>
    for InstallSnapshotRequest<SessionStateTypeConfig>
{
    fn into_request(self) -> tonic::Request<crate::protobuf::InstallSnapshotRequest> {
        let mes = crate::protobuf::InstallSnapshotRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            raft_type: crate::protobuf::RaftType::SessionState.into(),
        };
        tonic::Request::new(mes)
    }
}
