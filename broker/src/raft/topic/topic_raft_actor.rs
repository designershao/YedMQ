use std::sync::Arc;

use actix::prelude::*;
use openraft::{error::{ClientWriteError, RaftError}, raft::ClientWriteResponse};
use yedmq_mqtt::MqttPacketV3;

use crate::raft::{topic::types::{TopicRaft, TypeConfig}, Node, NodeId};

pub struct TopicRaftActor {
    raft: Arc<TopicRaft>,
}

impl Actor for TopicRaftActor {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>")]
pub struct Subscribe{
    node_id: NodeId,
    tenant_id: String,
    client_identifier: String,
    topic: String,
    qos: u8,
}

impl Handler<Subscribe> for TopicRaftActor {

    type Result = ResponseActFuture<Self, Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>>;

    fn handle(&mut self, msg: Subscribe, _: &mut Self::Context) -> Self::Result {
        let raft = self.raft.clone();
        Box::pin(async move {
            let command = crate::raft::topic::types::Request::SubscribeTopic {
                node_id: msg.node_id,
                tenant_id: msg.tenant_id,
                client_identifier: msg.client_identifier,
                topic: msg.topic,
                qos: msg.qos,
            };
            raft.client_write(
                command
            ).await
        }.into_actor(self))
    }
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>")]
pub struct Unsubscribe {
    node_id: NodeId,
    tenant_id: String,
    client_identifier: String,
    topic: String,
}

impl Handler<Unsubscribe> for TopicRaftActor {

    type Result = ResponseActFuture<Self, Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>>;

    fn handle(&mut self, msg: Unsubscribe, _: &mut Self::Context) -> Self::Result {
        let raft = self.raft.clone();
        Box::pin(async move {
            let command = crate::raft::topic::types::Request::UnsubscribeTopic {
                node_id: msg.node_id,
                tenant_id: msg.tenant_id,
                client_identifier: msg.client_identifier,
                topic: msg.topic,
            };
            raft.client_write(
                command
            ).await
        }.into_actor(self))
    }
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>")]
pub struct RegisterRetainPublishPacket {
    tenant_id: String,
    client_id: String,
    publish_packet: MqttPacketV3,
}

impl Handler<RegisterRetainPublishPacket> for TopicRaftActor {

    type Result = ResponseActFuture<Self, Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>>;

    fn handle(&mut self, msg: RegisterRetainPublishPacket, _: &mut Self::Context) -> Self::Result {
        let raft = self.raft.clone();
        Box::pin(async move {
            let command = crate::raft::topic::types::Request::RegisterRetainPublishPacket {
                tenant_id: msg.tenant_id,
                source_client_identifier: msg.client_id,
                publish_packet: msg.publish_packet,
            };
            raft.client_write(
                command
            ).await
        }.into_actor(self))
    }
}

#[derive(Message)]
#[rtype(result = "Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>")]
pub struct CleanRetainPublishPacket {
    tenant_id: String,
    topic_filter: String,
}

impl Handler<CleanRetainPublishPacket> for TopicRaftActor {

    type Result = ResponseActFuture<Self, Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>>;

    fn handle(&mut self, msg: CleanRetainPublishPacket, _: &mut Self::Context) -> Self::Result {
        let raft = self.raft.clone();
        Box::pin(async move {
            let command = crate::raft::topic::types::Request::CleanRetainPublishPacket {
                tenant_id: msg.tenant_id,
                topic_filter: msg.topic_filter,
            };
            raft.client_write(
                command
            ).await
        }.into_actor(self))
    }
}