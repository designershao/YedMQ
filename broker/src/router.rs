use std::sync::Arc;

use actix::{Addr, Recipient};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use tokio::{select, sync::RwLock};

use crate::{session::session_manager_actor::{SendMessageToSession, SessionManagerActor}, topic::topic_manager::TopicManagerTrait};
use anyhow::Result;
use yedmq_mqtt::MqttPacketV3;
use crate::protobuf::raft_service_client::RaftServiceClient;

#[derive(Serialize, Deserialize)]
// Represent router command
pub enum RouterCmd {
    // Route publish packet to the subscribtion session
    RoutePacket{ tenant_identifier: String, packet: MqttPacketV3 },

    // Route packet to all tenants
    RoutePacketToAllTenants(MqttPacketV3),
}

pub struct Router {
    pub session_manager_recipient: Recipient<SendMessageToSession>,
    pub topic_manager: Arc<RwLock<dyn TopicManagerTrait>>,
    pub router_receiver: tokio::sync::mpsc::Receiver<RouterCmd>,
    pub raft_manager: Arc<dyn crate::raft::raft_manager::RaftManagerTrait>,
}

impl Router {
    pub async fn run(&mut self) {
        loop {
            select! {
                cmd = self.router_receiver.recv() => {
                    match cmd {
                        Some(RouterCmd::RoutePacket{tenant_identifier,packet }) => {
                            match self.route(&tenant_identifier, &packet).await {
                                Ok(_) => {
                                },
                                Err(e) => {
                                    warn!("teanant {} route packet to session error: {}", tenant_identifier, e);
                                }
                            }
                        },
                        Some(RouterCmd::RoutePacketToAllTenants(packet_)) => {
                            match self.route_to_all_tenants(&packet_).await {
                                Ok(_) => {
                                }
                                Err(e) => {
                                    warn!("route packet to all tenants error: {}", e);
                                }
                            }
                        }
                        None => todo!()
                    }
                }
            }
        }
    }

    pub async fn route_to_all_tenants(&self, packet: &MqttPacketV3) -> Result<()> {
        let tenants = self.topic_manager.read().await.get_tenant_names().await;
        for tenant in tenants.iter() {
            self.route(tenant, packet).await?;
        }
        Ok(())
    }

    async fn route_to_other_nodes(&self,addr: &String, cmd: RouterCmd) -> Result<()> {
        let addr = format!("http://{}", addr);

        let mut client = RaftServiceClient::connect(addr.clone()).await.unwrap();

        let route_request = crate::protobuf::RoutePacketRequest {
            data: serde_json::to_string(&cmd).unwrap(),
        };

        let _ = client.route_packet(route_request).await;

        Ok(())
    }


    pub async fn route(&self, tenant_identifier: &String,  packet: &MqttPacketV3) -> Result<()> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = publish_packet.variable_header.topic_name.clone();
            let topic_manager = self.topic_manager.read().await;
            let subscriptions = topic_manager
                .get_subscribers(tenant_identifier.clone(), topic).await
                .unwrap();
            for item in subscriptions.iter() {
                if item.node_id != self.raft_manager.topic_raft().current_node_id() {
                    info!("not in local node, send to other node ");
                    // not the current node, send to other node
                    let router_cmd = RouterCmd::RoutePacket{
                        tenant_identifier: tenant_identifier.clone(),
                        packet: packet.clone(),
                    };
                    let node = self.raft_manager.topic_raft().get_node_by_id(item.node_id).await;
                    if let Some(node) = node {
                        if let Err(e) = self.route_to_other_nodes(&node.rpc_addr, router_cmd).await {
                            warn!("route packet to node {} error: {}", node.rpc_addr, e);
                        }
                    } else {
                        warn!("node {} not found", item.node_id);
                    }
                    
                } else {
                    let client_identifier = item.client_identifier.clone();
                    let packet = packet.clone();
                    if let MqttPacketV3::Publish(mut publish_packet) = packet {
                        debug!(
                            "tenant {} session {} send packet max qos {} , body is {:?}",
                            tenant_identifier,
                            client_identifier.clone(),
                            item.qos,
                            publish_packet.payload.payload
                        );
                        if publish_packet.fix_header.qos.unwrap() >= item.qos as i32 {
                            if item.qos == 0 && publish_packet.fix_header.qos.unwrap() > 0 {
                                publish_packet.fix_header.qos = Some(0);
                                publish_packet.variable_header.packet_identifier = None;
                                publish_packet.fix_header.remaining_length =
                                    publish_packet.fix_header.remaining_length - 2;
                            } else {
                                publish_packet.fix_header.qos = Some(item.qos.into());
                            }
                        }
                        if let Err(err) = self.session_manager_recipient.send(SendMessageToSession {
                            tenant_id: tenant_identifier.clone(),
                            client_id: client_identifier.clone(),
                            packet: MqttPacketV3::Publish(publish_packet),
                        }).await
                        {
                            warn!(
                                "tenant {} session {} send packet error, details: {}",
                                tenant_identifier,
                                client_identifier.clone(),
                                err
                            );
                        }
                    }
                }

            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::Sender;

    use actix::{Actor, Context, Handler};
    use yedmq_mqtt::v3::publish::PublishPacketBuilder;

    use crate::{settings::Settings, topic::topic_storage::Subscription};

    use super::*;

    struct MockSessionManagerActor {
        pub message_sender: Sender<SendMessageToSession>,
    }

    impl Actor for MockSessionManagerActor {
        type Context = Context<Self>;
    }

    impl Handler<SendMessageToSession> for MockSessionManagerActor {
        type Result = ();
    
        fn handle(&mut self, msg: SendMessageToSession, ctx: &mut Self::Context) -> Self::Result {
            let _ = self.message_sender.send(msg);
            ()
        }
    }

    #[actix::test]
    pub async fn when_route_packet_to_session_actor_message_should_correct() {

        let mut topic_manager_mock = crate::topic::topic_manager::MockTopicManagerTrait::new();
        topic_manager_mock.expect_get_subscribers().returning(|_, _| {
            Box::pin(
                async move {
                    Ok(vec![Arc::new(Subscription {
                        node_id: 123,
                        client_identifier: "client_id".to_string(),
                        qos: 1,
                    })])
                }
            )
        });

        let mut raft_manager_mock = crate::raft::raft_manager::MockRaftManagerTrait::new();

        let mut mock_topic_raft_manager = crate::raft::topic::MockTopicRaftManagerTrait::new();
        mock_topic_raft_manager.expect_current_node_id().return_const(123 as u64);

        raft_manager_mock.expect_topic_raft().return_const(Box::new(mock_topic_raft_manager));

        let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

        let topic_manager = Arc::new(RwLock::new(topic_manager_mock));

        let raft_manager = Arc::new(raft_manager_mock);

        let (message_sender, message_receiver) = std::sync::mpsc::channel();

        let session_manager_actor = MockSessionManagerActor {
            message_sender
        }.start();

        let router = Router {
            session_manager_recipient: session_manager_actor.recipient(),
            topic_manager: topic_manager.clone(),
            router_receiver: router_receiver,
            raft_manager: raft_manager.clone(),
        };

        let publish_packet = PublishPacketBuilder::new("/a/b".into(), vec![]).build();

        let _ = router.route(&"public".into(), &MqttPacketV3::Publish(publish_packet)).await;

        let msg = message_receiver.recv().unwrap();
        assert_eq!(msg.tenant_id, "public");
        assert_eq!(msg.client_id, "client_id");

    }
}