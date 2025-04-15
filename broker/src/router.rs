use std::{sync::Arc, time::Duration};

use actix::{Addr, Recipient};
use backoff::{backoff::Backoff, Error, ExponentialBackoff};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use tokio::{select, sync::RwLock};
use tonic::transport::Channel;

use crate::protobuf::raft_service_client::RaftServiceClient;
use crate::{
    session::session_manager_actor::{SendMessageToSession, SessionManagerActor},
    topic::topic_manager::TopicManagerTrait,
};
use anyhow::Result;
use yedmq_mqtt::MqttPacketV3;

#[derive(Serialize, Deserialize,Debug)]
// Represent router command
pub enum RouterCmd {
    // Route publish packet to the subscribtion session
    RoutePacket {
        tenant_identifier: String,
        packet: MqttPacketV3,
    },

    // Route packet from other node
    RoutePacketFromOtherNode {
        tenant_identifier: String,
        packet: MqttPacketV3,
    },

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
                        Some(RouterCmd::RoutePacketFromOtherNode{tenant_identifier,packet }) => {
                            match self.route_in_local_node(&tenant_identifier, &packet).await {
                                Ok(_) => {
                                },
                                Err(e) => {
                                    warn!("teanant {} route packet to session error: {}", tenant_identifier, e);
                                }
                            }
                        }
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

    async fn create_rpc_client_with_retry(addr: String) -> Result<RaftServiceClient<Channel>> {
        let mut backoff = ExponentialBackoff {
            initial_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(10),
            multiplier: 2.0,
            max_elapsed_time: Some(Duration::from_secs(60)),
            ..ExponentialBackoff::default()
        };

        let channel = loop {
            match tonic::transport::Endpoint::from_shared(addr.clone())?
                .connect()
                .await
            {
                Ok(channel) => break channel,
                Err(e) => {
                    if let Some(duration) = backoff.next_backoff() {
                        warn!("RPC client connection failed: {}. Retrying in {:?}...", e, duration);
                        tokio::time::sleep(duration).await;
                    } else {
                        return Err(anyhow::anyhow!(format!("Failed to connect after retries: {}", e)));
                    }
                }
            }
        };

        Ok(RaftServiceClient::new(channel))
    }

    async fn route_to_other_nodes(&self, addr: &String, cmd: RouterCmd) -> Result<()> {
        let addr = format!("http://{}", addr);

        let mut client = Router::create_rpc_client_with_retry(addr).await?;

        let route_request = crate::protobuf::RoutePacketRequest {
            data: serde_json::to_string(&cmd).unwrap(),
        };

        let res = client.route_packet(route_request).await;
        if let Ok(res) = res {
            let res = res.into_inner();
            if !res.success {
                return Err(anyhow::anyhow!(res.error.unwrap().message));
            } else {
                return Ok(());
            }
        } else {
            return Err(anyhow::anyhow!(res.unwrap_err().to_string()));
        }
    }

    pub async fn route_in_local_node(
        &self,
        tenant_identifier: &String,
        packet: &MqttPacketV3,
    ) -> Result<()> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = publish_packet.variable_header.topic_name.clone();
            let topic_manager = self.topic_manager.read().await;
            let subscriptions = topic_manager
                .get_subscribers(tenant_identifier.clone(), topic)
                .await
                .unwrap();
            for item in subscriptions.iter() {
                if item.node_id == self.raft_manager.topic_raft().current_node_id() {
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
                        if let Err(err) = self
                            .session_manager_recipient
                            .send(SendMessageToSession {
                                tenant_id: tenant_identifier.clone(),
                                client_id: client_identifier.clone(),
                                packet: MqttPacketV3::Publish(publish_packet),
                            })
                            .await
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

    pub async fn route(&self, tenant_identifier: &String, packet: &MqttPacketV3) -> Result<()> {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let topic = publish_packet.variable_header.topic_name.clone();
            let topic_manager = self.topic_manager.read().await;
            let subscriptions = topic_manager
                .get_subscribers(tenant_identifier.clone(), topic)
                .await;
            if let Err(e) = subscriptions {
                return Err(anyhow::anyhow!(e));
            }
            let subscriptions = subscriptions.unwrap();
            for item in subscriptions.iter() {
                if item.node_id != self.raft_manager.topic_raft().current_node_id() {
                    // not the current node, send to other node
                    let router_cmd = RouterCmd::RoutePacketFromOtherNode {
                        tenant_identifier: tenant_identifier.clone(),
                        packet: packet.clone(),
                    };
                    let node = self
                        .raft_manager
                        .topic_raft()
                        .get_node_by_id(item.node_id)
                        .await;
                    if let Some(node) = node {
                        debug!(
                            "client not in the current node, route packet to other node {}",
                            node
                        );
                        if let Err(e) = self.route_to_other_nodes(&node.rpc_addr, router_cmd).await
                        {
                            warn!("route packet to node {} error: {}", node.rpc_addr, e);
                        }
                    } else {
                        warn!("node {} not found", item.node_id);
                    }
                } else {
                    let client_identifier = item.client_identifier.clone();
                    let packet = packet.clone();
                    info!(
                        "route to current node tenant {} session {} max qos {}",
                        tenant_identifier,
                        client_identifier.clone(),
                        item.qos,
                    );
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
                        if let Err(err) = self
                            .session_manager_recipient
                            .send(SendMessageToSession {
                                tenant_id: tenant_identifier.clone(),
                                client_id: client_identifier.clone(),
                                packet: MqttPacketV3::Publish(publish_packet),
                            })
                            .await
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

    use crate::topic::topic_storage::Subscription;

    use super::*;

    struct MockSessionManagerActor {
        pub message_sender: Sender<SendMessageToSession>,
    }

    impl Actor for MockSessionManagerActor {
        type Context = Context<Self>;
    }

    impl Handler<SendMessageToSession> for MockSessionManagerActor {
        type Result = ();

        fn handle(&mut self, msg: SendMessageToSession, _ctx: &mut Self::Context) -> Self::Result {
            let _ = self.message_sender.send(msg);
            ()
        }
    }

    #[actix::test]
    pub async fn when_route_packet_to_session_actor_message_should_correct() {
        let mut topic_manager_mock = crate::topic::topic_manager::MockTopicManagerTrait::new();
        topic_manager_mock
            .expect_get_subscribers()
            .returning(|_, _| {
                Box::pin(async move {
                    Ok(vec![Arc::new(Subscription {
                        node_id: 123,
                        client_identifier: "client_id".to_string(),
                        qos: 1,
                    })])
                })
            });

        let mut raft_manager_mock = crate::raft::raft_manager::MockRaftManagerTrait::new();

        let mut mock_topic_raft_manager = crate::raft::topic::MockTopicRaftManagerTrait::new();
        mock_topic_raft_manager
            .expect_current_node_id()
            .return_const(123 as u64);

        raft_manager_mock
            .expect_topic_raft()
            .return_const(Box::new(mock_topic_raft_manager));

        let (_router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

        let topic_manager = Arc::new(RwLock::new(topic_manager_mock));

        let raft_manager = Arc::new(raft_manager_mock);

        let (message_sender, message_receiver) = std::sync::mpsc::channel();

        let session_manager_actor = MockSessionManagerActor { message_sender }.start();

        let router = Router {
            session_manager_recipient: session_manager_actor.recipient(),
            topic_manager: topic_manager.clone(),
            router_receiver: router_receiver,
            raft_manager: raft_manager.clone(),
        };

        let publish_packet = PublishPacketBuilder::new("/a/b".into(), vec![]).build();

        let _ = router
            .route(&"public".into(), &MqttPacketV3::Publish(publish_packet))
            .await;

        let msg = message_receiver.recv().unwrap();
        assert_eq!(msg.tenant_id, "public");
        assert_eq!(msg.client_id, "client_id");
    }
}
