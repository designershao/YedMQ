use std::{sync::Arc, time::Duration};

use log::warn;
use tokio::{net::TcpListener, sync::RwLock, select};
use anyhow::Result;

use crate::{connection::Connection, plugin::plugin_manager::PluginManager, session::{SessionManager, SessionManagerError, Session, SessionHandle, SenderMessage, ReceiverMessage}, router::RouterCmd, topic::TopicManager, qos_context::QosContext, settings::Settings, protocol::{v3::{connack::{ConnAckPacket, VariableHeader, ConnAckPacketBuilder}, fixed_header::FixHeader}, PacketType}};

pub struct MqttTcpListener {
    plugin_manager: Arc<PluginManager>,
    session_manager: Arc<RwLock<SessionManager>>,
    topic_manager: Arc<RwLock<TopicManager>>,
    router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
    settings: Arc<Settings>
}

impl MqttTcpListener {

    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.settings.listener.tcp.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            let plugin_manager = self.plugin_manager.clone();
            let session_manager = self.session_manager.clone();
            let topic_manager = self.topic_manager.clone();
            let router_sender = self.router_sender.clone();
            let settings = self.settings.clone();

            tokio::spawn(async move {
                let mut connection = Connection::new(stream);
                // wait the first connect packet, if the packet is not correct, the connection will be closed.
                let first_packet = connection.read_packet().await.unwrap();
                
                match first_packet {
                    crate::protocol::MqttPacketV3::Connect(packet) => {
                        let r = plugin_manager.clone().call_hook_on_connect_auth(&"".to_string(), &packet).await;
                        match r {
                            Ok(auth_result) => {
                                match auth_result {
                                    crate::plugin::plugin_manager::OnConnectAuthResult::Pass(tenant_id, user_id) => {

                                        let connack_packet = ConnAckPacketBuilder::new().set_return_code(crate::protocol::v3::connack::ConnackReturnCode::Accpet).build();
                                        if let Err(e) = connection.write_packet(&crate::protocol::MqttPacketV3::Connack(connack_packet)).await {
                                            warn!("write connack packet error: {}", e);
                                            connection.shutdown().await.unwrap();
                                            return
                                        }

                                        let _ = session_manager.write().await.create_tenant(tenant_id.clone()).await;
                                        let (deliver_packet_tx, mut deliver_packet_rx) = tokio::sync::mpsc::channel(100);
                                        let new_session = Session {
                                            will_message: None,
                                            client_identifier: packet.payload.client_identifier.clone(),
                                            tenant_identifier: tenant_id.clone(),
                                            subscription_topics: vec![],
                                            qos_context: QosContext::new(Duration::from_secs(settings.session.qos_expired_secs)),
                                            deliver_packet_tx: Some(deliver_packet_tx),
                                            topic_tree: topic_manager,
                                            clean_session: packet.variable_header.clean_session,
                                            session_state: crate::session::SessionState::Online,
                                            plugin_manager,
                                        };
                                        let tx = new_session.run_online_loop(
                                            packet.variable_header.keep_alive.into(),
                                        settings.session.packet_resend_interval_secs).await;

                                        let session_handle = SessionHandle{
                                            session_sender: tx.clone(),
                                            router_sender: router_sender,
                                        };
                                        let _ = session_manager.write().await.register(tenant_id.clone(), packet.payload.client_identifier.clone(), session_handle).await;
                                        loop {
                                            select! {
                                                packet = connection.read_packet() => {
                                                    if let Ok(packet) = packet {
                                                        let _ = tx.send(ReceiverMessage::Packet(packet)).await;
                                                    } else {
                                                        connection.shutdown().await.unwrap();
                                                        tx.send(ReceiverMessage::ConnectionHasShutdown).await.unwrap(); // notifiy session connection has shutdown
                                                        warn!("invalid mqtt packet, close the connection");
                                                        break;
                                                    }
                                                }
                                                cmd = deliver_packet_rx.recv() => {
                                                    match cmd {
                                                        Some(m) => {
                                                            match m {
                                                                SenderMessage::WritePacket(p) => {
                                                                    connection.write_packet(&p).await.unwrap();
                                                                },
                                                                SenderMessage::ShutdownConnection => {
                                                                    connection.shutdown().await.unwrap();
                                                                    break;
                                                                },
                                                                _ => {}
                                                            }
                                                        },
                                                        None => {
                                                            warn!("deliver packet channel closed");
                                                            connection.shutdown().await.unwrap();
                                                            tx.send(ReceiverMessage::ConnectionHasShutdown).await.unwrap(); // notifiy session connection has shutdown
                                                            break;
                                                        },
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    crate::plugin::plugin_manager::OnConnectAuthResult::Forbidden => {
                                        let connack_packet = ConnAckPacketBuilder::new().set_return_code(crate::protocol::v3::connack::ConnackReturnCode::InvalidUsernameOrPassword).build();
                                        if let Err(e) = connection.write_packet(&crate::protocol::MqttPacketV3::Connack(connack_packet)).await {
                                            warn!("write connack packet error: {}", e);
                                        }
                                        if let Err(e) = connection.shutdown().await {
                                            warn!("shutdown connection error: {}", e);
                                        }
                                    },
                                    crate::plugin::plugin_manager::OnConnectAuthResult::Error(_) => {
                                        let connack_packet = ConnAckPacketBuilder::new().set_return_code(crate::protocol::v3::connack::ConnackReturnCode::ServerUnavailable).build();
                                        if let Err(e) = connection.write_packet(&crate::protocol::MqttPacketV3::Connack(connack_packet)).await {
                                            warn!("write connack packet error: {}", e);
                                        }
                                        if let Err(e) = connection.shutdown().await {
                                            warn!("shutdown connection error: {}", e);
                                        }
                                    },
                                }
                            },
                            Err(e) => {
                                warn!("auth on connect failed: {}", e);
                            },
                        }
                    }
                    _ => {
                        connection.shutdown().await.unwrap();
                    }
                }
            });
        }
    }

}
