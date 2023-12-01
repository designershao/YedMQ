use std::{sync::Arc, time::Duration};

use anyhow::Result;
use log::warn;
use tokio::{net::TcpListener, select, sync::RwLock};

use crate::{
    connection::Connection,
    inflight::Inflight,
    plugin::{self, plugin_manager::PluginManager},
    protocol::{
        v3::{
            connack::{ConnAckPacket, ConnAckPacketBuilder, VariableHeader},
            fixed_header::FixHeader,
        },
        PacketType,
    },
    qos_context::QosContext,
    router::RouterCmd,
    session::{Session, SessionHandle, SessionManager, SessionManagerError},
    settings::Settings,
    topic::TopicManager,
};

pub struct MqttTcpListener {
    plugin_manager: Arc<PluginManager>,
    session_manager: Arc<RwLock<SessionManager>>,
    topic_manager: Arc<RwLock<TopicManager>>,
    router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
    settings: Arc<Settings>,
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
                        let r = plugin_manager
                            .clone()
                            .call_hook_on_connect_auth(&"".to_string(), &packet)
                            .await;
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

                                        {
                                            let _ = session_manager.write().await.create_tenant(tenant_id.clone()).await;
                                        }

                                        let new_session = Session {
                                            will_message: None,
                                            client_identifier: packet.payload.client_identifier.clone(),
                                            tenant_identifier: tenant_id.clone(),
                                            subscription_topics: vec![],
                                            inflight: Inflight::new(Duration::from_secs(settings.session.qos_expired_secs)),
                                            clean_session: packet.variable_header.clean_session,
                                            session_state: crate::session::SessionState::Online,
                                        };

                                        let (quit_signal, quit_waiter) = tokio::sync::oneshot::channel();

                                        let mut session_handle = None;

                                        {
                                            let mut session_manager = session_manager.write().await;
                                            let session_handle_result = session_manager.get_session_handle(tenant_id.clone(), packet.payload.client_identifier.clone()).await;
                                            if session_handle_result.is_ok() {
                                                let session_handle_pre = session_handle_result.unwrap();
                                                session_handle = Some(session_handle_pre);
                                            }
                                        }
                                        if session_handle.is_none() {
                                            let session_handle_new = SessionHandle::new(
                                                new_session,
                                                connection,
                                                plugin_manager,
                                                topic_manager,
                                                packet.variable_header.keep_alive.into(),
                                                settings.session.packet_resend_interval_secs,
                                                router_sender.clone(),
                                                quit_signal
                                            ).await;

                                            session_handle = Some(session_handle_new);

                                        } else {
                                            // if session is online, should close the pre connection
                                            //
                                            let mut session_handle_pre = session_handle.unwrap();
                                            if session_handle_pre.is_online().await {
                                                session_handle_pre.kick_off().await;
                                            }

                                            if !session_handle_pre.is_clean_session().await {
                                                session_handle_pre.into_online(
                                                    connection, 
                                                    plugin_manager, 
                                                    topic_manager,
                                                    packet.variable_header.keep_alive.into(),
                                                    settings.session.packet_resend_interval_secs,
                                                    router_sender,
                                                    quit_signal).await;
                                                session_handle = Some(session_handle_pre);
                                            }
                                        }

                                        {
                                            let _ = session_manager.write().await.register(tenant_id.clone(), packet.payload.client_identifier.clone(), session_handle.unwrap()).await;
                                        }

                                        let _ = quit_waiter.await; // quit session online state

                                        if packet.variable_header.clean_session {
                                            let mut session_manager = session_manager.write().await;
                                            let _ = session_manager.remove(tenant_id.clone(), packet.payload.client_identifier.clone()).await;
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
                            }
                            Err(e) => {
                                warn!("auth on connect failed: {}", e);
                            }
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
