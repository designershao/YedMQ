use std::{sync::Arc, time::Duration, net::SocketAddr};

use log::warn;
use tokio::{
    net::{TcpStream},
    sync::{mpsc::Sender, RwLock}, io::{AsyncRead, AsyncWrite},
};

use crate::{
    protocol::{
        v3::{
            connack::{ConnAckPacket, ConnAckPacketBuilder, VariableHeader},
            fixed_header::FixHeader,
        },
        PacketType,
    },
    connection::Connection, plugin::{plugin_manager::PluginManager, session_context::SessionContext}, router::RouterCmd,
    session::{SessionManager, Session, SessionHandle}, settings::Settings, topic::TopicManager, inflight::Inflight,
};

pub mod tcp_listener;
pub mod tcp_tls_listener;
pub mod ws_listener;
pub mod wss_listener;
pub mod websocket_tunnel;

async fn accept_connection<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    stream: T,
    plugin_manager: Arc<PluginManager>,
    session_manager: Arc<RwLock<SessionManager>>,
    topic_manager: Arc<RwLock<TopicManager>>,
    router_sender: Sender<RouterCmd>,
    settings: Arc<Settings>,
    peer_addr: SocketAddr
) {
    let mut connection: Connection<T> = Connection::new(stream);
    // wait the first connect packet, if the packet is not correct, the connection will be closed.
    let first_packet = connection.read_packet().await;

    if first_packet.is_err() {
        warn!("read first packet error: {}", first_packet.unwrap_err());
        return;
    }

    match first_packet.unwrap() {
        crate::protocol::MqttPacketV3::Connect(packet) => {
            // invalid mqtt protocol name
            if packet.variable_header.protocol_name != "MQTT" {
                warn!("invalid mqtt protocol name");
                connection.shutdown().await.unwrap();
                return;
            }
            //

            // unsupport protocol version
            if packet.variable_header.protocol_level != 4 {
                let connack_packet = ConnAckPacketBuilder::new()
                    .set_return_code(
                        crate::protocol::v3::connack::ConnackReturnCode::UnsupportedProtocolVersion,
                    )
                    .build();
                if let Err(e) = connection
                    .write_packet(&crate::protocol::MqttPacketV3::Connack(connack_packet))
                    .await
                {
                    warn!("write connack packet error: {}", e);
                    connection.shutdown().await.unwrap();
                    return;
                }
                connection.shutdown().await.unwrap();
                return;
            }
            //

            let r = plugin_manager
                .clone()
                .call_hook_on_connect_auth(&peer_addr.to_string(), &packet)
                .await;
            match r {
                Ok(auth_result) => {
                    match auth_result {
                        crate::plugin::plugin_manager::OnConnectAuthResult::Pass(
                            tenant_id,
                            user_id,
                        ) => {
                            println!("tenant_id: {}, user_id: {}", tenant_id, user_id);

                            let session_context = SessionContext {
                                tenant_id: tenant_id.clone(),
                                client_identifier: packet.payload.client_identifier.clone(),
                                username: user_id,
                                remote_addr: peer_addr.to_string() 
                            };

                            let connack_packet = ConnAckPacketBuilder::new()
                                .set_return_code(
                                    crate::protocol::v3::connack::ConnackReturnCode::Accpet,
                                )
                                .build();
                            if let Err(e) = connection
                                .write_packet(&crate::protocol::MqttPacketV3::Connack(
                                    connack_packet,
                                ))
                                .await
                            {
                                warn!("write connack packet error: {}", e);
                                connection.shutdown().await.unwrap();
                                return;
                            }

                            {
                                let _ = session_manager
                                    .clone()
                                    .write()
                                    .await
                                    .create_tenant(tenant_id.clone())
                                    .await;
                                let _ = topic_manager
                                    .clone()
                                    .write()
                                    .await
                                    .create_tenant(tenant_id.clone());
                            }

                            let new_session = Session {
                                will_message: None,
                                client_identifier: packet.payload.client_identifier.clone(),
                                tenant_identifier: tenant_id.clone(),
                                subscription_topics: vec![],
                                inflight: Inflight::new(Duration::from_secs(
                                    settings.session.qos_expired_secs,
                                )),
                                clean_session: packet.variable_header.clean_session,
                                session_state: crate::session::SessionState::Online,
                            };

                            let (quit_signal, quit_waiter) = tokio::sync::oneshot::channel();

                            let mut session_handle = None;

                            {
                                let mut session_manager = session_manager.write().await;
                                let session_handle_result = session_manager
                                    .get_session_handle(
                                        tenant_id.clone(),
                                        packet.payload.client_identifier.clone(),
                                    )
                                    .await;
                                if session_handle_result.is_ok() {
                                    let session_handle_pre = session_handle_result.unwrap();
                                    session_handle = Some(session_handle_pre);
                                }
                            }

                            let session_handle = match session_handle {
                                Some(mut session_handle_pre) => {
                                    if session_handle_pre.is_online().await {
                                        session_handle_pre.kick_off().await;
                                    }

                                    if !session_handle_pre.is_clean_session().await {
                                        session_handle_pre
                                            .into_online(
                                                connection,
                                                plugin_manager,
                                                topic_manager,
                                                packet.variable_header.keep_alive.into(),
                                                settings.session.packet_resend_interval_secs,
                                                router_sender,
                                                quit_signal,
                                                session_context,
                                            )
                                            .await;
                                    }
                                    session_handle_pre
                                }
                                None => {
                                    let session_handle_new = SessionHandle::new(
                                        new_session,
                                        connection,
                                        plugin_manager,
                                        topic_manager,
                                        packet.variable_header.keep_alive.into(),
                                        settings.session.packet_resend_interval_secs,
                                        router_sender.clone(),
                                        quit_signal,
                                        session_context,
                                    )
                                    .await;
                                    session_handle_new
                                }
                            };

                            {
                                let _ = session_manager
                                    .write()
                                    .await
                                    .register(
                                        tenant_id.clone(),
                                        packet.payload.client_identifier.clone(),
                                        session_handle,
                                    )
                                    .await;
                            }

                            let _ = quit_waiter.await; // quit session online state

                            if packet.variable_header.clean_session {
                                let mut session_manager = session_manager.write().await;
                                let _ = session_manager
                                    .remove(
                                        tenant_id.clone(),
                                        packet.payload.client_identifier.clone(),
                                    )
                                    .await;
                            }
                        }
                        crate::plugin::plugin_manager::OnConnectAuthResult::Forbidden => {
                            println!("forbidden");
                            let connack_packet = ConnAckPacketBuilder::new().set_return_code(crate::protocol::v3::connack::ConnackReturnCode::InvalidUsernameOrPassword).build();
                            if let Err(e) = connection
                                .write_packet(&crate::protocol::MqttPacketV3::Connack(
                                    connack_packet,
                                ))
                                .await
                            {
                                warn!("write connack packet error: {}", e);
                            }
                            if let Err(e) = connection.shutdown().await {
                                warn!("shutdown connection error: {}", e);
                            }
                        }
                        crate::plugin::plugin_manager::OnConnectAuthResult::Error(e) => {
                            println!("error: {}", e);
                            let connack_packet = ConnAckPacketBuilder::new().set_return_code(crate::protocol::v3::connack::ConnackReturnCode::ServerUnavailable).build();
                            if let Err(e) = connection
                                .write_packet(&crate::protocol::MqttPacketV3::Connack(
                                    connack_packet,
                                ))
                                .await
                            {
                                warn!("write connack packet error: {}", e);
                            }
                            if let Err(e) = connection.shutdown().await {
                                warn!("shutdown connection error: {}", e);
                            }
                        }
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
}
