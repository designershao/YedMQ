use std::{sync::Arc, time::Duration, net::SocketAddr};

use log::{warn, info};
use samoye_plugin::plugin::AuthenticationResult;
use tokio::{
    sync::{mpsc::Sender, RwLock}, io::{AsyncRead, AsyncWrite},
};
use tokio_tungstenite::tungstenite::{handshake::server::Callback, http::HeaderValue};

use crate::{connection::Connection, inflight::Inflight, plugin_manager::PluginManager, router::RouterCmd, session::{Session, SessionHandle, SessionManager, WillMessage}, settings::Settings, topic::TopicManager};

use samoye_mqtt::v3::connack::ConnAckPacketBuilder;


pub mod tcp_listener;
pub mod tcp_tls_listener;
pub mod ws_listener;
pub mod wss_listener;
pub mod websocket_tunnel;
pub mod websocket_tls_tunnel;

struct WsCallBack {}

impl Callback for WsCallBack {
    fn on_request(
        self,
        request: &tokio_tungstenite::tungstenite::handshake::server::Request,
        response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> std::prelude::v1::Result<tokio_tungstenite::tungstenite::handshake::server::Response, tokio_tungstenite::tungstenite::handshake::server::ErrorResponse> {
        let protocol = request.headers().get("Sec-WebSocket-Protocol").unwrap().to_str().unwrap().to_string();
        let mut mut_response = response.clone();
        mut_response.headers_mut().append("Sec-WebSocket-Protocol", HeaderValue::from_str(protocol.as_str()).unwrap());
        std::prelude::v1::Ok(mut_response)
    }
}

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
        samoye_mqtt::MqttPacketV3::Connect(packet) => {
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
                        samoye_mqtt::v3::connack::ConnackReturnCode::UnsupportedProtocolVersion,
                    )
                    .build();
                if let Err(e) = connection
                    .write_packet(&samoye_mqtt::MqttPacketV3::Connack(connack_packet))
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
                .do_connect_authenticate(&packet);

            match r {
                Ok(auth_result) => {
                    match auth_result {
                        AuthenticationResult::Success(tenant_id) => {

                            let connack_packet = ConnAckPacketBuilder::new()
                                .set_return_code(
                                    samoye_mqtt::v3::connack::ConnackReturnCode::Accpet,
                                )
                                .build();
                            if let Err(e) = connection
                                .write_packet(&samoye_mqtt::MqttPacketV3::Connack(
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

                            let will_message = match packet.variable_header.will_flag {
                                true => {
                                    Some(WillMessage{
                                        will_topic: packet.payload.will_topic.unwrap(),
                                        will_message: packet.payload.will_message.unwrap().into(),
                                        will_qos: packet.variable_header.will_qos,
                                        will_retain: packet.variable_header.will_retain,
                                    })
                                }
                                false => None
                            };

                            let username =packet.payload.username.clone();

                            

                            let new_session = Session {
                                username: username,
                                will_message: will_message,
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

                            info!("quit waiter done");

                            if packet.variable_header.clean_session {
                                info!("start clean session, client id {}", packet.payload.client_identifier);
                                let mut session_manager = session_manager.write().await;
                                let _ = session_manager
                                    .remove(
                                        tenant_id.clone(),
                                        packet.payload.client_identifier.clone(),
                                    )
                                    .await;
                            } else {
                                info!("not clean session, client id {}, set the session into offline mode", packet.payload.client_identifier);
                                let mut session_manager = session_manager.write().await;
                                let session_handle_result = session_manager
                                    .get_session_handle(
                                        tenant_id.clone(),
                                        packet.payload.client_identifier.clone(),
                                    )
                                    .await;
                                if session_handle_result.is_ok() {
                                    let mut session_handle = session_handle_result.unwrap();
                                    session_handle.into_offline().await;
                                    info!("set the session into offline mode done");
                                } else {
                                    warn!("get session handle error , error: {:?}", session_handle_result.err());
                                }
                            }
                        }
                        AuthenticationResult::Fail(return_code) => {
                            println!("forbidden");
                            let connect_ack_return_code =match return_code {
                                samoye_plugin::plugin::ConnectReturnCode::ConnectionForbidenUnauth => samoye_mqtt::v3::connack::ConnackReturnCode::InvalidUsernameOrPassword,
                                samoye_plugin::plugin::ConnectReturnCode::ConnectionForbidenInvalidClientIdentifier => samoye_mqtt::v3::connack::ConnackReturnCode::InvalidClientIdentifier,
                                samoye_plugin::plugin::ConnectReturnCode::ConnectionForbidenUnsupportUsernameOrPasswordFormat => samoye_mqtt::v3::connack::ConnackReturnCode::InvalidUsernameOrPassword,
                                _ => samoye_mqtt::v3::connack::ConnackReturnCode::ServerUnavailable
                            };
                            let connack_packet = ConnAckPacketBuilder::new().set_return_code(connect_ack_return_code).build();
                            if let Err(e) = connection
                                .write_packet(&samoye_mqtt::MqttPacketV3::Connack(
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
