use std::{net::SocketAddr, sync::Arc, time::Duration};

use log::{debug, error, warn};
use yedmq_plugin::plugin::{AuthenticationResultValue, Client, ClientProperties};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    select,
    sync::{mpsc::Sender, Mutex, RwLock},
};
use tokio_tungstenite::tungstenite::{handshake::server::Callback, http::HeaderValue};

use crate::{
    connection::Connection, inflight::Inflight, metric::Metric, plugin_manager::{PluginManager, PluginService}, router::RouterCmd, session::{session_manager::{
        ConnectionMessage, KickOffReason, Session, SessionContext, SessionManager, SessionMessage,
        SessionWrapper,
    }, WillMessage}, settings::Settings, topic::TopicManager
};

use yedmq_mqtt::v3::connack::ConnAckPacketBuilder;

pub mod tcp_listener;
pub mod tcp_tls_listener;
pub mod websocket_tls_tunnel;
pub mod websocket_tunnel;
pub mod ws_listener;
pub mod wss_listener;

struct WsCallBack {}

impl Callback for WsCallBack {
    fn on_request(
        self,
        request: &tokio_tungstenite::tungstenite::handshake::server::Request,
        response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> std::prelude::v1::Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        let protocol = request
            .headers()
            .get("Sec-WebSocket-Protocol")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let mut mut_response = response.clone();
        mut_response.headers_mut().append(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(protocol.as_str()).unwrap(),
        );
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
    peer_addr: SocketAddr,
    metric: Arc<Metric>,
) {
    let mut connection: Connection<T> = Connection::new(stream);
    // wait the first connect packet, if the packet is not correct, the connection will be closed.
    let first_packet = connection.read_packet().await;

    if first_packet.is_err() {
        warn!("read first packet error: {}", first_packet.unwrap_err());
        return;
    }

    match first_packet.unwrap() {
        yedmq_mqtt::MqttPacketV3::Connect(packet) => {
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
                        yedmq_mqtt::v3::connack::ConnackReturnCode::UnsupportedProtocolVersion,
                    )
                    .build();
                if let Err(e) = connection
                    .write_packet(&yedmq_mqtt::MqttPacketV3::Connack(connack_packet))
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

            let r = plugin_manager.clone().do_connect_authenticate(&packet);

            match r {
                Ok(auth_result) => match auth_result {
                    AuthenticationResultValue::Success(tenant_id) => {
                        let mut connack_packet_builder = ConnAckPacketBuilder::new()
                            .set_return_code(yedmq_mqtt::v3::connack::ConnackReturnCode::Accpet);

                        //

                        let mut session_sender_option = None;
                        if let Some(session_sender) = session_manager.read().await.get_session_sender(&tenant_id, &packet.payload.client_identifier) {
                            session_sender_option = Some(session_sender);
                        }
                        if let Some(session_sender) = session_sender_option {
                            debug!("session {} already exist, set session present", packet.payload.client_identifier);
                            connack_packet_builder = connack_packet_builder.set_session_present(true);

                            let (quit_sender, mut quit_receiver) = tokio::sync::mpsc::channel(1);

                            if let Err(_) = session_sender.send(SessionMessage::KickOff(KickOffReason::Other("kick off the same client identifiy connection".into(), quit_sender))).await {
                                error!("client id {} send kick off message error", packet.payload.client_identifier);
                                return;
                            }

                            debug!("wait previous session client id {} stop", packet.payload.client_identifier);

                            quit_receiver.recv().await;

                            debug!("previous session client id {} stoped", packet.payload.client_identifier);
                        }
                        //

                        let connack_packet = connack_packet_builder.build();
                        if let Err(e) = connection
                            .write_packet(&yedmq_mqtt::MqttPacketV3::Connack(connack_packet))
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
                                .create_tenant(&tenant_id);
                            let _ = topic_manager
                                .clone()
                                .write()
                                .await
                                .create_tenant(tenant_id.clone());
                        }

                        let client_properties = match packet.variable_header.will_flag {
                            true => {
                                let will_message = packet.payload.will_message.unwrap().to_string();
                                let client_properties = ClientProperties {
                                    username: packet.payload.username.clone(),
                                    clean_session: packet.variable_header.clean_session,
                                    will_retain: packet.variable_header.will_retain,
                                    will_topic: packet.payload.will_topic.clone(),
                                    will_message: Some(will_message.clone().into()),
                                };
                                let will_message = WillMessage {
                                    will_topic: packet.payload.will_topic.unwrap().to_string(),
                                    will_message: will_message.into(),
                                    will_qos: packet.variable_header.will_qos,
                                    will_retain: packet.variable_header.will_retain,
                                };
                                (client_properties, Some(will_message))
                            },
                            false => {
                                let client_properties = ClientProperties {
                                    username: packet.payload.username.clone(),
                                    clean_session: packet.variable_header.clean_session,
                                    will_retain: packet.variable_header.will_retain,
                                    will_topic: None,
                                    will_message: None,
                                };
                                (client_properties, None)
                            },
                        };

                        let (connection_sender, mut connection_receiver) =
                            tokio::sync::mpsc::channel(100);

                        let session_context = SessionContext {
                            connection: connection_sender.clone(),
                            keep_alive: packet.variable_header.keep_alive.into(),
                            clean_session: packet.variable_header.clean_session,
                            client_info: Client {
                                tenant_id: tenant_id.clone(),
                                client_identifier: packet.payload.client_identifier.clone(),
                                properties: client_properties.0,
                                socket_addr: peer_addr,
                            },
                        };

                        let existed_session_sender_option =
                            session_manager.read().await.get_session_sender(
                                &tenant_id,
                                &packet.payload.client_identifier
                            );

                        let (mut session_sender, session_receiver) =
                            tokio::sync::mpsc::channel(100);

                        if existed_session_sender_option.is_none() {
                            // no same client session has existed, create new session event loop
                            if let Err(e) = session_manager.write().await.register(
                                tenant_id.clone(),
                                packet.payload.client_identifier.to_string(),
                                session_sender.clone(),
                            ) {
                                warn!(
                                    "register session error: {}, exit the accept connection loop",
                                    e
                                );
                                return;
                            }

                            let session = Session {
                                will_message: client_properties.1,
                                client_identifier: packet.payload.client_identifier.to_string(),
                                tenant_identifier: tenant_id.to_string(),
                                subscription_topics: vec![],
                                inflight: Arc::new(Mutex::new(Inflight::new(Duration::from_secs(
                                    settings.session.qos_expired_secs,
                                )))),
                            };

                            let mut session_wrapper = SessionWrapper::new(
                                session,
                                session_receiver,
                                plugin_manager.clone(),
                                topic_manager.clone(),
                            );

                            let session_sender_clone = session_sender.clone();
                            let _join_handle = tokio::task::spawn(async move {
                                session_wrapper
                                    .run_event_loop(session_sender_clone, router_sender, session_manager.clone(), settings.session.packet_resend_interval_secs)
                                    .await;
                            });
                            session_sender
                                .send(SessionMessage::Activate(session_context))
                                .await
                                .unwrap();
                        } else {
                            // same client session has existed, update the session sender
                            session_sender = existed_session_sender_option.unwrap();
                            session_sender
                                .send(SessionMessage::Activate(session_context))
                                .await
                                .unwrap();
                        }

                        debug!(
                            "session {} connected from {}",
                            packet.payload.client_identifier, peer_addr);

                        metric.increase_clients_connected();

                        loop {
                            select! {
                                read_packet_result = connection.read_packet() => {
                                    match read_packet_result {
                                        Ok(packet) => {

                                            metric.increase_bytes_received(packet.to_bytes().len() as u64);  

                                            if let Err(e) = session_sender
                                                .send(SessionMessage::ReceiveFromClient(packet))
                                                .await {
                                                    warn!("socket: {}, send packet error: {}, session sender may be closed, exit the accept connection loop", peer_addr, e);
                                                    if let Err(e) = connection.shutdown().await {
                                                        warn!("shutdown connection error: {}", e);
                                                    };
                                                    return;
                                                }
                                        }
                                        Err(e) => {
                                            warn!("session {} read packet error: {}", packet.payload.client_identifier , e);
                                            if let Err(e) = session_sender
                                                .send(SessionMessage::KickOff(KickOffReason::InvalidMqttPacket))
                                                .await {
                                                    warn!("send packet error: {}", e);
                                                }
                                            if let Err(e) = connection.shutdown().await {
                                                warn!("shutdown connection error: {}", e);
                                            };
                                            return;
                                        }
                                    }
                                }
                                msg = connection_receiver.recv() => {
                                    match msg {
                                        Some(msg) => {
                                            match msg {
                                                ConnectionMessage::Disconnect => {
                                                    connection.shutdown().await.unwrap();
                                                    return;
                                                }
                                                ConnectionMessage::WritePacket(msg) => {

                                                    if let Err(e) = connection.write_packet(&msg).await {
                                                        warn!("write packet error: {}", e);
                                                    }

                                                    metric.increase_bytes_sent(msg.to_bytes().len() as u64);  
                                                }
                                            }
                                        }
                                        None => {
                                            connection.shutdown().await.unwrap();
                                            if let Err(e) = session_sender
                                                .send(SessionMessage::InActivate).await {
                                                    warn!("send packet error: {}", e);
                                                }

                                            metric.decrease_clients_connected();

                                            return;
                                        }
                                    }
                                }

                            }
                        }
                    }
                    AuthenticationResultValue::Fail(return_code) => {
                        println!("forbidden");
                        let connect_ack_return_code =match return_code {
                                yedmq_plugin::plugin::ConnectReturnCode::ConnectionForbidenUnauth => yedmq_mqtt::v3::connack::ConnackReturnCode::InvalidUsernameOrPassword,
                                yedmq_plugin::plugin::ConnectReturnCode::ConnectionForbidenInvalidClientIdentifier => yedmq_mqtt::v3::connack::ConnackReturnCode::InvalidClientIdentifier,
                                yedmq_plugin::plugin::ConnectReturnCode::ConnectionForbidenUnsupportUsernameOrPasswordFormat => yedmq_mqtt::v3::connack::ConnackReturnCode::InvalidUsernameOrPassword,
                                _ => yedmq_mqtt::v3::connack::ConnackReturnCode::ServerUnavailable
                            };
                        let connack_packet = ConnAckPacketBuilder::new()
                            .set_return_code(connect_ack_return_code)
                            .build();
                        if let Err(e) = connection
                            .write_packet(&yedmq_mqtt::MqttPacketV3::Connack(connack_packet))
                            .await
                        {
                            warn!("write connack packet error: {}", e);
                        }
                        if let Err(e) = connection.shutdown().await {
                            warn!("shutdown connection error: {}", e);
                        }
                    }
                },
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
