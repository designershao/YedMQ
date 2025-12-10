use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;

use actix::prelude::*;
use actix::{Actor, Addr, Context};
use bytes::{Buf, BytesMut};
use log::{error, info, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use yedmq_mqtt::v3::connack::{self, ConnAckPacketBuilder, ConnackReturnCode};
use yedmq_mqtt::v3::connect::ConnectPacket;
use yedmq_mqtt::MqttPacketV3;
use yedmq_plugin_host::plugin_manager::{AuthenticateResult, PluginManager};
use yedmq_plugin_host::protocol::plugin_protocol::AuthenticateRequest;

use crate::session::session_actor::SessionActorMessage;
use crate::session::session_manager_actor::CreateSessionMessage;
use crate::session::{session_actor, WillMessage};

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {

    #[error("unsupported protocol version. Supported versions: {supported_versions:?}, current version: {current_version}")]
    UnsupportedProtocolVersion {
        supported_versions: Vec<String>,
        current_version: String,
    },

    #[error("connection closed")]
    ConnectionClosed,

    #[error("max message size exceeded {0}")]
    MaxMessageSizeExceeded(String),

    #[error("packet parse error {0}")]
    PacketParseError(String),

    #[error("io error {0}")]
    Io(#[from] std::io::Error),

    #[error("create session error {0}")]
    SessionManagerServiceUnavailable(String),

    #[error("connection unauthorized, reason {0}")]
    Unauthenticate(String),

    #[error("plugin error {0}")]
    PluginError(#[from] yedmq_plugin_host::plugin_manager::PluginManagerError),
}

#[derive(Message, Debug)]
#[rtype(result = "Result<(), ConnectionError>")]
pub enum ConnectionActorMessage {
    WritePacketToClient(MqttPacketV3),
    Disconnect,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct UpdateSession {
    pub session: Recipient<SessionActorMessage>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct NotifyUpdateDisconnectedNormally {
    pub disconnected_normally: bool,
}

pub struct ConnectionActor<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    peer_addr: SocketAddr,
    pub reader: Rc<RefCell<tokio::io::ReadHalf<T>>>,
    pub writer: Rc<RefCell<tokio::io::WriteHalf<T>>>,
    pub plugin_service: Arc<PluginManager>,
    pub max_message_size: u32,
    pub disconnected_normally: bool,
    pub buffer_size: usize,
    session: Option<Recipient<SessionActorMessage>>,
    read_packet_handle: Option<SpawnHandle>,
    pub client_certificate: Option<Vec<u8>>,
}

impl<T> ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(
        stream: T,
        max_message_size: u32,
        default_buffer_size: usize,
        peer_addr: SocketAddr,
        plugin_service: Arc<PluginManager>,
        client_certificate: Option<Vec<u8>>,
    ) -> ConnectionActor<T> {
        let (reader, writer) = tokio::io::split(stream);
        ConnectionActor {
            reader: Rc::new(RefCell::new(reader)),
            writer: Rc::new(RefCell::new(writer)),
            client_certificate,
            max_message_size,
            disconnected_normally: false,
            buffer_size: default_buffer_size,
            peer_addr,
            plugin_service,
            session: None,
            read_packet_handle: None,
        }
    }
}

pub async fn write_packet<T: AsyncWrite>(
    writer: Rc<RefCell<tokio::io::WriteHalf<T>>>,
    packet: &MqttPacketV3,
) -> tokio::io::Result<()> {
    writer.borrow_mut().write_all(&packet.to_bytes()).await?;
    writer.borrow_mut().flush().await?;
    Ok(())
}

pub async fn read_packet<T: AsyncRead + Unpin>(
    reader: &Rc<RefCell<tokio::io::ReadHalf<T>>>,
    buffer: &mut BytesMut,
    max_message_size: u32,
) -> Result<MqttPacketV3, ConnectionError> {
    loop {
        let packet_result: std::prelude::v1::Result<
            (&[u8], (&[u8], MqttPacketV3)),
            nom::Err<nom::error::Error<&[u8]>>,
        > = yedmq_mqtt::parse(buffer, max_message_size);

        if let Ok((_, (consumed_bytes, packet))) = packet_result {
            buffer.advance(consumed_bytes.len());
            return Ok(packet);
        } else {
            let err = packet_result.err().unwrap();
            match err {
                nom::Err::Incomplete(_) => {
                    let n = reader.borrow_mut().read_buf(buffer).await?;

                    if 0 == n {
                        if buffer.is_empty() {
                            return Err(ConnectionError::ConnectionClosed);
                        } else {
                            return Err(ConnectionError::ConnectionClosed);
                        }
                    }
                }
                nom::Err::Error(err_inner) => {
                    if err_inner.code == nom::error::ErrorKind::Verify {
                        warn!("Message size exceeds maximum allowed size of the system.");
                        return Err(ConnectionError::MaxMessageSizeExceeded(
                            "Message size exceeds maximum allowed size of the system.".to_string(),
                        ));
                    } else {
                        error!("read packet error: {:?}", err_inner);
                        return Err(ConnectionError::PacketParseError(
                            "Invalid MQTT Packet".to_string(),
                        ));
                    }
                }
                _ => {
                    error!("read packet error: {:?}", err);
                    return Err(ConnectionError::PacketParseError(
                        "Invalid MQTT Packet".to_string(),
                    ));
                }
            }
        }
    }
}

pub struct HandleInitialConnectResult {
    pub session_recipient: Recipient<SessionActorMessage>,
    pub session_present: bool,
}

async fn handle_initial_connect<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    packet: ConnectPacket,
    plugin_service: Arc<PluginManager>,
    self_addr: &Addr<ConnectionActor<T>>,
    peer_addr: SocketAddr,
    client_certificate: Option<Vec<u8>>,
) -> Result<HandleInitialConnectResult, ConnectionError> {
    // invalid mqtt protocol name
    if packet.variable_header.protocol_name != "MQTT" {
        warn!("invalid mqtt protocol name");
        return Err(ConnectionError::PacketParseError(
            "invalid mqtt protocol name".to_string(),
        ));
    }
    //

    // unsupported protocol version
    if packet.variable_header.protocol_level != 4 {
        return Err(ConnectionError::UnsupportedProtocolVersion {
            supported_versions: vec!["3.1.1".to_string()],
            current_version: packet.variable_header.protocol_level.to_string(),
        });
    }
    //

    let authenticate_request = AuthenticateRequest {
        client_id: packet.payload.client_identifier.clone(),
        username: packet.payload.username.as_ref().unwrap_or(&"".to_string()).clone(),
        password: packet.payload.password.as_ref().unwrap_or(&"".to_string()).clone(),
        client_ip: peer_addr.ip().to_string(),
        client_cert: client_certificate.unwrap_or_default(),
        protocol_version: "3.1.1".to_string(),
        properties: None,
    };

    let plugin_authenticate_result = plugin_service.call_authenticate_hook(authenticate_request).await;

    match plugin_authenticate_result {
        Ok(result) => match result {
            AuthenticateResult{
                authenticated: true,
                tenant_id,
                ..
            } => {
                let will_message = match packet.variable_header.will_flag {
                    true => Some(WillMessage {
                        will_topic: packet.payload.will_topic.unwrap_or("".to_string()),
                        will_message: packet
                            .payload
                            .will_message
                            .unwrap_or("".to_string())
                            .into_bytes(),
                        will_qos: packet.variable_header.will_qos,
                        will_retain: packet.variable_header.will_retain,
                    }),
                    false => None,
                };
                let tenant_id = tenant_id.unwrap_or("public".to_string());
                let recipient = self_addr.clone().recipient();
                let session_manager_actor_addr = crate::session::session_manager_actor::SessionManagerActor::from_registry();
                let recipient = session_manager_actor_addr
                    .send(CreateSessionMessage {
                        tenant_id,
                        client_id: packet.payload.client_identifier.clone(),
                        clean_session: packet.variable_header.clean_session,
                        connection_addr: recipient.clone(),
                        keep_alive: packet.variable_header.keep_alive as u64,
                        will_message,
                        peer_addr,
                        username: packet.payload.username.clone(),
                    })
                    .await
                    .map_err(|e| match e {
                        MailboxError::Closed => {
                            error!("session manager actor mailbox closed");
                            ConnectionError::SessionManagerServiceUnavailable("Session manager actor mailbox closed".to_string())
                        }
                        MailboxError::Timeout => {
                            error!("session manager actor mailbox timeout");
                            ConnectionError::SessionManagerServiceUnavailable("Session manager actor mailbox timeout".to_string())
                        }
                    })?
                    .map_err(|e| ConnectionError::SessionManagerServiceUnavailable(e.to_string()))
                    .map(|r| HandleInitialConnectResult { 
                        session_recipient: r.session_actor_recipient, 
                        session_present: r.session_present }
                    );
                recipient
            }
            AuthenticateResult {
                authenticated: false,
                ..
            } => {
                Err(ConnectionError::Unauthenticate("authenticate plugin rejected".to_string()))
            }
        },
        Err(e) => {
            Err(ConnectionError::PluginError(e))
        }
    }
}

impl<T> Actor for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let max_message_size = self.max_message_size;
        let self_addr = ctx.address();
        let reader = self.reader.clone();
        let mut buffer = BytesMut::with_capacity(self.buffer_size);
        let plugin_service = self.plugin_service.clone();
        let peer_addr = self.peer_addr;
        let client_cert = self.client_certificate.clone();
        let handle = ctx.spawn(
            async move {
                let first_packet = read_packet(&reader, &mut buffer, max_message_size).await;
                if let Ok(packet) = first_packet {
                    match packet {
                        MqttPacketV3::Connect(packet) => {
                            match handle_initial_connect(
                                packet,
                                plugin_service,
                                &self_addr,
                                peer_addr,
                                client_cert
                            )
                            .await {
                                Ok(handle_initial_connect_result) => {
                                    let connack = ConnAckPacketBuilder::new()
                                        .set_return_code(ConnackReturnCode::Accept)
                                        .set_session_present(handle_initial_connect_result.session_present)
                                        .build();

                                    if let Err(e) = self_addr
                                        .send(ConnectionActorMessage::WritePacketToClient(
                                            yedmq_mqtt::MqttPacketV3::Connack(connack),
                                        ))
                                    .await {
                                        error!("send connack packet error, connection may be closed: {}", e);
                                        self_addr.do_send(ConnectionActorMessage::Disconnect);
                                        return;
                                    }

                                    self_addr.send(UpdateSession { session: handle_initial_connect_result.session_recipient.clone() }).await.unwrap();

                                    loop {
                                        let packet =
                                            read_packet(&reader, &mut buffer, max_message_size).await;
                                        if let Ok(packet) = packet {
                                            if matches!(packet, MqttPacketV3::Disconnect(_)) {
                                                self_addr
                                                    .send(NotifyUpdateDisconnectedNormally {
                                                        disconnected_normally: true,
                                                    })
                                                    .await
                                                    .unwrap();
                                            }
                                            handle_initial_connect_result.session_recipient.do_send(
                                                session_actor::SessionActorMessage::InboundPacket(
                                                    packet,
                                                ),
                                            );
                                        } else {
                                            break;
                                        }
                                    }
                                },
                                Err(e) => {
                                    let connack_packet = match e {
                                        ConnectionError::UnsupportedProtocolVersion { .. } => {
                                            ConnAckPacketBuilder::new()
                                                .set_return_code(ConnackReturnCode::UnsupportedProtocolVersion)
                                                .build()
                                        }
                                        ConnectionError::Unauthenticate(reason) => {
                                            error!("connection unauthenticated: {}", reason);
                                            ConnAckPacketBuilder::new()
                                                .set_return_code(ConnackReturnCode::UnAuthorized)
                                                .build()
                                        }
                                        ConnectionError::PluginError(e) => {
                                            error!("handle initial connect error: {}", e);
                                            ConnAckPacketBuilder::new()
                                                .set_return_code(ConnackReturnCode::ServerUnavailable)
                                                .build()
                                        }
                                        ConnectionError::SessionManagerServiceUnavailable(e) => {
                                            error!("handle initial connect error: {}", e);
                                            ConnAckPacketBuilder::new()
                                                .set_return_code(ConnackReturnCode::ServerUnavailable)
                                                .build()
                                        }
                                        _ => {
                                            error!("handle initial connect error: {}", e);
                                            ConnAckPacketBuilder::new()
                                                .set_return_code(ConnackReturnCode::ServerUnavailable)
                                                .build()
                                        }
                                    };
                                    if let Err(e) = self_addr
                                        .send(ConnectionActorMessage::WritePacketToClient(
                                            yedmq_mqtt::MqttPacketV3::Connack(connack_packet),
                                        ))
                                        .await {
                                            error!("send connack packet error: {}", e);
                                        }
                                    self_addr.do_send(ConnectionActorMessage::Disconnect);
                                    return;
                                }
                            }
                        }
                        _ => {
                            error!("client first packet is not connect packet");
                            return;
                        }
                    }
                } else {
                    error!("client first packet is not connect packet");
                    let connack_packet = ConnAckPacketBuilder::new()
                        .set_return_code(connack::ConnackReturnCode::UnsupportedProtocolVersion)
                        .build();
                    if let Err(e) = self_addr
                        .send(ConnectionActorMessage::WritePacketToClient(
                            yedmq_mqtt::MqttPacketV3::Connack(connack_packet),
                        ))
                        .await {
                            error!("send connack packet error: {}", e);
                        }
                    return;
                }
                self_addr.do_send(ConnectionActorMessage::Disconnect);
            }
            .into_actor(self),
        );
        self.read_packet_handle = Some(handle);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if self.session.is_some() {
            if !self.disconnected_normally {
                warn!(
                    "ConnectionActor stopped unexpectedly! Sending UnexpectDisconnect to session"
                );
                self.session
                    .clone()
                    .unwrap()
                    .do_send(session_actor::SessionActorMessage::UnexpectClientDisconnected);
            } else {
                self.session
                    .clone()
                    .unwrap()
                    .do_send(session_actor::SessionActorMessage::ClientDisconnected);
            }
        }
    }
}

impl<T> Handler<UpdateSession> for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: UpdateSession, _ctx: &mut Self::Context) -> Self::Result {
        self.session = Some(msg.session);
    }
}

impl<T> Drop for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn drop(&mut self) {
        info!("🔥 ConnectionActor Dropped!");
    }
}

impl<T> Handler<ConnectionActorMessage> for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ResponseFuture<Result<(), ConnectionError>>;

    fn handle(&mut self, msg: ConnectionActorMessage, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            ConnectionActorMessage::WritePacketToClient(packet) => {
                let writer = self.writer.clone();
                Box::pin(async move {
                    write_packet(writer, &packet).await?;
                    Ok(())
                })
            }
            ConnectionActorMessage::Disconnect => {
                info!("connection received disconnect message");
                ctx.stop();

                let writer = self.writer.clone();
                Box::pin(async move {
                    writer.borrow_mut().shutdown().await?;
                    Ok(())
                })
            }
        }
    }
}

impl<T> Handler<NotifyUpdateDisconnectedNormally> for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(
        &mut self,
        msg: NotifyUpdateDisconnectedNormally,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.disconnected_normally = msg.disconnected_normally;
    }
}