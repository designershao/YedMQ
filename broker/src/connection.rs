use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use actix::prelude::*;
use actix::{Actor, Addr, Context};
use bytes::{Buf, Bytes, BytesMut};
use governor::clock::{Clock, DefaultClock};
use governor::state::{direct::NotKeyed, InMemoryState};
use governor::{Quota, RateLimiter};
use log::{debug, error, info, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use yedmq_mqtt::packet::{
    Connack as NeutralConnack, Connect, Packet, Properties, ProtocolVersion, ReasonCode,
};
use yedmq_mqtt::v3::connack::{ConnAckPacketBuilder, ConnackReturnCode};
use yedmq_mqtt::MqttPacketV3;
use yedmq_plugin_host::plugin_manager::{AuthenticateResult, PluginManager};
use yedmq_plugin_host::protocol::plugin_protocol::AuthenticateRequest;

use crate::metric::Metric;
use crate::mqtt_message_expiry;
use crate::mqtt_properties::properties_to_struct;
use crate::session::session_actor::SessionActorMessage;
use crate::session::session_manager_actor::CreateSessionMessage;
use crate::session::{session_actor, WillMessage};
use crate::settings::RateLimit;

type ConnectionRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

fn build_rate_limiter(rate_limit: &RateLimit) -> Option<ConnectionRateLimiter> {
    if rate_limit.messages_rate <= 0 || rate_limit.messages_burst <= 0 {
        warn!(
            "rate limit disabled due to invalid config: messages_rate={}, messages_burst={}",
            rate_limit.messages_rate, rate_limit.messages_burst
        );
        return None;
    }

    let rate = NonZeroU32::new(rate_limit.messages_rate as u32)?;
    let burst = NonZeroU32::new(rate_limit.messages_burst as u32)?;

    Some(RateLimiter::direct(
        Quota::per_second(rate).allow_burst(burst),
    ))
}

fn is_transient_initial_connect_error(error: &ConnectionError) -> bool {
    match error {
        ConnectionError::SessionManagerServiceUnavailable(message) => {
            message.contains("newer session has existed")
                || message.contains("Session version rejected")
        }
        _ => false,
    }
}

fn log_initial_connect_error(error: &ConnectionError) {
    if is_transient_initial_connect_error(error) {
        warn!("handle initial connect transiently rejected: {}", error);
    } else {
        error!("handle initial connect error: {}", error);
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub enum NetworkEvent {
    WriteError(std::io::Error),
    ReadError(std::io::Error),
    ClientDisconnected,
}

pub enum NetworkCommand {
    Send(Bytes),
    Shutdown,
}

pub struct NetworkSender {
    tx: mpsc::Sender<NetworkCommand>,
    task_handle: Option<JoinHandle<()>>,
}

impl NetworkSender {
    pub fn spawn<T>(
        mut writer: tokio::io::WriteHalf<T>,
        event_tx: mpsc::UnboundedSender<NetworkEvent>,
        metric: Arc<Metric>,
    ) -> Self
    where
        T: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, mut rx) = mpsc::channel::<NetworkCommand>(1000);

        let task_handle = tokio::spawn(async move {
            let mut batch = BytesMut::with_capacity(64 * 1024);
            let mut msg_count = 0;
            let mut interval = tokio::time::interval(Duration::from_millis(10));

            'main_loop: loop {
                tokio::select! {
                    cmd = rx.recv() => {
                        match cmd {
                            Some(NetworkCommand::Send(data)) => {
                                batch.extend_from_slice(&data);
                                msg_count += 1;

                                if batch.len() >= 64 * 1024 || msg_count >= 200 {
                                    if let Err(e) = Self::flush(&mut writer, &mut batch, &mut msg_count, &metric).await {
                                        error!("Write error: {}", e);
                                        let _ = event_tx.send(NetworkEvent::WriteError(e));
                                        break 'main_loop;
                                    }
                                }
                            }

                            Some(NetworkCommand::Shutdown) => {
                                info!("NetworkSender received shutdown command");
                                break 'main_loop;
                            }

                            None => {
                                info!("NetworkSender command channel closed");
                                break 'main_loop;
                            }
                        }
                    }

                    _ = interval.tick() => {
                        if !batch.is_empty() {
                            if let Err(e) = Self::flush(&mut writer, &mut batch, &mut msg_count, &metric).await {
                                error!("Periodic flush error: {}", e);
                                let _ = event_tx.send(NetworkEvent::WriteError(e));
                                break 'main_loop;
                            }
                        }
                    }
                }
            }

            info!("NetworkSender cleanup started");

            if !batch.is_empty() {
                info!("Flushing {} remaining bytes", batch.len());
                let _ = Self::flush(&mut writer, &mut batch, &mut msg_count, &metric).await;
            }

            let mut remaining = 0;
            while let Ok(cmd) = rx.try_recv() {
                if let NetworkCommand::Send(data) = cmd {
                    batch.extend_from_slice(&data);
                    remaining += 1;
                }
            }

            if !batch.is_empty() {
                info!("Flushing {} remaining messages", remaining);
                let _ = Self::flush(&mut writer, &mut batch, &mut msg_count, &metric).await;
            }

            if let Err(e) = writer.shutdown().await {
                error!("Shutdown error: {}", e);
            }

            info!("NetworkSender task exited");
        });

        Self {
            tx,
            task_handle: Some(task_handle),
        }
    }

    async fn flush<T>(
        writer: &mut tokio::io::WriteHalf<T>,
        batch: &mut BytesMut,
        count: &mut usize,
        metric: &Arc<Metric>,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        if batch.is_empty() {
            return Ok(());
        }

        let len = batch.len();
        writer.write_all(batch).await?;
        writer.flush().await?;

        metric.increase_bytes_sent(len as u64);
        for _ in 0..*count {
            metric.increase_packets_sent();
        }

        batch.clear();
        *count = 0;
        Ok(())
    }

    pub fn try_send(&self, data: Bytes) -> Result<(), mpsc::error::TrySendError<NetworkCommand>> {
        self.tx.try_send(NetworkCommand::Send(data))
    }

    pub async fn shutdown(mut self) {
        info!("NetworkSender shutdown initiated");

        let _ = self.tx.send(NetworkCommand::Shutdown).await;

        if let Some(handle) = self.task_handle.take() {
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => info!("NetworkSender task completed"),
                Ok(Err(e)) => error!("NetworkSender task panicked: {:?}", e),
                Err(_) => warn!("NetworkSender shutdown timeout"),
            }
        }
    }
}

impl Drop for NetworkSender {
    fn drop(&mut self) {
        if let Some(handle) = self.task_handle.take() {
            warn!("NetworkSender dropped without proper shutdown, aborting task");
            handle.abort();
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("unsupported protocol version. Supported versions: {supported_versions:?}, current version: {current_version}"
    )]
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

    #[error("unsupported MQTT 5 feature {0}")]
    UnsupportedMqtt5Feature(&'static str),

    #[error("MQTT 5 protocol error {reason_code:?}: {message}")]
    Mqtt5ProtocolError {
        reason_code: ReasonCode,
        message: String,
    },

    #[error("plugin error {0}")]
    PluginError(#[from] yedmq_plugin_host::plugin_manager::PluginManagerError),
}

#[derive(Debug)]
pub enum DisconnectReason {
    Normal,
    KeepAliveExpired,
    RateLimited,
    InternalError(String),
}

#[derive(Message, Debug)]
#[rtype(result = "Result<(), ConnectionError>")]
pub enum ConnectionActorMessage {
    WritePacketToClient(Packet),
    Disconnect(DisconnectReason),
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct UpdateSession {
    pub session: Recipient<SessionActorMessage>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct UpdateProtocolVersion {
    pub protocol_version: ProtocolVersion,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct NotifyUpdateDisconnectedNormally {
    pub disconnected_normally: bool,
}

pub struct ConnectionActor<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    peer_addr: SocketAddr,
    network_sender: Option<NetworkSender>,

    // batch buffer
    encode_buffer: BytesMut,
    pending_count: usize,
    batch_size: usize,
    //

    // config
    pub max_message_size: u32,
    pub buffer_size: usize,
    pub client_certificate: Option<Vec<u8>>,
    //

    // plugin service
    pub plugin_service: Arc<PluginManager>,

    pub disconnected_normally: bool,
    protocol_version: ProtocolVersion,
    session: Option<Recipient<SessionActorMessage>>,

    read_packet_handle: Option<SpawnHandle>,
    event_listener_handle: Option<SpawnHandle>,

    pub metric: Arc<Metric>,

    _phantom: std::marker::PhantomData<T>,
}

pub struct ConnectionActorStartConfig {
    pub max_message_size: u32,
    pub default_buffer_size: usize,
    pub peer_addr: SocketAddr,
    pub plugin_service: Arc<PluginManager>,
    pub client_certificate: Option<Vec<u8>>,
    pub metric: Arc<Metric>,
    pub rate_limit: RateLimit,
}

impl<T> ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn create_and_start(stream: T, config: ConnectionActorStartConfig) -> Addr<Self> {
        let addr = ConnectionActor::create(move |ctx| {
            let (mut actor, mut reader, mut event_rx) = Self::new(
                stream,
                config.max_message_size,
                config.default_buffer_size,
                config.peer_addr,
                config.plugin_service.clone(),
                config.client_certificate.clone(),
                config.metric.clone(),
            );

            let self_addr = ctx.address();

            let event_addr = self_addr.clone();
            let event_handle = ctx.spawn(
                async move {
                    while let Some(event) = event_rx.recv().await {
                        event_addr.do_send(event);
                    }
                }
                .into_actor(&actor),
            );
            actor.event_listener_handle = Some(event_handle);
            let read_addr = self_addr.clone();
            let max_msg_size = actor.max_message_size;
            let buf_size = actor.buffer_size;
            let plugin_svc = config.plugin_service.clone();
            let peer = config.peer_addr;
            let cert = config.client_certificate.clone();
            let metric_clone = config.metric.clone();
            let rate_limiter = build_rate_limiter(&config.rate_limit);

            let handle = ctx.spawn(
                async move {
                    let mut buffer = BytesMut::with_capacity(buf_size);

                    let (first_packet, protocol_version) = match read_packet(&mut reader, &mut buffer, max_msg_size, None, Some(metric_clone.clone())).await {
                        Ok(packet) => packet,
                        Err(e) => {
                            if let ConnectionError::UnsupportedProtocolVersion { .. } = &e {
                                if let Err(send_error) = read_addr
                                    .send(ConnectionActorMessage::WritePacketToClient(
                                        error_connack_packet(ProtocolVersion::V3_1_1, &e),
                                    ))
                                    .await {
                                    error!("send unsupported protocol CONNACK error: {}", send_error);
                                }
                                read_addr.do_send(ConnectionActorMessage::Disconnect(DisconnectReason::InternalError("unsupported protocol version".to_string())));
                                return;
                            }
                            if let ConnectionError::UnsupportedMqtt5Feature(_) = &e {
                                if read_addr
                                    .send(UpdateProtocolVersion {
                                        protocol_version: ProtocolVersion::V5_0,
                                    })
                                    .await
                                    .is_err()
                                {
                                    error!("failed to update protocol version before MQTT 5 error CONNACK");
                                }
                                if let Err(send_error) = read_addr
                                    .send(ConnectionActorMessage::WritePacketToClient(
                                        error_connack_packet(ProtocolVersion::V5_0, &e),
                                    ))
                                    .await {
                                    error!("send unsupported MQTT 5 feature CONNACK error: {}", send_error);
                                }
                                read_addr.do_send(ConnectionActorMessage::Disconnect(DisconnectReason::InternalError("unsupported MQTT 5 feature".to_string())));
                                return;
                            }
                            error!("Failed to read first packet: {}", e);
                            read_addr.do_send(NetworkEvent::ReadError(
                                std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                            ));
                            return;
                        }
                    };

                    match first_packet {
                        Packet::Connect(packet) => {
                            if read_addr
                                .send(UpdateProtocolVersion { protocol_version })
                                .await
                                .is_err() {
                                error!("Failed to send UpdateProtocolVersion message to self. The actor is likely shutting down.");
                                read_addr.do_send(ConnectionActorMessage::Disconnect(DisconnectReason::InternalError("Failed to send UpdateProtocolVersion message to self. The actor is likely shutting down.".to_string())));
                                return;
                            }

                            match handle_initial_connect(
                                packet,
                                plugin_svc,
                                &read_addr,
                                peer,
                                cert,
                                metric_clone.clone(),
                            ).await {
                                Ok(result) => {
                                    let connack = success_connack_packet(
                                        protocol_version,
                                        result.session_present,
                                        max_msg_size,
                                    );

                                    if let Err(e) = read_addr
                                        .send(ConnectionActorMessage::WritePacketToClient(
                                            connack,
                                        ))
                                        .await {
                                        error!("send connack packet error, connection may be closed: {}", e);
                                        read_addr.do_send(ConnectionActorMessage::Disconnect(DisconnectReason::InternalError("send connack packet error, connection may be closed".to_string())));
                                        return;
                                    }

                                    if read_addr.send(UpdateSession { session: result.session_recipient.clone() }).await.is_err() {
                                        error!("Failed to send UpdateSession message to self. The actor is likely shutting down.");
                                        read_addr.do_send(ConnectionActorMessage::Disconnect(DisconnectReason::InternalError("Failed to send UpdateSession message to self. The actor is likely shutting down.".to_string())));
                                        return;
                                    }
                                    loop {
                                        match read_packet(&mut reader, &mut buffer, max_msg_size, Some(protocol_version), Some(metric_clone.clone())).await {
                                            Ok((packet, _)) => {
                                                metric_clone.increase_packets_received();
                                                if matches!(packet, Packet::Auth(_)) {
                                                    if let Err(e) = read_addr
                                                        .send(ConnectionActorMessage::WritePacketToClient(
                                                            server_disconnect_packet(ReasonCode::BadAuthenticationMethod),
                                                        ))
                                                        .await {
                                                        error!("send MQTT 5 AUTH rejection DISCONNECT error: {}", e);
                                                    }
                                                    read_addr.do_send(ConnectionActorMessage::Disconnect(DisconnectReason::InternalError("MQTT 5 enhanced authentication is not supported".to_string())));
                                                    break;
                                                }
                                                if matches!(packet, Packet::Disconnect(_)) && read_addr
                                                    .send(NotifyUpdateDisconnectedNormally {
                                                        disconnected_normally: true,
                                                    })
                                                    .await
                                                    .is_err() {
                                                    error!("Failed to send NotifyUpdateDisconnectedNormally message to self. The actor is likely shutting down.");
                                                }

                                                let is_publish = matches!(packet, Packet::Publish(_));
                                                if is_publish {
                                                    if let Some(limiter) = rate_limiter.as_ref() {
                                                        match limiter.check() {
                                                            Ok(_) => {
                                                                result.session_recipient.do_send(
                                                                    session_actor::SessionActorMessage::InboundPacket(
                                                                        packet,
                                                                    ),
                                                                );
                                                            }
                                                        Err(not_ready) => {
                                                            let wait_time = not_ready.wait_time_from(DefaultClock::default().now());
                                                            warn!(
                                                                "rate limited: dropping publish after wait_time={:?}",
                                                                wait_time
                                                            );
                                                            continue;
                                                        }
                                                        }
                                                    } else {
                                                        result.session_recipient.do_send(
                                                            session_actor::SessionActorMessage::InboundPacket(
                                                                packet,
                                                            ),
                                                        );
                                                    }
                                                } else {
                                                    result.session_recipient.do_send(
                                                        session_actor::SessionActorMessage::InboundPacket(
                                                            packet,
                                                        ),
                                                    );
                                                }
                                            }
                                            Err(ConnectionError::ConnectionClosed) => {
                                                info!("Client closed connection");
                                                read_addr.do_send(NetworkEvent::ClientDisconnected);
                                                break;
                                            }
                                            Err(e) => {
                                                error!("Read packet error: {}", e);
                                                if let ConnectionError::Mqtt5ProtocolError {
                                                    reason_code,
                                                    ..
                                                } = &e
                                                {
                                                    if let Err(send_error) = read_addr
                                                        .send(ConnectionActorMessage::WritePacketToClient(
                                                            server_disconnect_packet(*reason_code),
                                                        ))
                                                        .await
                                                    {
                                                        error!("send MQTT 5 protocol-error DISCONNECT error: {}", send_error);
                                                    }
                                                }
                                                read_addr.do_send(NetworkEvent::ReadError(
                                                    std::io::Error::new(
                                                        std::io::ErrorKind::InvalidData,
                                                        e.to_string(),
                                                    )
                                                ));
                                                break;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    let connack_packet = error_connack_packet(protocol_version, &e);
                                    if let Err(e) = read_addr
                                        .send(ConnectionActorMessage::WritePacketToClient(
                                            connack_packet,
                                        ))
                                        .await {
                                        error!("send connack packet error: {}", e);
                                    }
                                    read_addr.do_send(ConnectionActorMessage::Disconnect(DisconnectReason::InternalError("handle initial connect error".to_string())));
                                }
                            }
                        }
                        _ => {
                            error!("client first packet is not connect packet");
                        }
                    }
                }
                    .into_actor(&actor)
            );

            actor.read_packet_handle = Some(handle);
            actor
        });
        addr
    }

    pub fn new(
        stream: T,
        max_message_size: u32,
        default_buffer_size: usize,
        peer_addr: SocketAddr,
        plugin_service: Arc<PluginManager>,
        client_certificate: Option<Vec<u8>>,
        metric: Arc<Metric>,
    ) -> (
        Self,
        tokio::io::ReadHalf<T>,
        mpsc::UnboundedReceiver<NetworkEvent>,
    ) {
        let (reader, writer) = tokio::io::split(stream);

        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let network_sender = NetworkSender::spawn(writer, event_tx, metric.clone());

        let actor = ConnectionActor {
            network_sender: Some(network_sender),
            encode_buffer: BytesMut::with_capacity(64 * 1024),
            pending_count: 0,
            batch_size: 100,
            client_certificate,
            _phantom: std::marker::PhantomData,
            event_listener_handle: None,
            max_message_size,
            disconnected_normally: false,
            protocol_version: ProtocolVersion::V3_1_1,
            buffer_size: default_buffer_size,
            peer_addr,
            plugin_service,
            session: None,
            read_packet_handle: None,
            metric,
        };

        (actor, reader, event_rx)
    }

    fn flush_batch(&mut self) {
        if self.pending_count == 0 {
            return;
        }

        if let Some(sender) = &self.network_sender {
            let data = self.encode_buffer.split().freeze();
            if let Err(e) = sender.try_send(data) {
                error!("Failed to send batch: {:?}", e);
            }
            self.pending_count = 0;
        }
    }

    fn handle_disconnection(&mut self, ctx: &mut Context<Self>, reason: String) {
        warn!("Connection disconnecting: {}", reason);

        if let Some(session) = &self.session {
            if self.disconnected_normally {
                session.do_send(session_actor::SessionActorMessage::ClientDisconnected);
            } else {
                session.do_send(session_actor::SessionActorMessage::UnexpectClientDisconnected);
            }
        }

        ctx.stop();
    }
}

#[allow(clippy::type_complexity)]
pub async fn read_packet<T: AsyncRead + Unpin>(
    reader: &mut tokio::io::ReadHalf<T>,
    buffer: &mut BytesMut,
    max_message_size: u32,
    protocol_version: Option<ProtocolVersion>,
    metric: Option<Arc<Metric>>,
) -> Result<(Packet, ProtocolVersion), ConnectionError> {
    loop {
        match try_parse_packet(buffer, max_message_size, protocol_version)? {
            Some((consumed_len, packet, protocol_version)) => {
                buffer.advance(consumed_len);
                return Ok((packet, protocol_version));
            }
            None => {
                let n = reader.read_buf(buffer).await?;
                if let Some(m) = &metric {
                    m.increase_bytes_received(n as u64);
                }

                if 0 == n {
                    return Err(ConnectionError::ConnectionClosed);
                }
            }
        }
    }
}

fn try_parse_packet(
    buffer: &BytesMut,
    max_message_size: u32,
    protocol_version: Option<ProtocolVersion>,
) -> Result<Option<(usize, Packet, ProtocolVersion)>, ConnectionError> {
    let protocol_version = match protocol_version {
        Some(protocol_version) => protocol_version,
        None => match detect_initial_connect_protocol(buffer, max_message_size)? {
            Some(protocol_version) => protocol_version,
            None => return Ok(None),
        },
    };

    match protocol_version {
        ProtocolVersion::V3_1_1 => try_parse_v3_packet(buffer, max_message_size),
        ProtocolVersion::V5_0 => try_parse_v5_packet(buffer, max_message_size),
    }
}

fn try_parse_v3_packet(
    buffer: &BytesMut,
    max_message_size: u32,
) -> Result<Option<(usize, Packet, ProtocolVersion)>, ConnectionError> {
    let packet_result: std::prelude::v1::Result<
        (&[u8], (&[u8], MqttPacketV3)),
        nom::Err<nom::error::Error<&[u8]>>,
    > = yedmq_mqtt::parse(buffer, max_message_size);

    match packet_result {
        Ok((_, (consumed_bytes, packet))) => Ok(Some((
            consumed_bytes.len(),
            Packet::from(packet),
            ProtocolVersion::V3_1_1,
        ))),
        Err(nom::Err::Incomplete(_)) => Ok(None),
        Err(nom::Err::Error(err_inner)) => {
            if err_inner.code == nom::error::ErrorKind::Verify {
                warn!("Message size exceeds maximum allowed size of the system.");
                Err(ConnectionError::MaxMessageSizeExceeded(
                    "Message size exceeds maximum allowed size of the system.".to_string(),
                ))
            } else {
                error!("read MQTT v3 packet error: {:?}", err_inner);
                Err(ConnectionError::PacketParseError(
                    "Invalid MQTT v3 packet".to_string(),
                ))
            }
        }
        Err(err) => {
            error!("read MQTT v3 packet error: {:?}", err);
            Err(ConnectionError::PacketParseError(
                "Invalid MQTT v3 packet".to_string(),
            ))
        }
    }
}

fn try_parse_v5_packet(
    buffer: &BytesMut,
    max_message_size: u32,
) -> Result<Option<(usize, Packet, ProtocolVersion)>, ConnectionError> {
    match yedmq_mqtt::v5::parse(buffer, max_message_size) {
        Ok((remaining, packet)) => Ok(Some((
            buffer.len() - remaining.len(),
            packet,
            ProtocolVersion::V5_0,
        ))),
        Err(yedmq_mqtt::v5::common::Mqtt5ParseError::Incomplete(_)) => Ok(None),
        Err(yedmq_mqtt::v5::common::Mqtt5ParseError::RemainingLengthExceeded {
            remaining_length,
            max_message_size,
        }) => {
            warn!("MQTT 5 message size exceeds maximum allowed size of the system.");
            Err(ConnectionError::MaxMessageSizeExceeded(format!(
                "MQTT 5 remaining length {remaining_length} exceeds max message size {max_message_size}"
            )))
        }
        Err(yedmq_mqtt::v5::common::Mqtt5ParseError::MalformedPacket(
            "enhanced authentication is not supported",
        )) => Err(ConnectionError::UnsupportedMqtt5Feature(
            "enhanced authentication",
        )),
        Err(err) => {
            error!("read MQTT v5 packet error: {}", err);
            Err(ConnectionError::Mqtt5ProtocolError {
                reason_code: ReasonCode::ProtocolError,
                message: format!("Invalid MQTT v5 packet: {err}"),
            })
        }
    }
}

fn detect_initial_connect_protocol(
    buffer: &BytesMut,
    max_message_size: u32,
) -> Result<Option<ProtocolVersion>, ConnectionError> {
    let (body, fixed_header) = match yedmq_mqtt::v5::fixed_header::parse(buffer, max_message_size) {
        Ok(parsed) => parsed,
        Err(yedmq_mqtt::v5::common::Mqtt5ParseError::Incomplete(_)) => return Ok(None),
        Err(yedmq_mqtt::v5::common::Mqtt5ParseError::RemainingLengthExceeded {
            remaining_length,
            max_message_size,
        }) => {
            return Err(ConnectionError::MaxMessageSizeExceeded(format!(
                    "MQTT remaining length {remaining_length} exceeds max message size {max_message_size}"
                )));
        }
        Err(err) => {
            return Err(ConnectionError::PacketParseError(format!(
                "Invalid first MQTT packet: {err}"
            )));
        }
    };

    if fixed_header.packet_type != yedmq_mqtt::v5::fixed_header::ControlPacketType::Connect {
        return Err(ConnectionError::PacketParseError(
            "client first packet is not CONNECT".to_string(),
        ));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if body.len() < remaining_length {
        return Ok(None);
    }
    let connect_body = &body[..remaining_length];
    let (input, protocol_name) = yedmq_mqtt::v5::common::parse_utf8_string(connect_body)
        .map_err(|err| ConnectionError::PacketParseError(err.to_string()))?;
    if protocol_name != "MQTT" {
        return Err(ConnectionError::PacketParseError(
            "invalid mqtt protocol name".to_string(),
        ));
    }
    let (_, protocol_level) = yedmq_mqtt::v5::common::parse_u8(input, "CONNECT protocol level")
        .map_err(|err| ConnectionError::PacketParseError(err.to_string()))?;

    match protocol_level {
        4 => Ok(Some(ProtocolVersion::V3_1_1)),
        5 => Ok(Some(ProtocolVersion::V5_0)),
        other => Err(ConnectionError::UnsupportedProtocolVersion {
            supported_versions: vec!["3.1.1".to_string(), "5.0".to_string()],
            current_version: other.to_string(),
        }),
    }
}

pub struct HandleInitialConnectResult {
    pub session_recipient: Recipient<SessionActorMessage>,
    pub session_present: bool,
}

fn success_connack_packet(
    protocol_version: ProtocolVersion,
    session_present: bool,
    max_message_size: u32,
) -> Packet {
    match protocol_version {
        ProtocolVersion::V3_1_1 => {
            let connack = ConnAckPacketBuilder::new()
                .set_return_code(ConnackReturnCode::Accept)
                .set_session_present(session_present)
                .build();
            Packet::from(MqttPacketV3::Connack(connack))
        }
        ProtocolVersion::V5_0 => Packet::Connack(NeutralConnack {
            protocol_version: ProtocolVersion::V5_0,
            session_present,
            reason_code: ReasonCode::Success,
            properties: Properties {
                maximum_packet_size: Some(max_message_size),
                topic_alias_maximum: Some(0),
                subscription_identifier_available: Some(false),
                shared_subscription_available: Some(false),
                ..Properties::default()
            },
        }),
    }
}

fn error_connack_packet(protocol_version: ProtocolVersion, error: &ConnectionError) -> Packet {
    match protocol_version {
        ProtocolVersion::V3_1_1 => {
            let return_code = match error {
                ConnectionError::UnsupportedProtocolVersion { .. } => {
                    ConnackReturnCode::UnsupportedProtocolVersion
                }
                ConnectionError::Unauthenticate(reason) => {
                    error!("connection unauthenticated: {}", reason);
                    ConnackReturnCode::UnAuthorized
                }
                ConnectionError::PluginError(e) => {
                    error!("handle initial connect error: {}", e);
                    ConnackReturnCode::ServerUnavailable
                }
                ConnectionError::SessionManagerServiceUnavailable(e) => {
                    log_initial_connect_error(&ConnectionError::SessionManagerServiceUnavailable(
                        e.clone(),
                    ));
                    ConnackReturnCode::ServerUnavailable
                }
                _ => {
                    log_initial_connect_error(error);
                    ConnackReturnCode::ServerUnavailable
                }
            };
            Packet::from(MqttPacketV3::Connack(
                ConnAckPacketBuilder::new()
                    .set_return_code(return_code)
                    .build(),
            ))
        }
        ProtocolVersion::V5_0 => {
            let reason_code = match error {
                ConnectionError::UnsupportedProtocolVersion { .. } => {
                    ReasonCode::UnsupportedProtocolVersion
                }
                ConnectionError::Unauthenticate(reason) => {
                    error!("connection unauthenticated: {}", reason);
                    ReasonCode::NotAuthorized
                }
                ConnectionError::UnsupportedMqtt5Feature(feature) => {
                    warn!("unsupported MQTT 5 feature during connect: {}", feature);
                    match *feature {
                        "enhanced authentication" => ReasonCode::BadAuthenticationMethod,
                        _ => ReasonCode::ImplementationSpecificError,
                    }
                }
                ConnectionError::PluginError(e) => {
                    error!("handle initial connect error: {}", e);
                    ReasonCode::ServerUnavailable
                }
                ConnectionError::SessionManagerServiceUnavailable(e) => {
                    log_initial_connect_error(&ConnectionError::SessionManagerServiceUnavailable(
                        e.clone(),
                    ));
                    ReasonCode::ServerUnavailable
                }
                _ => {
                    log_initial_connect_error(error);
                    ReasonCode::ServerUnavailable
                }
            };
            Packet::Connack(NeutralConnack {
                protocol_version: ProtocolVersion::V5_0,
                session_present: false,
                reason_code,
                properties: Properties::default(),
            })
        }
    }
}

fn server_disconnect_packet(reason_code: ReasonCode) -> Packet {
    Packet::Disconnect(yedmq_mqtt::packet::Disconnect {
        protocol_version: ProtocolVersion::V5_0,
        reason_code,
        session_expiry_interval: None,
        properties: Properties::default(),
    })
}

async fn handle_initial_connect<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    packet: Connect,
    plugin_service: Arc<PluginManager>,
    self_addr: &Addr<ConnectionActor<T>>,
    peer_addr: SocketAddr,
    client_certificate: Option<Vec<u8>>,
    metric: Arc<Metric>,
) -> Result<HandleInitialConnectResult, ConnectionError> {
    let session_options = session_options_for_session_manager(&packet);

    let authenticate_request = AuthenticateRequest {
        client_id: packet.client_id.clone(),
        username: packet.username.as_ref().unwrap_or(&"".to_string()).clone(),
        password: packet.password.as_ref().unwrap_or(&"".to_string()).clone(),
        client_ip: peer_addr.ip().to_string(),
        client_cert: client_certificate.unwrap_or_default(),
        protocol_version: protocol_version_label(packet.protocol_version).to_string(),
        properties: properties_to_struct(&packet.properties),
    };

    let plugin_authenticate_result = plugin_service
        .call_authenticate_hook(authenticate_request)
        .await;

    match plugin_authenticate_result {
        Ok(result) => match result {
            AuthenticateResult {
                authenticated: true,
                tenant_id,
                ..
            } => {
                let will_message = packet.will.as_ref().map(|will| WillMessage {
                    will_topic: will.topic.clone(),
                    will_message: will.payload.to_vec(),
                    will_qos: will.qos,
                    will_retain: will.retain,
                });
                let tenant_id = tenant_id.unwrap_or("public".to_string());
                let recipient = self_addr.clone().recipient();
                let session_manager_actor_addr =
                    crate::session::session_manager_actor::SessionManagerActor::from_registry();
                let recipient = session_manager_actor_addr
                    .send(CreateSessionMessage {
                        tenant_id,
                        client_id: packet.client_id.clone(),
                        clean_session: session_options.clean_session,
                        clean_start: session_options.clean_start,
                        protocol_version: session_options.protocol_version,
                        session_expiry_interval: session_options.session_expiry_interval,
                        connection_addr: recipient.clone(),
                        keep_alive: packet.keep_alive as u64,
                        will_message,
                        peer_addr,
                        username: packet.username.clone(),
                    })
                    .await
                    .map_err(|e| match e {
                        MailboxError::Closed => {
                            error!("session manager actor mailbox closed");
                            ConnectionError::SessionManagerServiceUnavailable(
                                "Session manager actor mailbox closed".to_string(),
                            )
                        }
                        MailboxError::Timeout => {
                            error!("session manager actor mailbox timeout");
                            ConnectionError::SessionManagerServiceUnavailable(
                                "Session manager actor mailbox timeout".to_string(),
                            )
                        }
                    })?
                    .map_err(|e| match e {
                        crate::session::session_manager_actor::SessionManagerError::NotInitialized(
                            dependency,
                        ) => {
                            error!("session manager not initialized: {}", dependency);
                            ConnectionError::SessionManagerServiceUnavailable(format!(
                                "session manager not initialized: {}",
                                dependency
                            ))
                        }
                        other => ConnectionError::SessionManagerServiceUnavailable(other.to_string()),
                    })
                    .map(|r| HandleInitialConnectResult {
                        session_recipient: r.session_actor_recipient,
                        session_present: r.session_present,
                    });

                metric.increase_clients_connected();
                recipient
            }
            AuthenticateResult {
                authenticated: false,
                ..
            } => Err(ConnectionError::Unauthenticate(
                "authenticate plugin rejected".to_string(),
            )),
        },
        Err(e) => Err(ConnectionError::PluginError(e)),
    }
}

fn protocol_version_label(protocol_version: ProtocolVersion) -> &'static str {
    match protocol_version {
        ProtocolVersion::V3_1_1 => "3.1.1",
        ProtocolVersion::V5_0 => "5.0",
    }
}

#[derive(Debug, Clone, Copy)]
struct SessionStartOptions {
    clean_session: bool,
    clean_start: bool,
    protocol_version: ProtocolVersion,
    session_expiry_interval: Option<u32>,
}

fn session_options_for_session_manager(packet: &Connect) -> SessionStartOptions {
    match packet.protocol_version {
        ProtocolVersion::V3_1_1 => SessionStartOptions {
            clean_session: packet.clean_start,
            clean_start: packet.clean_start,
            protocol_version: ProtocolVersion::V3_1_1,
            session_expiry_interval: None,
        },
        ProtocolVersion::V5_0 => {
            let session_expiry_interval = packet
                .session_expiry_interval
                .or(packet.properties.session_expiry_interval)
                .unwrap_or(0);
            SessionStartOptions {
                clean_session: packet.clean_start && session_expiry_interval == 0,
                clean_start: packet.clean_start,
                protocol_version: ProtocolVersion::V5_0,
                session_expiry_interval: Some(session_expiry_interval),
            }
        }
    }
}

impl<T> Actor for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("ConnectionActor started for peer {}", self.peer_addr);

        ctx.set_mailbox_capacity(0);

        ctx.run_interval(Duration::from_millis(1), |act, _| {
            act.flush_batch();
        });
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        info!("ConnectionActor stopping for peer {}", self.peer_addr);

        self.flush_batch();

        if let Some(sender) = self.network_sender.take() {
            actix::spawn(async move {
                sender.shutdown().await;
            });
        }

        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("ConnectionActor stopped for peer {}", self.peer_addr);

        if let Some(session) = &self.session {
            self.metric.decrease_clients_connected();
            if self.disconnected_normally {
                session.do_send(session_actor::SessionActorMessage::ClientDisconnected);
            } else {
                warn!(
                    "ConnectionActor stopped unexpectedly! Sending UnexpectDisconnect to session"
                );
                session.do_send(session_actor::SessionActorMessage::UnexpectClientDisconnected);
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

impl<T> Handler<UpdateProtocolVersion> for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: UpdateProtocolVersion, _ctx: &mut Self::Context) -> Self::Result {
        self.protocol_version = msg.protocol_version;
    }
}

impl<T> Drop for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn drop(&mut self) {
        debug!("🔥 ConnectionActor Dropped!");
    }
}

impl<T> Handler<ConnectionActorMessage> for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = Result<(), ConnectionError>;

    fn handle(&mut self, msg: ConnectionActorMessage, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            ConnectionActorMessage::WritePacketToClient(mut packet) => {
                if self.network_sender.is_none() {
                    return Err(ConnectionError::ConnectionClosed);
                }

                if !mqtt_message_expiry::prepare_packet_for_delivery(
                    &mut packet,
                    self.protocol_version,
                    mqtt_message_expiry::now_unix_secs(),
                ) {
                    return Ok(());
                }

                match self.protocol_version {
                    ProtocolVersion::V3_1_1 => {
                        let packet = MqttPacketV3::try_from(packet)
                            .map_err(|e| ConnectionError::PacketParseError(e.to_string()))?;
                        packet.encode(&mut self.encode_buffer);
                    }
                    ProtocolVersion::V5_0 => {
                        packet.set_protocol_version(ProtocolVersion::V5_0);
                        yedmq_mqtt::v5::encode(&packet, &mut self.encode_buffer)
                            .map_err(|e| ConnectionError::PacketParseError(e.to_string()))?;
                    }
                }
                //let bytes = packet.to_bytes();
                //self.encode_buffer.extend_from_slice(&bytes);
                self.pending_count += 1;

                // reach the limit, flush now
                if self.pending_count >= self.batch_size {
                    self.flush_batch();
                }

                Ok(())
            }
            ConnectionActorMessage::Disconnect(reason) => {
                if let DisconnectReason::Normal = reason {
                    self.disconnected_normally = true;
                }

                self.flush_batch();

                if let Some(sender) = self.network_sender.take() {
                    let fut = async move {
                        sender.shutdown().await;
                    };
                    ctx.spawn(fut.into_actor(self).map(|_, _, ctx| {
                        ctx.stop();
                    }));
                } else {
                    ctx.stop();
                }

                Ok(())
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

impl<T> Handler<NetworkEvent> for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, event: NetworkEvent, ctx: &mut Self::Context) {
        match event {
            NetworkEvent::WriteError(e) => {
                error!("Write error for peer {}: {}", self.peer_addr, e);
                self.handle_disconnection(ctx, format!("Write error: {}", e));
            }

            NetworkEvent::ReadError(e) => {
                error!("Read error for peer {}: {}", self.peer_addr, e);
                self.handle_disconnection(ctx, format!("Read error: {}", e));
            }

            NetworkEvent::ClientDisconnected => {
                info!("Client disconnected from peer {}", self.peer_addr);
                self.handle_disconnection(ctx, "Client closed connection".to_string());
            }
        }
    }
}
