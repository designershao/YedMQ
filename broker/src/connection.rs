use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use actix::prelude::*;
use actix::{Actor, Addr, Context};
use bytes::{Buf, Bytes, BytesMut};
use governor::clock::{Clock, DefaultClock};
use governor::{Quota, RateLimiter};
use log::{debug, error, info, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use yedmq_mqtt::v3::connack::{ConnAckPacketBuilder, ConnackReturnCode};
use yedmq_mqtt::v3::connect::ConnectPacket;
use yedmq_mqtt::MqttPacketV3;
use yedmq_plugin_host::plugin_manager::{AuthenticateResult, PluginManager};
use yedmq_plugin_host::protocol::plugin_protocol::AuthenticateRequest;

use crate::metric::Metric;
use crate::session::session_actor::SessionActorMessage;
use crate::session::session_manager_actor::CreateSessionMessage;
use crate::session::{session_actor, WillMessage};

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

    #[error("plugin error {0}")]
    PluginError(#[from] yedmq_plugin_host::plugin_manager::PluginManagerError),
}

#[derive(Debug)]
pub enum DisconnectReason {
    Normal,
    KeepAliveExpired,
    InternalError(String),
}

#[derive(Message, Debug)]
#[rtype(result = "Result<(), ConnectionError>")]
pub enum ConnectionActorMessage {
    WritePacketToClient(MqttPacketV3),
    Disconnect(DisconnectReason),
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
    session: Option<Recipient<SessionActorMessage>>,

    read_packet_handle: Option<SpawnHandle>,
    event_listener_handle: Option<SpawnHandle>,

    pub metric: Arc<Metric>,

    _phantom: std::marker::PhantomData<T>,
}

impl<T> ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn create_and_start(
        stream: T,
        max_message_size: u32,
        default_buffer_size: usize,
        peer_addr: SocketAddr,
        plugin_service: Arc<PluginManager>,
        client_certificate: Option<Vec<u8>>,
        metric: Arc<Metric>,
    ) -> Addr<Self> {
        let addr = ConnectionActor::create(move |ctx| {
            let (mut actor, mut reader, mut event_rx) = Self::new(
                stream,
                max_message_size,
                default_buffer_size,
                peer_addr,
                plugin_service.clone(),
                client_certificate.clone(),
                metric.clone(),
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
            let plugin_svc = plugin_service.clone();
            let peer = peer_addr;
            let cert = client_certificate.clone();
            let metric_clone = metric.clone();

            let handle = ctx.spawn(
                async move {
                    let mut buffer = BytesMut::with_capacity(buf_size);

                    let first_packet = match read_packet(&mut reader, &mut buffer, max_msg_size, Some(metric_clone.clone())).await {
                        Ok(packet) => packet,
                        Err(e) => {
                            error!("Failed to read first packet: {}", e);
                            read_addr.do_send(NetworkEvent::ReadError(
                                std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                            ));
                            return;
                        }
                    };

                    match first_packet {
                        MqttPacketV3::Connect(packet) => {
                            match handle_initial_connect(
                                packet,
                                plugin_svc,
                                &read_addr,
                                peer,
                                cert,
                                metric_clone.clone(),
                            ).await {
                                Ok(result) => {
                                    let connack = ConnAckPacketBuilder::new()
                                        .set_return_code(ConnackReturnCode::Accept)
                                        .set_session_present(result.session_present)
                                        .build();

                                    if let Err(e) = read_addr
                                        .send(ConnectionActorMessage::WritePacketToClient(
                                            yedmq_mqtt::MqttPacketV3::Connack(connack),
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
                                    let rate_limiter = RateLimiter::direct(
                                        Quota::per_second(NonZeroU32::new(1000).unwrap())
                                            .allow_burst(NonZeroU32::new(100).unwrap())
                                    );

                                    loop {
                                        match read_packet(&mut reader, &mut buffer, max_msg_size, Some(metric_clone.clone())).await {
                                            Ok(packet) => {
                                                metric_clone.increase_packets_received();
                                                if matches!(packet, MqttPacketV3::Disconnect(_)) && read_addr
                                                    .send(NotifyUpdateDisconnectedNormally {
                                                        disconnected_normally: true,
                                                    })
                                                    .await
                                                    .is_err() {
                                                    error!("Failed to send NotifyUpdateDisconnectedNormally message to self. The actor is likely shutting down.");
                                                }

                                                match rate_limiter.check() {
                                                    Ok(_) => {
                                                        result.session_recipient.do_send(
                                                            session_actor::SessionActorMessage::InboundPacket(
                                                                packet,
                                                            ),
                                                        );
                                                    }
                                                    Err(not_ready) => {
                                                        let wait_time = not_ready.wait_time_from(DefaultClock::default().now());
                                                        tokio::time::sleep(wait_time).await;
                                                        info!("rate limited")
                                                    }
                                                }
                                            }
                                            Err(ConnectionError::ConnectionClosed) => {
                                                info!("Client closed connection");
                                                read_addr.do_send(NetworkEvent::ClientDisconnected);
                                                break;
                                            }
                                            Err(e) => {
                                                error!("Read packet error: {}", e);
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
                                    if let Err(e) = read_addr
                                        .send(ConnectionActorMessage::WritePacketToClient(
                                            yedmq_mqtt::MqttPacketV3::Connack(connack_packet),
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

pub async fn read_packet<T: AsyncRead + Unpin>(
    reader: &mut tokio::io::ReadHalf<T>,
    buffer: &mut BytesMut,
    max_message_size: u32,
    metric: Option<Arc<Metric>>,
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
                    let n = reader.read_buf(buffer).await?;
                    if let Some(m) = &metric {
                        m.increase_bytes_received(n as u64);
                    }

                    if 0 == n {
                        return Err(ConnectionError::ConnectionClosed);
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
    metric: Arc<Metric>,
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
        username: packet
            .payload
            .username
            .as_ref()
            .unwrap_or(&"".to_string())
            .clone(),
        password: packet
            .payload
            .password
            .as_ref()
            .unwrap_or(&"".to_string())
            .clone(),
        client_ip: peer_addr.ip().to_string(),
        client_cert: client_certificate.unwrap_or_default(),
        protocol_version: "3.1.1".to_string(),
        properties: None,
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
                let session_manager_actor_addr =
                    crate::session::session_manager_actor::SessionManagerActor::from_registry();
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
                    .map_err(|e| ConnectionError::SessionManagerServiceUnavailable(e.to_string()))
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
            ConnectionActorMessage::WritePacketToClient(packet) => {
                if self.network_sender.is_none() {
                    return Err(ConnectionError::ConnectionClosed);
                }

                // write to buffer
                packet.encode(&mut self.encode_buffer);
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
                self.disconnected_normally = true;
                self.handle_disconnection(ctx, "Client closed connection".to_string());
            }
        }
    }
}
