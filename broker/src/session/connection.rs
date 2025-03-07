use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;

use actix::prelude::*;
use actix::{Actor, Addr, Context};
use bytes::{Buf, BytesMut};
use log::{error, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use yedmq_mqtt::v3::connack::{self, ConnAckPacketBuilder};
use yedmq_mqtt::v3::connect::ConnectPacket;
use yedmq_mqtt::MqttPacketV3;

use crate::connection::ConnectionError;
use crate::plugin_manager;

use super::session_actor::{SessionActor, SessionActorMessage};
use super::session_manager_actor::CreateSessionMessage;
use super::{session_actor, WillMessage};

#[derive(Message)]
#[rtype(result = "()")]
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
    pub session_manager_recipient: Recipient<CreateSessionMessage>,
    peer_addr: SocketAddr,
    pub reader: Rc<RefCell<tokio::io::ReadHalf<T>>>,
    pub writer: Rc<RefCell<tokio::io::WriteHalf<T>>>,
    pub plugin_service: Arc<dyn plugin_manager::PluginService + 'static>,
    pub max_message_size: u32,
    pub disconnected_normally: bool,
    pub buffer_size: usize,
    session: Option<Recipient<SessionActorMessage>>,
    read_packet_handle: Option<SpawnHandle>,
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
        plugin_service: Arc<dyn plugin_manager::PluginService + 'static>,
        session_manager: Recipient<CreateSessionMessage>,
    ) -> ConnectionActor<T> {
        let (reader, writer) = tokio::io::split(stream);
        ConnectionActor {
            reader: Rc::new(RefCell::new(reader)),
            writer: Rc::new(RefCell::new(writer)),
            max_message_size,
            disconnected_normally: false,
            buffer_size: default_buffer_size,
            peer_addr,
            plugin_service,
            session_manager_recipient: session_manager,
            session: None,
            read_packet_handle: None,
        }
    }
}

pub async fn write_packet<T: AsyncWrite>(
    writer: Rc<RefCell<tokio::io::WriteHalf<T>>>,
    packet: &MqttPacketV3,
) -> anyhow::Result<()> {
    writer.borrow_mut().write(&packet.to_bytes()).await?;
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

async fn handle_initial_connect<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    packet: ConnectPacket,
    session_manager_recipient: Recipient<CreateSessionMessage>,
    plugin_service: Arc<dyn plugin_manager::PluginService + 'static>,
    self_addr: &Addr<ConnectionActor<T>>,
    peer_addr: SocketAddr,
) -> anyhow::Result<Recipient<SessionActorMessage>> {
    // invalid mqtt protocol name
    if packet.variable_header.protocol_name != "MQTT" {
        warn!("invalid mqtt protocol name");
        return Err(anyhow::anyhow!("invalid mqtt protocol name"));
    }
    //

    // unsupport protocol version
    if packet.variable_header.protocol_level != 4 {
        let connack_packet = ConnAckPacketBuilder::new()
            .set_return_code(yedmq_mqtt::v3::connack::ConnackReturnCode::UnsupportedProtocolVersion)
            .build();
        self_addr
            .send(ConnectionActorMessage::WritePacketToClient(
                yedmq_mqtt::MqttPacketV3::Connack(connack_packet),
            ))
            .await
            .unwrap();
        return Err(anyhow::anyhow!("unsupport protocol version"));
    }
    //

    let plugin_authentication_result = plugin_service.do_connect_authenticate(&packet);

    match plugin_authentication_result {
        Ok(result) => match result {
            yedmq_plugin::plugin::AuthenticationResultValue::Success(tenant_id) => {
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
                let recipient = self_addr.clone().recipient();
                let result = session_manager_recipient
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
                    .await;
                if let Ok(session) = result.unwrap() {
                    let connack_packet = ConnAckPacketBuilder::new()
                        .set_return_code(yedmq_mqtt::v3::connack::ConnackReturnCode::Accpet)
                        .build();
                    self_addr
                        .send(ConnectionActorMessage::WritePacketToClient(
                            yedmq_mqtt::MqttPacketV3::Connack(connack_packet),
                        ))
                        .await
                        .unwrap();
                    Ok(session)
                } else {
                    let connack_packet = ConnAckPacketBuilder::new()
                        .set_return_code(
                            yedmq_mqtt::v3::connack::ConnackReturnCode::ServerUnavailable,
                        )
                        .build();
                    self_addr
                        .send(ConnectionActorMessage::WritePacketToClient(
                            yedmq_mqtt::MqttPacketV3::Connack(connack_packet),
                        ))
                        .await
                        .unwrap();
                    Err(anyhow::anyhow!("create session error"))
                }
            }
            yedmq_plugin::plugin::AuthenticationResultValue::Fail(reason) => {
                let connack_reason = match reason {
                        yedmq_plugin::plugin::ConnectReturnCode::ConnectAccepted => yedmq_mqtt::v3::connack::ConnackReturnCode::Accpet,
                        yedmq_plugin::plugin::ConnectReturnCode::ConnectionForbidenUnSupportMqttVersion => yedmq_mqtt::v3::connack::ConnackReturnCode::UnsupportedProtocolVersion,
                        yedmq_plugin::plugin::ConnectReturnCode::ConnectionForbidenInvalidClientIdentifier => yedmq_mqtt::v3::connack::ConnackReturnCode::InvalidClientIdentifier,
                        yedmq_plugin::plugin::ConnectReturnCode::ConnectionForbidenServerUnavailable => yedmq_mqtt::v3::connack::ConnackReturnCode::ServerUnavailable,
                        yedmq_plugin::plugin::ConnectReturnCode::ConnectionForbidenUnsupportUsernameOrPasswordFormat => yedmq_mqtt::v3::connack::ConnackReturnCode::InvalidUsernameOrPassword,
                        yedmq_plugin::plugin::ConnectReturnCode::ConnectionForbidenUnauth => yedmq_mqtt::v3::connack::ConnackReturnCode::UnAuthorized,
                    };
                let connack_packet = ConnAckPacketBuilder::new()
                    .set_return_code(connack_reason)
                    .build();
                self_addr
                    .send(ConnectionActorMessage::WritePacketToClient(
                        yedmq_mqtt::MqttPacketV3::Connack(connack_packet),
                    ))
                    .await
                    .unwrap();
                Err(anyhow::anyhow!("connect error"))
            }
        },
        Err(_) => {
            let connack_packet = ConnAckPacketBuilder::new()
                .set_return_code(yedmq_mqtt::v3::connack::ConnackReturnCode::ServerUnavailable)
                .build();
            self_addr
                .send(ConnectionActorMessage::WritePacketToClient(
                    yedmq_mqtt::MqttPacketV3::Connack(connack_packet),
                ))
                .await
                .unwrap();
            Err(anyhow::anyhow!("connect error"))
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
        let session_manager = self.session_manager_recipient.clone();
        let peer_addr = self.peer_addr.clone();
        let handle = ctx.spawn(
            async move {
                let first_packet = read_packet(&reader, &mut buffer, max_message_size).await;
                if let Ok(packet) = first_packet {
                    match packet {
                        MqttPacketV3::Connect(packet) => {
                            let session_res = handle_initial_connect(
                                packet,
                                session_manager,
                                plugin_service,
                                &self_addr,
                                peer_addr,
                            )
                            .await;
                            if let Ok(session) = session_res {
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
                                        session.do_send(
                                            session_actor::SessionActorMessage::InboundPacket(
                                                packet,
                                            ),
                                        );
                                    } else {
                                        break;
                                    }
                                }
                            } else {
                                error!("create session error");
                                return;
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
                    self_addr
                        .send(ConnectionActorMessage::WritePacketToClient(
                            yedmq_mqtt::MqttPacketV3::Connack(connack_packet),
                        ))
                        .await
                        .unwrap();
                    return;
                }
                self_addr.do_send(ConnectionActorMessage::Disconnect);
            }
            .into_actor(self),
        );
        self.read_packet_handle = Some(handle);
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
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
        println!("🔥 ConnectionActor Dropped!");
    }
}

impl<T> Handler<ConnectionActorMessage> for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: ConnectionActorMessage, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            ConnectionActorMessage::WritePacketToClient(packet) => {
                let writer = self.writer.clone();
                ctx.spawn(
                    async move {
                        write_packet(writer, &packet).await.unwrap();
                    }
                    .into_actor(self),
                );
            }
            ConnectionActorMessage::Disconnect => {
                let writer = self.writer.clone();
                async move {
                    let _ = writer.borrow_mut().shutdown().await.unwrap();
                }
                .into_actor(self)
                .wait(ctx);
                self.read_packet_handle.and_then(|handle| {
                    ctx.cancel_future(handle);
                    Some(())
                });
                ctx.stop();
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use actix::{Actor, Context, Handler, Recipient};
    use anyhow::Ok;
    use nom::AsBytes;
    use yedmq_mqtt::{
        v3::{
            connack::{ConnAckPacketBuilder, ConnackReturnCode},
            connect::ConnectPacketBuilder,
        },
        MqttPacketV3,
    };
    use yedmq_plugin::plugin::ConnectReturnCode;

    use crate::{
        plugin_manager::{
            MockPluginService, PluginService, SubscribeAuthorizationResult, SubscribeReturnCode,
        },
        session::{session_actor, session_manager_actor},
    };

    use super::ConnectionActor;

    struct MockSessionManager {}

    impl Actor for MockSessionManager {
        type Context = Context<Self>;
    }

    impl Handler<session_manager_actor::CreateSessionMessage> for MockSessionManager {
        type Result = Result<
            Recipient<session_actor::SessionActorMessage>,
            session_manager_actor::SessionManagerError,
        >;

        fn handle(
            &mut self,
            _msg: session_manager_actor::CreateSessionMessage,
            _ctx: &mut Self::Context,
        ) -> Self::Result {
            let mock_session = MockSession {}.start();
            std::result::Result::Ok(mock_session.recipient())
        }
    }

    struct MockSession {}

    impl Actor for MockSession {
        type Context = Context<Self>;
    }

    impl Handler<session_actor::SessionActorMessage> for MockSession {
        type Result = ();

        fn handle(
            &mut self,
            _msg: session_actor::SessionActorMessage,
            _ctx: &mut Self::Context,
        ) -> Self::Result {
            ()
        }
    }

    #[actix::test]
    async fn when_connect_success_should_send_connack() {
        let session_manager_actor = MockSessionManager {}.start();
        let connect_packet = ConnectPacketBuilder::new("test".to_string())
            .clean_session(true)
            .build();
        let conack_packet = ConnAckPacketBuilder::new().build();

        let mock_io = tokio_test::io::Builder::new()
            .read(MqttPacketV3::Connect(connect_packet).to_bytes().as_bytes())
            .write(MqttPacketV3::Connack(conack_packet).to_bytes().as_bytes())
            .build();

        let mut mock_plugin_service = MockPluginService::new();
        mock_plugin_service
            .expect_do_connect_authenticate()
            .returning(|_| {
                Ok(yedmq_plugin::plugin::AuthenticationResultValue::Success("tenant_a".into()))
            });
        let connection_actor = ConnectionActor::new(
            mock_io,
            256,
            4096,
            "127.0.0.1:8080".parse().unwrap(),
            Arc::new(mock_plugin_service),
            session_manager_actor.recipient(),
        );
        connection_actor.start();

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    #[actix::test]
    async fn when_authenticate_failed_should_send_correct_connack() {
        let session_manager_actor = MockSessionManager {}.start();
        let connect_packet = ConnectPacketBuilder::new("test".to_string())
            .clean_session(true)
            .build();
        let conack_packet = ConnAckPacketBuilder::new()
            .set_return_code(ConnackReturnCode::UnAuthorized)
            .build();

        let mock_io = tokio_test::io::Builder::new()
            .read(MqttPacketV3::Connect(connect_packet).to_bytes().as_bytes())
            .write(MqttPacketV3::Connack(conack_packet).to_bytes().as_bytes())
            .build();

        let mut mock_plugin_service = MockPluginService::new();
        mock_plugin_service
            .expect_do_connect_authenticate()
            .returning(|_| {
                Ok(yedmq_plugin::plugin::AuthenticationResultValue::Fail(
                    ConnectReturnCode::ConnectionForbidenUnauth,
                ))
            });

        let connection_actor = ConnectionActor::new(
            mock_io,
            256,
            4096,
            "127.0.0.1:8080".parse().unwrap(),
            Arc::new(mock_plugin_service),
            session_manager_actor.recipient(),
        );
        connection_actor.start();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
