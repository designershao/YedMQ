use std::net::SocketAddr;
use std::sync::Arc;

use actix::prelude::*;
use actix::{Actor, Addr, Context};
use bytes::{Buf, BytesMut};
use futures::lock::Mutex;
use log::{error, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use yedmq_mqtt::v3::connack::ConnAckPacketBuilder;
use yedmq_mqtt::v3::connect::ConnectPacket;
use yedmq_mqtt::MqttPacketV3;

use crate::connection::ConnectionError;
use crate::plugin_manager;

use super::session_actor::SessionActor;
use super::session_manager_actor::{CreateSessionMessage, SessionManagerActor};
use super::{session_actor, WillMessage};

#[derive(Message)]
#[rtype(result = "()")]
pub enum ConnectionActorMessage {
    WritePacketToClient(MqttPacketV3),
    Disconnect,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct UpdateSession<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub session: Addr<SessionActor<T>>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct NotifyUpdateDisconnectedNormally {
    pub disconnected_normally: bool,
}

pub struct ConnectionActor<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    pub session_manager: Addr<SessionManagerActor<T>>,
    peer_addr: SocketAddr,
    pub reader: Arc<Mutex<tokio::io::ReadHalf<T>>>,
    pub writer: Arc<Mutex<tokio::io::WriteHalf<T>>>,
    pub plugin_service: Arc<dyn plugin_manager::PluginService + 'static>,
    pub max_message_size: u32,
    pub disconnected_normally: bool,
    pub buffer_size: usize,
    session: Option<Addr<SessionActor<T>>>,
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
        session_manager: Addr<SessionManagerActor<T>>,
    ) -> ConnectionActor<T> {
        let (reader, writer) = tokio::io::split(stream);
        ConnectionActor {
            reader: Arc::new(Mutex::new(reader)),
            writer: Arc::new(Mutex::new(writer)),
            max_message_size,
            disconnected_normally: false,
            buffer_size: default_buffer_size,
            peer_addr,
            plugin_service,
            session_manager,
            session: None,
        }
    }
}

pub async fn write_packet<T: AsyncWrite>(
    writer: Arc<Mutex<tokio::io::WriteHalf<T>>>,
    packet: &MqttPacketV3,
) -> anyhow::Result<()> {
    let mut writer = writer.lock().await;
    writer.write(&packet.to_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_packet<T: AsyncRead + Unpin>(
    reader: &Arc<Mutex<tokio::io::ReadHalf<T>>>,
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
                    let n = reader.lock().await.read_buf(buffer).await?;

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
    session_manager: Addr<SessionManagerActor<T>>,
    plugin_service: Arc<dyn plugin_manager::PluginService + 'static>,
    self_addr: &Addr<ConnectionActor<T>>,
    peer_addr: SocketAddr,
) -> anyhow::Result<Addr<SessionActor<T>>> {
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
                let result = session_manager
                    .send(CreateSessionMessage {
                        tenant_id,
                        client_id: packet.payload.client_identifier.clone(),
                        clean_session: packet.variable_header.clean_session,
                        connection_addr: self_addr.clone(),
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
        Err(_) => todo!(),
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
        let session_manager = self.session_manager.clone();
        let peer_addr = self.peer_addr.clone();
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
                                        session_actor::SessionActorMessage::InboundPacket(packet),
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
                return;
            }
        }
        .into_actor(self)
        .map(|_, _, ctx| {
            ctx.stop();
        })
        .spawn(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if !self.disconnected_normally {
            warn!("ConnectionActor stopped unexpectedly! Sending UnexpectDisconnect to session");
            self.session
                .clone()
                .unwrap()
                .do_send(session_actor::UnexpectClientDisconnected {});
        } else {
            self.session
                .clone()
                .unwrap()
                .do_send(session_actor::ClientDisconnected {});
        }
    }
}

impl<T> Handler<UpdateSession<T>> for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: UpdateSession<T>, _ctx: &mut Self::Context) -> Self::Result {
        self.session = Some(msg.session);
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
                async move {
                    write_packet(writer, &packet).await.unwrap();
                }
                .into_actor(self)
                .wait(ctx);
            }
            ConnectionActorMessage::Disconnect => {
                let writer = self.writer.clone();
                async move {
                    let _ = writer.lock().await.shutdown().await.unwrap();
                }
                .into_actor(self)
                .wait(ctx);
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
    use actix::Actor;
    use nom::AsBytes;
    use yedmq_mqtt::{v3::publish::PublishPacketBuilder, MqttPacketV3};

    use crate::session::session_actor::SessionActor;

    use super::{ConnectionActor, ConnectionActorMessage};

    #[actix::test]
    async fn when_received_write_packet_to_client_message_should_write_packet_to_io_stream() {
        let mocker_session_addr =
            SessionActor::mock(Box::new(|_msg, _ctx| Box::new(Some(())))).start();

        let publish_packet = PublishPacketBuilder::new("a/b/c".to_string(), vec![0x01]).build();

        let packet = MqttPacketV3::Publish(publish_packet);

        let mock_io = tokio_test::io::Builder::new()
            .read(packet.clone().to_bytes().as_bytes())
            .write(packet.to_bytes().as_bytes())
            .build();

        let connection_actor =
            ConnectionActor::new(mocker_session_addr, mock_io, 1000, 4096).start();

        connection_actor
            .send(ConnectionActorMessage::WritePacketToClient(packet))
            .await
            .unwrap();
    }
}
