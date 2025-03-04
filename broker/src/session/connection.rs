use std::sync::Arc;

use actix::prelude::*;
use actix::{Actor, Addr, Context};
use bytes::{Buf, BytesMut};
use futures::lock::Mutex;
use log::{error, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use yedmq_mqtt::MqttPacketV3;

use crate::connection::ConnectionError;

use super::session_actor;

#[derive(Message)]
#[rtype(result = "()")]
pub enum ConnectionActorMessage {
    WritePacketToClient(MqttPacketV3),
    Disconnect,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct NotifyUpdateDisconnectedNormally {
    pub disconnected_normally: bool,
}

pub struct ConnectionActor<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    pub session: Addr<session_actor::SessionActor<T>>,
    pub reader: Arc<Mutex<tokio::io::ReadHalf<T>>>,
    pub writer: Arc<Mutex<tokio::io::WriteHalf<T>>>,
    pub max_message_size: u32,
    pub disconnected_normally: bool,

    pub buffer_size: usize,
}

impl<T> ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(
        session: Addr<session_actor::SessionActor<T>>,
        stream: T,
        max_message_size: u32,
        default_buffer_size: usize,
    ) -> ConnectionActor<T> {
        let (reader, writer) = tokio::io::split(stream);
        ConnectionActor {
            session,
            reader: Arc::new(Mutex::new(reader)),
            writer: Arc::new(Mutex::new(writer)),
            max_message_size,
            disconnected_normally: false,
            buffer_size: default_buffer_size,
        }
    }
}


pub async fn write_packet<T: AsyncWrite>(writer:Arc<Mutex<tokio::io::WriteHalf<T>>>, packet: &MqttPacketV3) -> anyhow::Result<()>{
    let mut writer = writer.lock().await;
    writer.write(&packet.to_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_packet<T: AsyncRead + Unpin>
(
    reader: Arc<Mutex<tokio::io::ReadHalf<T>>>,
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

impl<T> Actor for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let max_message_size = self.max_message_size;
        let session = self.session.clone();
        let self_addr = ctx.address();
        let reader = self.reader.clone();
        let mut buffer = BytesMut::with_capacity(self.buffer_size);
        async move {
            loop {
                let packet = read_packet(reader.clone(), &mut buffer, max_message_size).await;
                if let Ok(packet) = packet  {
                    if matches!(packet, MqttPacketV3::Disconnect(_)) {
                        self_addr.send(NotifyUpdateDisconnectedNormally{ disconnected_normally: true }).await.unwrap();
                    }
                    session
                        .do_send(session_actor::SessionActorMessage::InboundPacket(packet));
                } else {
                    break;
                }
            }
        }        
        .into_actor(self).map(|_, _, ctx| {
            ctx.stop();
        })
        .spawn(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if !self.disconnected_normally {
            warn!("ConnectionActor stopped unexpectedly! Sending UnexpectDisconnect to session");
            self.session
                .do_send(session_actor::UnexpectClientDisconnected {});
        } else {
            self.session.do_send(session_actor::ClientDisconnected {});
        }
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

    use crate::{
        connection::{self, Connection},
        session::session_actor::SessionActor,
    };

    use super::{ConnectionActor, ConnectionActorMessage};

    #[actix::test]
    async fn test_connection_actor() {
        let mocker_session_addr =
            SessionActor::mock(Box::new(|_msg, _ctx| Box::new(Some(())))).start();

        let publish_packet = PublishPacketBuilder::new("a/b/c".to_string(), vec![0x01]).build();

        let packet = MqttPacketV3::Publish(publish_packet);

        let mock_io = tokio_test::io::Builder::new()
            .read(packet.clone().to_bytes().as_bytes())
            .write(packet.to_bytes().as_bytes())
            .build();

        let connection_actor = ConnectionActor::new(mocker_session_addr, mock_io, 1000, 4096).start();

        connection_actor
            .send(ConnectionActorMessage::WritePacketToClient(packet))
            .await
            .unwrap();
    }
}
