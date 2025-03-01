use actix::prelude::*;
use actix::{Actor, Addr, Context};
use log::warn;
use tokio::io::{AsyncRead, AsyncWrite};
use yedmq_mqtt::MqttPacketV3;

use crate::connection::{Connection, ConnectionError};

use super::session_actor;

#[derive(Message)]
#[rtype(result = "()")]
pub enum ConnectionActorMessage {
    WritePacketToClient(MqttPacketV3),
    Disconnect,
}

pub struct ConnectionActor<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    session: Addr<session_actor::SessionActor<T>>,
    connection: Option<Connection<T>>,
    max_message_size: u32,
}

impl<T> Actor for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        if let Some(mut connection) = self.connection.take() {
            let max_message_size = self.max_message_size;
            let session = self.session.clone();
            let self_addr = ctx.address();
            async move {
                loop {
                    let read_pacekt_res = connection.read_packet_ex(max_message_size).await;
                    if let Ok(packet) = read_pacekt_res {
                            session
                                .do_send(session_actor::SessionActorMessage::ProcessPacket(packet));
                    } else {
                        let err = read_pacekt_res.err().unwrap();
                        match err {
                            ConnectionError::UnknownIOError(error) => {
                                warn!("unknown io error: {}", error);
                            }
                            _ => {
                                warn!("read packet error: {}", err);
                            }
                        }
                        let _ = self_addr.send(ConnectionActorMessage::Disconnect).await;
                        break;
                    }
                }
            }
            .into_actor(self)
            .spawn(ctx);
        }
    }
}

impl<T> Handler<ConnectionActorMessage> for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: ConnectionActorMessage, ctx: &mut Self::Context) -> Self::Result {
        if let Some(mut connection) = self.connection.take() {
            match msg {
                ConnectionActorMessage::WritePacketToClient(packet) => {
                    async move{
                        let _ = connection.write_packet(&packet).await;
                    }.into_actor(self).wait(ctx);
                }
                ConnectionActorMessage::Disconnect => {
                    async move{
                        let _ = connection.shutdown().await;
                    }.into_actor(self).wait(ctx);
                    ctx.stop();
                }
            }
        }
    }
}
