use actix::prelude::*;
use actix::{Actor, Addr, Context};
use log::warn;
use tokio::io::{AsyncRead, AsyncWrite};
use yedmq_mqtt::MqttPacketV3;

use crate::connection::Connection;

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
    pub connection: Option<Connection<T>>,
    pub max_message_size: u32,
    pub disconnected_normally: bool,
}

impl <T> ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(
        session: Addr<session_actor::SessionActor<T>>,
        connection: Connection<T>,
        max_message_size: u32,
    ) -> ConnectionActor<T> {
        ConnectionActor {
            session,
            connection: Some(connection),
            max_message_size,
            disconnected_normally: false,
        }
    }
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
                    let packet = connection.read_packet_ex(max_message_size).await.unwrap();
                    if matches!(packet, MqttPacketV3::Disconnect(_)) {
                        self_addr.send(NotifyUpdateDisconnectedNormally{ disconnected_normally: true }).await.unwrap();
                    }
                    session
                        .do_send(session_actor::SessionActorMessage::InboundPacket(packet));
                }
            }
            .into_actor(self)
            .spawn(ctx);
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if !self.disconnected_normally {
            warn!("ConnectionActor stopped unexpectedly! Sending UnexpectDisconnect to session");
            self.session.do_send(session_actor::UnexpectClientDisconnected {});
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
        if let Some(mut connection) = self.connection.take() {
            match msg {
                ConnectionActorMessage::WritePacketToClient(packet) => {
                    async move{
                        connection.write_packet(&packet).await.unwrap();
                    }.into_actor(self).wait(ctx);
                }
                ConnectionActorMessage::Disconnect => {
                    async move{
                        let _ = connection.shutdown().await.unwrap();
                    }.into_actor(self).wait(ctx);
                    ctx.stop();
                }
            }
        } 
    }
}

impl<T> Handler<NotifyUpdateDisconnectedNormally> for ConnectionActor<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: NotifyUpdateDisconnectedNormally, _ctx: &mut Self::Context) -> Self::Result {
        self.disconnected_normally = msg.disconnected_normally;
    }
}