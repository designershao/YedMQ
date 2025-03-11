use std::sync::Arc;

use actix::Actor;
use anyhow::Result;
use log::warn;
use tokio::net::TcpListener;
use tokio_util::io::StreamReader;


use crate::connection::ConnectionActor;

use super::{
    websocket_tunnel::{StreamWrapper, WebsocketTunnel},
    WsCallBack,
};
pub struct MqttWsListener {
    pub app: Arc<crate::app::YedMQApp>,
}

impl MqttWsListener {
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.app.settings.listener.ws.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            let remote_addr = stream.peer_addr().unwrap();

            let ws_stream = tokio_tungstenite::accept_hdr_async(stream, WsCallBack {}).await;

            if let Ok(ws_stream) = ws_stream {
                let websocket_tunnel = WebsocketTunnel {
                    inner: StreamReader::new(StreamWrapper { inner: ws_stream }),
                };

                let settings = self.app.settings.clone();
                ConnectionActor::new(
                    websocket_tunnel,
                    settings.mqtt.max_message_size,
                    4096,
                    remote_addr,
                    self.app.plugin_manager.clone(),
                    self.app.session_manager.get().unwrap().clone().recipient(),
                )
                .start();
            } else {
                warn!(
                    "Failed to accept WebSocket connection from {}, err {}",
                    remote_addr,
                    ws_stream.err().unwrap()
                );
            }
        }
    }
}
