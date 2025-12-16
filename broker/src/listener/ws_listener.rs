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
    pub arbiter_pool: Arc<crate::arbiter_pool::ArbiterPool>,
}

impl MqttWsListener {
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.app.settings.listener.ws.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            match stream.peer_addr() {
                Ok(remote_addr) => {
                    let ws_stream = tokio_tungstenite::accept_hdr_async(stream, WsCallBack {}).await;

                    if let Ok(ws_stream) = ws_stream {
                        let websocket_tunnel = WebsocketTunnel {
                            inner: StreamReader::new(StreamWrapper { inner: ws_stream }),
                        };

                        let settings = self.app.settings.clone();
                        let plugin_manager_clone = self.app.plugin_manager.clone();
                        self.arbiter_pool.start_actor(move || {
                            ConnectionActor::new(
                                websocket_tunnel,
                                settings.mqtt.max_message_size,
                                4096,
                                remote_addr,
                                plugin_manager_clone,
                                None
                            )
                        });
                    } else {
                        warn!(
                            "Failed to accept WebSocket connection from {}, err {}",
                            remote_addr,
                            ws_stream.err().unwrap()
                        );
                    }
                }
                Err(_) => {
                    warn!("failed to get peer address, close the connection");
                }
            }

        }
    }
}
