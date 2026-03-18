use std::sync::Arc;

use anyhow::Result;
use log::warn;
use tokio::net::TcpListener;
use tokio_util::io::StreamReader;

use crate::connection::{ConnectionActor, ConnectionActorStartConfig};

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
                    match tokio_tungstenite::accept_hdr_async(stream, WsCallBack {}).await {
                        Ok(ws_stream) => {
                            let websocket_tunnel = WebsocketTunnel {
                                inner: StreamReader::new(StreamWrapper { inner: ws_stream }),
                            };

                            let settings = self.app.settings.clone();
                            let plugin_manager_clone = self.app.plugin_manager.clone();
                            let metric = self.app.metric.clone();
                            ConnectionActor::create_and_start(
                                websocket_tunnel,
                                ConnectionActorStartConfig {
                                    max_message_size: settings.mqtt.max_message_size,
                                    default_buffer_size: 4096,
                                    peer_addr: remote_addr,
                                    plugin_service: plugin_manager_clone,
                                    client_certificate: None,
                                    metric,
                                    rate_limit: settings.listener.ws.rate_limit.clone(),
                                },
                            );
                        }
                        Err(err) => {
                            warn!(
                                "Failed to accept WebSocket connection from {}, err {}",
                                remote_addr, err
                            );
                        }
                    }
                }
                Err(_) => {
                    warn!("failed to get peer address, close the connection");
                }
            }
        }
    }
}
