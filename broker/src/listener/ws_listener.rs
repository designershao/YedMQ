use std::sync::Arc;

use anyhow::Result;
use log::warn;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_util::io::StreamReader;

use crate::{
    metric::Metric, plugin_manager::PluginManager, router::RouterCmd,
    session::session_manager::SessionManager, settings::Settings, topic::TopicManager,
};

use super::{
    accept_connection,
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

            let peer_addr = stream.peer_addr().unwrap();
            let plugin_manager = self.app.plugin_manager.clone();
            let session_manager = self.app.session_manager.clone();
            let topic_manager = self.app.topic_manager.clone();
            let router_sender = self.app.router_sender.clone();
            let settings = self.app.settings.clone();

            let remote_addr = stream.peer_addr().unwrap();

            let ws_stream = tokio_tungstenite::accept_hdr_async(stream, WsCallBack {}).await;

            if let Ok(ws_stream) = ws_stream {
                let websocket_tunnel = WebsocketTunnel {
                    inner: StreamReader::new(StreamWrapper { inner: ws_stream }),
                };

                tokio::spawn(accept_connection(
                    websocket_tunnel,
                    plugin_manager,
                    session_manager,
                    topic_manager,
                    router_sender,
                    settings,
                    peer_addr,
                    self.app.metric.clone(),
                ));
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
