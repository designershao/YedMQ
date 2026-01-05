use std::sync::Arc;

use anyhow::Result;
use log::warn;
use tokio::net::TcpListener;

use crate::connection::ConnectionActor;

pub struct MqttTcpListener {
    pub app: Arc<crate::app::YedMQApp>,
    pub arbiter_pool: Arc<crate::arbiter_pool::ArbiterPool>,
}

impl MqttTcpListener {
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.app.settings.listener.tcp.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            match stream.peer_addr() {
                Ok(peer_addr) => {
                    let settings = self.app.settings.clone();
                    let plugin_manager = self.app.plugin_manager.clone();
                    let metric = self.app.metric.clone();
                    ConnectionActor::create_and_start(
                        stream,
                        settings.mqtt.max_message_size,
                        4096,
                        peer_addr,
                        plugin_manager,
                        None,
                        metric,
                    );
                }
                Err(_) => {
                    warn!("failed to get peer address, close the connection");
                }
            }
        }
    }
}
