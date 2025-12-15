use std::sync::Arc;

use anyhow::Result;
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
            let peer_addr = stream.peer_addr().unwrap();
            let settings = self.app.settings.clone();
            let plugin_manager = self.app.plugin_manager.clone();
            self.arbiter_pool.start_actor(move || {
                ConnectionActor::new(
                    stream,
                    settings.mqtt.max_message_size,
                    4096,
                    peer_addr,
                    plugin_manager,
                    None
                )
            });
        }
    }
}