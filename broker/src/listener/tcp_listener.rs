use std::sync::Arc;

use actix::Actor;
use anyhow::Result;
use tokio::net::TcpListener;

use crate::connection::ConnectionActor;

pub struct MqttTcpListener {
    pub app: Arc<crate::app::YedMQApp>,
}

impl MqttTcpListener {
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.app.settings.listener.tcp.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let peer_addr = stream.peer_addr().unwrap();
            let settings = self.app.settings.clone();
            ConnectionActor::new(
                stream,
                settings.mqtt.max_message_size,
                4096,
                peer_addr,
                self.app.plugin_manager.clone(),
                self.app.session_manager.get().unwrap().clone().recipient(),
            )
            .start();
        }
    }
}