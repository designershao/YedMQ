use std::sync::Arc;

use log::warn;
use tokio::net::TcpListener;
use anyhow::Result;

use crate::{connection::Connection, plugin::plugin_manager::PluginManager, session::SessionManager, router::RouterCmd, topic::TopicManager};

pub struct MqttTcpListener {
    external_addr: String,
    plugin_manager: Arc<PluginManager>,
    session_manager: Arc<SessionManager>,
    topic_manager: Arc<TopicManager>,
    router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
}

impl MqttTcpListener {

    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.external_addr.clone()).await?;
        loop {
            let (mut stream, _) = listener.accept().await?;

            let plugin_manager = self.plugin_manager.clone();
            let session_manager = self.session_manager.clone();

            tokio::spawn(async move {
                let mut connection = Connection::new(stream);
                // wait the first connect packet, if the packet is not correct, the connection will be closed.
                let first_packet = connection.read_packet().await.unwrap();
                
                match first_packet {
                    crate::protocol::MqttPacketV3::Connect(packet) => {
                        let r = plugin_manager.clone().call_hook_on_connect_auth(&"".to_string(), &packet).await;
                        match r {
                            Ok(auth_result) => {
                                match auth_result {
                                    crate::plugin::plugin_manager::OnConnectAuthResult::Pass(tenant_id, user_id) => {
                                        todo!()
                                    },
                                    crate::plugin::plugin_manager::OnConnectAuthResult::Forbidden => {
                                        todo!()
                                    },
                                    crate::plugin::plugin_manager::OnConnectAuthResult::Error(_) => todo!(),
                                }
                            },
                            Err(e) => {
                                warn!("auth on connect failed: {}", e);
                            },
                        }
                    }
                    _ => {
                        connection.shutdown().await.unwrap();
                    }
                }
            });
        }
    }

}
