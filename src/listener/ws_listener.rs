use std::{sync::Arc, time::Duration};

use anyhow::Result;
use log::warn;
use tokio::{net::{TcpListener, TcpStream}, select, sync::RwLock, io::{AsyncRead, AsyncWrite}};

use crate::{
    connection::Connection,
    inflight::Inflight,
    plugin::{self, plugin_manager::PluginManager, session_context::SessionContext},
    protocol::{
        v3::{
            connack::{ConnAckPacket, ConnAckPacketBuilder, VariableHeader},
            fixed_header::FixHeader,
        },
        PacketType,
    },
    router::RouterCmd,
    session::{Session, SessionHandle, SessionManager, SessionManagerError},
    settings::Settings,
    topic::TopicManager,
};

use super::accept_connection;
pub struct MqttWsListener {
    pub plugin_manager: Arc<PluginManager>,
    pub session_manager: Arc<RwLock<SessionManager>>,
    pub topic_manager: Arc<RwLock<TopicManager>>,
    pub router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
    pub settings: Arc<Settings>,
}

impl MqttWsListener {

    pub async fn run(self) -> Result<()> {

        let listener = TcpListener::bind(self.settings.listener.ws.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            let peer_addr = stream.peer_addr().unwrap();
            let plugin_manager = self.plugin_manager.clone();
            let session_manager = self.session_manager.clone();
            let topic_manager = self.topic_manager.clone();
            let router_sender = self.router_sender.clone();
            let settings = self.settings.clone();

            let remote_addr = stream.peer_addr().unwrap();

            let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();

            tokio::spawn(
                accept_connection(ws_stream, plugin_manager, session_manager, topic_manager, router_sender, settings, peer_addr)
            );
        }
    }
}