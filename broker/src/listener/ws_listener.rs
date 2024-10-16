use std::{sync::Arc, time::Duration};

use anyhow::Result;
use log::warn;
use tokio::{net::{TcpListener, TcpStream}, select, sync::RwLock, io::{AsyncRead, AsyncWrite}};
use tokio_tungstenite::tungstenite::{handshake::server::Callback, http::HeaderValue};
use tokio_util::io::StreamReader;

use crate::{
    connection::Connection, inflight::Inflight, plugin_manager::PluginManager, plugin_service::service::PluginService, router::RouterCmd, session::{Session, SessionHandle, SessionManager, SessionManagerError}, settings::Settings, topic::TopicManager
};

use samoye_mqtt::{
        v3::{
            connack::{ConnAckPacket, ConnAckPacketBuilder, VariableHeader},
            fixed_header::FixHeader,
        },
        PacketType,
    };

use super::{accept_connection, websocket_tunnel::{WebsocketTunnel, StreamWrapper}, WsCallBack};
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

            let ws_stream = tokio_tungstenite::accept_hdr_async(stream, WsCallBack{}).await;

            if let Ok(ws_stream) = ws_stream  {
                let websocket_tunnel = WebsocketTunnel {
                    inner: StreamReader::new(
                        StreamWrapper {
                            inner: ws_stream
                        }
                    ),
                };

                tokio::spawn(
                    accept_connection(websocket_tunnel, plugin_manager, session_manager, topic_manager, router_sender, settings, peer_addr)
                );
            } else {
                warn!("Failed to accept WebSocket connection from {}, err {}", remote_addr, ws_stream.err().unwrap());
            }

        }
    }
}