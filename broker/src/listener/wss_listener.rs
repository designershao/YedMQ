use std::{sync::Arc, time::Duration, fs::File, io::Read};

use anyhow::Result;
use log::warn;
use tokio::{net::TcpListener, select, sync::RwLock, io::{AsyncRead, AsyncWrite}};
use tokio_native_tls::native_tls::{Identity, self};
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

use super::{accept_connection, websocket_tls_tunnel::{WebsocketTlsTunnel, StreamWrapper}, WsCallBack};
pub struct MqttWssListener {
    pub plugin_manager: Arc<PluginManager>,
    pub session_manager: Arc<RwLock<SessionManager>>,
    pub topic_manager: Arc<RwLock<TopicManager>>,
    pub router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
    pub settings: Arc<Settings>,
}

impl MqttWssListener {

    pub async fn run(self) -> Result<()> {

        let mut cert_file = File::open(&self.settings.listener.wss.cert_file)?;
        let mut key_file = File::open(&self.settings.listener.wss.key_file)?;

        let mut cert = vec![];
        cert_file.read_to_end(&mut cert).unwrap();

        let mut key = vec![];
        key_file.read_to_end(&mut key).unwrap();
        
        let cert = Identity::from_pkcs8(&cert, &key)?;

        let tls_acceptor = tokio_native_tls::TlsAcceptor::from(native_tls::TlsAcceptor::builder(cert).build()?);

        let listener = TcpListener::bind(self.settings.listener.wss.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            let peer_addr = stream.peer_addr().unwrap();
            let plugin_manager = self.plugin_manager.clone();
            let session_manager = self.session_manager.clone();
            let topic_manager = self.topic_manager.clone();
            let router_sender = self.router_sender.clone();
            let settings = self.settings.clone();

            let remote_addr = stream.peer_addr().unwrap();

            let mut tls_stream = tls_acceptor.accept(stream).await.unwrap();

            let ws_stream = tokio_tungstenite::accept_hdr_async(tls_stream, WsCallBack{}).await;

            if let Ok(ws_stream) = ws_stream {
                let websocket_tunnel = WebsocketTlsTunnel {
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