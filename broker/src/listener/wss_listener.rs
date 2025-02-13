use std::{fs::File, io::Read, sync::Arc};

use anyhow::Result;
use log::warn;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_native_tls::native_tls::{self, Identity};
use tokio_util::io::StreamReader;

use crate::{
    metric::Metric, plugin_manager::PluginManager, router::RouterCmd,
    session::session_manager::SessionManager, settings::Settings, topic::TopicManager,
};

use super::{
    accept_connection,
    websocket_tls_tunnel::{StreamWrapper, WebsocketTlsTunnel},
    WsCallBack,
};

pub struct MqttWssListener {
    pub app: Arc<crate::app::YedMQApp>,
}

impl MqttWssListener {
    pub async fn run(self) -> Result<()> {
        let mut cert_file = File::open(&self.app.settings.listener.wss.cert_file)?;
        let mut key_file = File::open(&self.app.settings.listener.wss.key_file)?;

        let mut cert = vec![];
        cert_file.read_to_end(&mut cert).unwrap();

        let mut key = vec![];
        key_file.read_to_end(&mut key).unwrap();

        let cert = Identity::from_pkcs8(&cert, &key)?;

        let tls_acceptor =
            tokio_native_tls::TlsAcceptor::from(native_tls::TlsAcceptor::builder(cert).build()?);

        let listener = TcpListener::bind(self.app.settings.listener.wss.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            let peer_addr = stream.peer_addr().unwrap();
            let plugin_manager = self.app.plugin_manager.clone();
            let session_manager = self.app.session_manager.clone();
            let topic_manager = self.app.topic_manager.clone();
            let router_sender = self.app.router_sender.clone();
            let settings = self.app.settings.clone();

            let remote_addr = stream.peer_addr().unwrap();

            let tls_stream = tls_acceptor.accept(stream).await.unwrap();

            let ws_stream = tokio_tungstenite::accept_hdr_async(tls_stream, WsCallBack {}).await;

            if let Ok(ws_stream) = ws_stream {
                let websocket_tunnel = WebsocketTlsTunnel {
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
