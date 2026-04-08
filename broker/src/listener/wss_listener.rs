use std::sync::Arc;

use anyhow::Result;
use log::warn;
use tokio::net::TcpListener;
use tokio_util::io::StreamReader;

use crate::connection::{ConnectionActor, ConnectionActorStartConfig};

use super::{
    websocket_tls_tunnel::{StreamWrapper, WebsocketTlsTunnel},
    WsCallBack,
};

pub struct MqttWssListener {
    pub app: Arc<crate::app::YedMQApp>,
    pub arbiter_pool: Arc<crate::arbiter_pool::ArbiterPool>,
}

impl MqttWssListener {
    pub async fn run(self) -> Result<()> {
        let config = super::build_server_tls_config(
            &self.app.settings.listener.wss.cert_file,
            &self.app.settings.listener.wss.key_file,
            self.app.settings.listener.wss.verify_client_cert,
            &self.app.settings.listener.wss.cacert_file,
        )?;

        let tls_acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));

        let listener = TcpListener::bind(self.app.settings.listener.wss.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            match stream.peer_addr() {
                Ok(remote_addr) => match tls_acceptor.accept(stream).await {
                    Err(e) => {
                        log::error!("TLS accept error from {}: {}", remote_addr, e);
                        continue;
                    }
                    Ok(tls_stream) => {
                        let client_certificate_vec = tls_stream
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|certs| certs.first())
                            .map(|client_cert_der| client_cert_der.as_ref().to_vec());

                        match tokio_tungstenite::accept_hdr_async(tls_stream, WsCallBack {}).await {
                            Ok(ws_stream) => {
                                let websocket_tunnel = WebsocketTlsTunnel {
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
                                        client_certificate: client_certificate_vec,
                                        metric,
                                        rate_limit: settings.listener.wss.rate_limit.clone(),
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
                },
                Err(_) => {
                    warn!("failed to get peer address, close the connection");
                }
            }
        }
    }
}
