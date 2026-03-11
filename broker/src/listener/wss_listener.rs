use std::sync::Arc;

use anyhow::{anyhow, Result};
use log::warn;
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
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
        let certs =
            match CertificateDer::pem_file_iter(&self.app.settings.listener.tcp_tls.cert_file) {
                Ok(iter) => iter.collect::<Result<Vec<_>, _>>()?,
                Err(e) => match e {
                    rustls::pki_types::pem::Error::Io(error) => {
                        return Err(anyhow!("failed to read TLS certificate file: {}", error))
                    }
                    _ => return Err(anyhow!("failed to load TLS certificate: {}", e)),
                },
            };

        let key = PrivateKeyDer::from_pem_file(&self.app.settings.listener.wss.key_file)?;

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;

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
                        let client_certs = tls_stream.get_ref().1.peer_certificates();
                        let client_certificate_vec = {
                            if let Some(certs) = client_certs {
                                if let Some(client_cert_der) = certs.first() {
                                    let certificate_vec = client_cert_der.as_ref().to_vec();
                                    Some(certificate_vec)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        };

                        let ws_stream =
                            tokio_tungstenite::accept_hdr_async(tls_stream, WsCallBack {}).await;

                        if let Ok(ws_stream) = ws_stream {
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
                        } else {
                            warn!(
                                "Failed to accept WebSocket connection from {}, err {}",
                                remote_addr,
                                ws_stream.err().unwrap()
                            );
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
