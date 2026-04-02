use std::sync::Arc;

use anyhow::Result;
use log::warn;
use tokio::net::TcpListener;

use crate::connection::{ConnectionActor, ConnectionActorStartConfig};

pub struct MqttTcpTlsListener {
    pub app: Arc<crate::app::YedMQApp>,
    pub arbiter_pool: Arc<crate::arbiter_pool::ArbiterPool>,
}

impl MqttTcpTlsListener {
    pub async fn run(&mut self) -> Result<()> {
        let config = super::build_server_tls_config(
            &self.app.settings.listener.tcp_tls.cert_file,
            &self.app.settings.listener.tcp_tls.key_file,
            self.app.settings.listener.tcp_tls.verify_client_cert,
            &self.app.settings.listener.tcp_tls.cacert_file,
        )?;

        let tls_acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));

        let listener =
            TcpListener::bind(self.app.settings.listener.tcp_tls.external.clone()).await?;

        loop {
            let (stream, _) = listener.accept().await?;

            let tls_acceptor = tls_acceptor.clone();

            match stream.peer_addr() {
                Ok(remote_addr) => match tls_acceptor.accept(stream).await {
                    Err(e) => {
                        log::error!("TLS accept error from {}: {}", remote_addr, e);
                        continue;
                    }
                    Ok(tls_stream) => {
                        let settings = self.app.settings.clone();

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
                        let plugin_manager_clone = self.app.plugin_manager.clone();
                        let metric = self.app.metric.clone();
                        ConnectionActor::create_and_start(
                            tls_stream,
                            ConnectionActorStartConfig {
                                max_message_size: settings.mqtt.max_message_size,
                                default_buffer_size: 4096,
                                peer_addr: remote_addr,
                                plugin_service: plugin_manager_clone,
                                client_certificate: client_certificate_vec,
                                metric,
                                rate_limit: settings.listener.tcp_tls.rate_limit.clone(),
                            },
                        );
                    }
                },
                Err(_) => {
                    warn!("failed to get peer address, close the connection");
                }
            }
        }
    }
}
