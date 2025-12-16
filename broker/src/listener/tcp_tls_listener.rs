use std::sync::Arc;

use actix::Actor;
use anyhow::Result;
use log::warn;
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

use crate::connection::ConnectionActor;

pub struct MqttTcpTlsListener {
    pub app: Arc<crate::app::YedMQApp>,
    pub arbiter_pool: Arc<crate::arbiter_pool::ArbiterPool>,
}

impl MqttTcpTlsListener {
    pub async fn run(&mut self) -> Result<()> {

        let certs = CertificateDer::pem_file_iter(&self.app.settings.listener.tcp_tls.cert_file)?.collect::<Result<Vec<_>,_>>()?;
        let key = PrivateKeyDer::from_pem_file(&self.app.settings.listener.tcp_tls.key_file)?;

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;

        let tls_acceptor =
            tokio_rustls::TlsAcceptor::from(Arc::new(config));


        let listener =
            TcpListener::bind(self.app.settings.listener.tcp_tls.external.clone()).await?;

        loop {
            let (stream, _) = listener.accept().await?;

            let tls_acceptor = tls_acceptor.clone();

            match stream.peer_addr() {
                Ok(remote_addr) => {
                    match tls_acceptor.accept(stream).await {
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
                                        let certificate_vec =  client_cert_der.as_ref().to_vec();
                                        Some(certificate_vec   )
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            };
                            let plugin_manager_clone = self.app.plugin_manager.clone();
                            self.arbiter_pool.start_actor(move || {
                                ConnectionActor::new(
                                    tls_stream,
                                    settings.mqtt.max_message_size,
                                    4096,
                                    remote_addr,
                                    plugin_manager_clone,
                                    client_certificate_vec
                                )
                            });
                        }
                    }

                }
                Err(_) => {
                    warn!("failed to get peer address, close the connection");
                }
            }
        }
    }
}
