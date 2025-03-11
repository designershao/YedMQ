use std::{fs::File, io::Read, sync::Arc};

use actix::Actor;
use anyhow::Result;
use tokio::net::TcpListener;
use tokio_native_tls::native_tls::{self, Identity};

use crate::connection::ConnectionActor;

pub struct MqttTcpTlsListener {
    pub app: Arc<crate::app::YedMQApp>,
}

impl MqttTcpTlsListener {
    pub async fn run(&mut self) -> Result<()> {
        let mut cert_file = File::open(&self.app.settings.listener.tcp_tls.cert_file)?;
        let mut key_file = File::open(&self.app.settings.listener.tcp_tls.key_file)?;

        let mut cert = vec![];
        cert_file.read_to_end(&mut cert).unwrap();

        let mut key = vec![];
        key_file.read_to_end(&mut key).unwrap();

        let cert = Identity::from_pkcs8(&cert, &key)?;

        let tls_acceptor =
            tokio_native_tls::TlsAcceptor::from(native_tls::TlsAcceptor::builder(cert).build()?);

        let listener =
            TcpListener::bind(self.app.settings.listener.tcp_tls.external.clone()).await?;

        loop {
            let (stream, _) = listener.accept().await?;

            let tls_acceptor = tls_acceptor.clone();

            let remote_addr = stream.peer_addr().unwrap();

            let tls_stream = tls_acceptor.accept(stream).await.unwrap();

            let settings = self.app.settings.clone();
            ConnectionActor::new(
                tls_stream,
                settings.mqtt.max_message_size,
                4096,
                remote_addr,
                self.app.plugin_manager.clone(),
                self.app.session_manager.get().unwrap().clone().recipient(),
            )
            .start();
        }
    }
}
