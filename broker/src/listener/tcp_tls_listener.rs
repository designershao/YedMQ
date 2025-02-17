use std::{fs::File, io::Read, sync::Arc};

use anyhow::Result;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_native_tls::native_tls::{self, Identity};

use crate::{
    metric::Metric, plugin_manager::PluginManager, router::RouterCmd,
    session::session_manager::SessionManager, settings::Settings, topic::TopicManager,
};

use super::accept_connection;

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

            let plugin_manager = self.app.plugin_manager.clone();
            let session_manager = self.app.session_manager.clone();
            let topic_manager = self.app.topic_manager.clone();
            let router_sender = self.app.router_sender.get().unwrap().clone();
            let settings = self.app.settings.clone();
            let tls_acceptor = tls_acceptor.clone();

            let remote_addr = stream.peer_addr().unwrap();

            let tls_stream = tls_acceptor.accept(stream).await.unwrap();

            tokio::spawn(accept_connection(
                tls_stream,
                plugin_manager,
                session_manager,
                topic_manager,
                router_sender,
                settings,
                remote_addr,
                self.app.metric.clone(),
            ));
        }
    }
}
