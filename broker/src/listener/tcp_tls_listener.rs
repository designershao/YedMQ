use std::{sync::Arc, fs::File, io::Read};

use anyhow::Result;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_native_tls::native_tls::{Identity, self};

use crate::{
    metric::Metric, plugin_manager::PluginManager, router::RouterCmd, session::session_manager::SessionManager, settings::Settings, topic::TopicManager
};


use super::accept_connection;

pub struct MqttTcpTlsListener {
    pub plugin_manager: Arc<PluginManager>,
    pub session_manager: Arc<RwLock<SessionManager>>,
    pub topic_manager: Arc<RwLock<TopicManager>>,
    pub router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
    pub settings: Arc<Settings>,
    pub metric: Arc<Metric>,
}


impl MqttTcpTlsListener
{
    pub async fn run(&mut self) -> Result<()> {
        let mut cert_file = File::open(&self.settings.listener.tcp_tls.cert_file)?;
        let mut key_file = File::open(&self.settings.listener.tcp_tls.key_file)?;

        let mut cert = vec![];
        cert_file.read_to_end(&mut cert).unwrap();

        let mut key = vec![];
        key_file.read_to_end(&mut key).unwrap();
        
        let cert = Identity::from_pkcs8(&cert, &key)?;
        
        let tls_acceptor = tokio_native_tls::TlsAcceptor::from(native_tls::TlsAcceptor::builder(cert).build()?);

        let listener = TcpListener::bind(self.settings.listener.tcp_tls.external.clone()).await?;

        loop {
            let (stream, _) = listener.accept().await?;

            let plugin_manager = self.plugin_manager.clone();
            let session_manager = self.session_manager.clone();
            let topic_manager = self.topic_manager.clone();
            let router_sender = self.router_sender.clone();
            let settings = self.settings.clone();
            let tls_acceptor = tls_acceptor.clone();

            let remote_addr = stream.peer_addr().unwrap();

            let tls_stream = tls_acceptor.accept(stream).await.unwrap();

            tokio::spawn(accept_connection(tls_stream, plugin_manager, session_manager, topic_manager, router_sender, settings, remote_addr, self.metric.clone()));
        }
    }
}
