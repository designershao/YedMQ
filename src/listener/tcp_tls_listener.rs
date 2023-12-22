use std::{sync::Arc, time::Duration, fs::File, io::Read};

use anyhow::Result;
use log::warn;
use tokio::{net::TcpListener, select, sync::RwLock};
use tokio_native_tls::native_tls::{Identity, self};

use crate::{
    connection::Connection,
    inflight::Inflight,
    plugin::{self, plugin_manager::PluginManager, session_context::SessionContext},
    protocol::{
        v3::{
            connack::{ConnAckPacket, ConnAckPacketBuilder, VariableHeader},
            fixed_header::FixHeader,
        },
        PacketType,
    },
    router::RouterCmd,
    session::{Session, SessionHandle, SessionManager, SessionManagerError},
    settings::Settings,
    topic::TopicManager,
};

pub struct MqttTcpTlsListener {
    pub plugin_manager: Arc<PluginManager>,
    pub session_manager: Arc<RwLock<SessionManager>>,
    pub topic_manager: Arc<RwLock<TopicManager>>,
    pub router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
    pub settings: Arc<Settings>,
}


impl MqttTcpTlsListener {
    pub async fn run(&mut self) -> Result<()> {
        let mut cert_file = File::open(&self.settings.listener.tcp_tls.cert_file)?;
        let mut key_file = File::open(&self.settings.listener.tcp_tls.key_file)?;

        let mut cert = vec![];
        cert_file.read_to_end(&mut cert).unwrap();

        let mut key = vec![];
        key_file.read_to_end(&mut key).unwrap();
        
        let cert = Identity::from_pkcs8(&cert, &key)?;
        
        let tls_acceptor = tokio_native_tls::TlsAcceptor::from(native_tls::TlsAcceptor::builder(cert).build()?);

        Ok(())
    }
}
