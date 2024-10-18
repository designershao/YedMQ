use std::{sync::Arc, time::Duration};

use anyhow::Result;
use log::warn;
use tokio::{net::TcpListener, select, sync::RwLock, io::{AsyncRead, AsyncWrite}};

use crate::{
    connection::Connection, inflight::Inflight, plugin_manager::PluginManager,  router::RouterCmd, session::{Session, SessionHandle, SessionManager, SessionManagerError}, settings::Settings, topic::TopicManager
};

use samoye_mqtt::{
    v3::{
        connack::{ConnAckPacket, ConnAckPacketBuilder, VariableHeader},
        fixed_header::FixHeader,
    },
    PacketType,
};

use super::accept_connection;

pub struct MqttTcpListener {
    pub plugin_manager: Arc<PluginManager>,
    pub session_manager: Arc<RwLock<SessionManager>>,
    pub topic_manager: Arc<RwLock<TopicManager>>,
    pub router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
    pub settings: Arc<Settings>,
}

impl MqttTcpListener
{
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.settings.listener.tcp.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            let peer_addr = stream.peer_addr().unwrap();
            let plugin_manager = self.plugin_manager.clone();
            let session_manager = self.session_manager.clone();
            let topic_manager = self.topic_manager.clone();
            let router_sender = self.router_sender.clone();
            let settings = self.settings.clone();

            tokio::spawn(
                accept_connection(stream, plugin_manager, session_manager, topic_manager, router_sender, settings, peer_addr)
            );
        }
    }
}


mod tests {
    use std::{path::PathBuf, collections::HashMap};

    use tokio::io::{AsyncWriteExt, AsyncReadExt};

    use crate::settings::Plugin;

    use samoye_mqtt::MqttPacketV3;

    use super::*;

    async fn get_test_plugin_manager() -> Arc<PluginManager> {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests");

        let plugin_manager = PluginManager::new(plugin_path.to_str().unwrap().to_string())
            .unwrap();
        Arc::new(plugin_manager)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    pub async fn test_invalid_connect_packet_income() {

        let plugin_manager =get_test_plugin_manager().await;

        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

        let settings = Settings {
            session: crate::settings::Session { qos_expired_secs: 2, packet_resend_interval_secs: resend_duration_secs },
            listener: crate::settings::Listener { 
                tcp: crate::settings::Tcp { external: "127.0.0.1:18088".to_string() },
                tcp_tls: crate::settings::TcpTls {
                    external: "".to_string(),
                    cacert_file: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string()
                },
                ws: crate::settings::Ws { external: "".to_string() },
                wss: crate::settings::Wss {
                     external: "".to_string(),
                    cacert_file: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string()
                    }
            },
            plugin: crate::settings::Plugin { dir: "test".to_string() }
        };

        let listener = MqttTcpListener {
            plugin_manager,
            session_manager: Arc::new(RwLock::new(SessionManager{ session_table:  HashMap::<String,RwLock<HashMap<String, SessionHandle>>>::new()})),
            topic_manager: Arc::new(RwLock::new(TopicManager::new())),
            router_sender: router_sender,
            settings: Arc::new(settings),
        };

        tokio::spawn(async move {
            listener.run().await.unwrap();
        });

        let mut writer = tokio::net::TcpStream::connect("0.0.0.0:18088").await.unwrap();
        let connect_packet = samoye_mqtt::v3::connect::ConnectPacketBuilder::new("test".to_string())
            .clean_session(true)
            .keep_alive(keep_live_duration_secs)
            .build();


        let invalid_mqtt_packet = &[0x10, 0x28,0x00,0x08,0x4D,0x51,0x54,0x54,0x04,0xEE,0x00,0x00,0x00,0x04,0x4D,0x51,0x54,0x54,0x00,0x04,0x4D,0x51,0x54,0x54,0x00,0x04,0x4D,0x51,0x54,0x54,0x00,0x04,0x4D,0x51,0x54,0x54,0x00,0x04,0x4D,0x51,0x54,0x54];
        writer.write(invalid_mqtt_packet).await.unwrap();
        writer.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = writer.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(true)
        } else {
            assert!(false)
        }
    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    pub async fn test_tcp_listener_connect() {

        let plugin_manager =get_test_plugin_manager().await;

        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

        let settings = Settings {
            session: crate::settings::Session { qos_expired_secs: 2, packet_resend_interval_secs: resend_duration_secs },
            listener: crate::settings::Listener {
                tcp: crate::settings::Tcp { external: "127.0.0.1:18088".to_string() },
                tcp_tls: crate::settings::TcpTls {
                    external: "".to_string(),
                    cacert_file: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string()
                },
                ws: crate::settings::Ws { external: "".to_string() },
                wss: crate::settings::Wss {
                     external: "".to_string(),
                    cacert_file: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string()
                    }
            },
            plugin: crate::settings::Plugin { dir: "test".to_string() }
        };

        let listener = MqttTcpListener {
            plugin_manager,
            session_manager: Arc::new(RwLock::new(SessionManager{ session_table:  HashMap::<String,RwLock<HashMap<String, SessionHandle>>>::new()})),
            topic_manager: Arc::new(RwLock::new(TopicManager::new())),
            router_sender: router_sender,
            settings: Arc::new(settings),
        };

        tokio::spawn(async move {
            listener.run().await.unwrap();
        });

        let mut writer = tokio::net::TcpStream::connect("0.0.0.0:18088").await.unwrap();
        let connect_packet = samoye_mqtt::v3::connect::ConnectPacketBuilder::new("test".to_string())
            .clean_session(true)
            .keep_alive(keep_live_duration_secs)
            .build();

        let connect_packet = samoye_mqtt::MqttPacketV3::Connect(connect_packet);
        writer.write(&connect_packet.to_bytes()).await.unwrap();
        writer.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = writer.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = samoye_mqtt::parse(&buf).unwrap().1.1;
            match packet {
                MqttPacketV3::Connack(connack_packet) => {
                    assert_eq!(connack_packet.variable_header.connect_return_code, 0x00);
                }
                _ => assert!(false)
            }
        }

    }

}