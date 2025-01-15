use std::sync::Arc;

use anyhow::Result;
use tokio::{net::TcpListener, sync::RwLock};

use crate::{
     metric::Metric, plugin_manager::PluginManager, router::RouterCmd, session::session_manager::SessionManager, settings::Settings, topic::TopicManager
};


use super::accept_connection;

pub struct MqttTcpListener {
    pub plugin_manager: Arc<PluginManager>,
    pub session_manager: Arc<RwLock<SessionManager>>,
    pub topic_manager: Arc<RwLock<TopicManager>>,
    pub router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
    pub settings: Arc<Settings>,
    pub metric: Arc<Metric>,
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
                accept_connection(stream, plugin_manager, session_manager, topic_manager, router_sender, settings, peer_addr, self.metric.clone())
            );
        }
    }
}


#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, time::Duration};

    use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::mpsc::Sender};

    use yedmq_mqtt::MqttPacketV3;

    use crate::session::session_manager::SessionMessage;

    use super::*;

    fn random_tcp_port() -> u16 {
        use rand::Rng;
        rand::thread_rng().gen_range(1024..=65535)
    }

    async fn get_test_plugin_manager(settings: Arc<crate::settings::Settings>) -> Arc<PluginManager> {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests");


        let plugin_manager = PluginManager::new(
            plugin_path.to_str().unwrap().to_string(),
            settings,
        )
            .unwrap();
        Arc::new(plugin_manager)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    pub async fn test_invalid_connect_packet_income() {


        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (router_sender, _) = tokio::sync::mpsc::channel(10);

        let tcp_port = random_tcp_port();

        let settings = Settings {
            session: crate::settings::Session { qos_expired_secs: 2, packet_resend_interval_secs: resend_duration_secs },
            listener: crate::settings::Listener { 
                tcp: crate::settings::Tcp { external: format!("127.0.0.1:{}", tcp_port).to_string() },
                tcp_tls: crate::settings::TcpTls {
                    external: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string()
                },
                ws: crate::settings::Ws { external: "".to_string() },
                wss: crate::settings::Wss {
                     external: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string()
                    },
                api: crate::settings::Api { external: "".to_string(), auth: crate::settings::AuthConfig { users: vec![] } }
            },
            plugin: crate::settings::Plugin { dir: "test".to_string() },
            mqtt: crate::settings::Mqtt { 
                sys_topic_interval_secs: 10, 
                max_message_size: yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE,
                default_authentication: crate::settings::DefaultAuthenticationValue::Allow, 
                default_authorization: crate::settings::DefaultAuthorizationValue::Allow 
            }
        };

        let metric = Arc::new(Metric::new());
        let settings = Arc::new(settings);
        let plugin_manager =get_test_plugin_manager(settings.clone()).await;

        let listener = MqttTcpListener {
            plugin_manager,
            session_manager: Arc::new(RwLock::new(SessionManager{ sessions:  HashMap::<String,HashMap<String, Sender<SessionMessage>>>::new()})),
            topic_manager: Arc::new(RwLock::new(TopicManager::new())),
            router_sender: router_sender,
            settings: settings.clone(),
            metric: metric.clone(),
        };

        tokio::spawn(async move {
            listener.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(1000)).await; // ensure listener start

        let mut writer = tokio::net::TcpStream::connect(format!("0.0.0.0:{}", tcp_port)).await.unwrap();
        let _connect_packet = yedmq_mqtt::v3::connect::ConnectPacketBuilder::new("test".to_string())
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

        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (router_sender, _) = tokio::sync::mpsc::channel(10);

        let tcp_port = random_tcp_port();

        let settings = Settings {
            session: crate::settings::Session { qos_expired_secs: 2, packet_resend_interval_secs: resend_duration_secs },
            listener: crate::settings::Listener {
                tcp: crate::settings::Tcp { external: format!("127.0.0.1:{}", tcp_port).to_string() },
                tcp_tls: crate::settings::TcpTls {
                    external: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string()
                },
                ws: crate::settings::Ws { external: "".to_string() },
                wss: crate::settings::Wss {
                     external: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string()
                    },
                api: crate::settings::Api { external: "".to_string(), auth: crate::settings::AuthConfig { users: vec![] } }
            },
            plugin: crate::settings::Plugin { dir: "test".to_string() },
            mqtt: crate::settings::Mqtt { 
                sys_topic_interval_secs: 10, 
                max_message_size: yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE,
                default_authentication: crate::settings::DefaultAuthenticationValue::Allow, 
                default_authorization: crate::settings::DefaultAuthorizationValue::Allow 
            }
        };

        let metric = Arc::new(Metric::new());

        let settings = Arc::new(settings);
        let plugin_manager =get_test_plugin_manager(settings.clone()).await;

        let listener = MqttTcpListener {
            plugin_manager,
            session_manager: Arc::new(RwLock::new(SessionManager{ sessions:  HashMap::<String,HashMap<String, Sender<SessionMessage>>>::new()})),
            topic_manager: Arc::new(RwLock::new(TopicManager::new())),
            router_sender: router_sender,
            settings: settings.clone(),
            metric: metric.clone(),
        };

        tokio::spawn(async move {
            listener.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(1000)).await; // ensure listener start

        let mut writer = tokio::net::TcpStream::connect(format!("0.0.0.0:{}", tcp_port)).await.unwrap();
        let connect_packet = yedmq_mqtt::v3::connect::ConnectPacketBuilder::new("test".to_string())
            .clean_session(true)
            .keep_alive(keep_live_duration_secs)
            .build();

        let connect_packet = yedmq_mqtt::MqttPacketV3::Connect(connect_packet);
        writer.write(&connect_packet.to_bytes()).await.unwrap();
        writer.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = writer.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = yedmq_mqtt::parse(&buf, yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE).unwrap().1.1;
            match packet {
                MqttPacketV3::Connack(connack_packet) => {
                    assert_eq!(connack_packet.variable_header.connect_return_code, 0x00);
                }
                _ => assert!(false)
            }
        }

    }

}