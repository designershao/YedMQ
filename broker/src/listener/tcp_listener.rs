use std::sync::Arc;

use actix::Actor;
use anyhow::Result;
use tokio::net::TcpListener;

use crate::connection::ConnectionActor;

pub struct MqttTcpListener {
    pub app: Arc<crate::app::YedMQApp>,
}

impl MqttTcpListener {
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.app.settings.listener.tcp.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let peer_addr = stream.peer_addr().unwrap();
            let settings = self.app.settings.clone();
            ConnectionActor::new(
                stream,
                settings.mqtt.max_message_size,
                4096,
                peer_addr,
                self.app.plugin_manager.clone(),
                self.app.session_manager.get().unwrap().clone().recipient(),
            )
            .start();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        env, fs,
        path::{Path, PathBuf},
        time::Duration,
    };

    use actix::spawn;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::{Mutex, OnceCell, RwLock},
    };

    use yedmq_mqtt::MqttPacketV3;

    use crate::{
        plugin_manager::PluginManager,
        raft::raft_manager::RaftManager,
        session::{
            session_actor_map_storage::{SessionActorMapStorage, SessionClock},
            session_manager_actor::SessionManagerActor, session_state_storage::SessionStateStorage,
        },
        settings::{Cluster, Settings, RPC},
        topic::{topic_manager::TopicManager, topic_storage::TopicStorage},
    };

    use super::*;

    fn random_tcp_port() -> u16 {
        use rand::Rng;
        rand::thread_rng().gen_range(1024..=65535)
    }

    async fn mock_raft_manager() -> RaftManager {
        // Generate a random temporary directory
        let tmp_dir = env::temp_dir();
        let random_dir = Path::new(&tmp_dir).join(uuid::Uuid::new_v4().to_string());
        fs::create_dir_all(&random_dir).unwrap();
        let test_temp_store_dir = random_dir.to_str().unwrap().to_string();

        let test_cluster_cfg = Cluster {
            cluster_name: "test_cluster".to_string(),
            heartbeat_interval: 10,
            node_id: 1,
            store_dir: test_temp_store_dir.clone(),
            rpc: RPC {
                external: "127.0.0.1:4321".to_string(),
            },
        };

        RaftManager::new(
            test_cluster_cfg,
        )
        .await
    }

    async fn mock_topic_manager(
        topic_storage: Arc<RwLock<TopicStorage>>,
        raft_manager: Arc<RaftManager>,
        test_node_id: u64,
    ) -> TopicManager {
        TopicManager::new(topic_storage.clone(), raft_manager.clone(), test_node_id)
    }

    async fn mock_app(settings: Arc<crate::settings::Settings>) -> crate::app::YedMQApp {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests");

        let plugin_manager =
            PluginManager::new(plugin_path.to_str().unwrap().to_string(), settings.clone())
                .unwrap();
        let plugin_manager = Arc::new(plugin_manager);

        let topic_storage = Arc::new(RwLock::new(TopicStorage::new()));

        let raft_manager = Arc::new(mock_raft_manager().await);

        let topic_manager = Arc::new(RwLock::new(
            mock_topic_manager(topic_storage.clone(), raft_manager.clone(), 1).await,
        ));
        let (router_sender, _) = tokio::sync::mpsc::channel(10);

        let router_sender_once_cell = OnceCell::new();
        let _ = router_sender_once_cell.set(router_sender.clone());

        let session_clock = Arc::new(SessionClock::new(1, ".".to_string()));

        let session_manager = SessionManagerActor::new(
            plugin_manager.clone(),
            topic_manager.clone(),
            router_sender.clone(),
            settings.clone(),
            raft_manager.clone(),
            session_clock.clone(),
        )
        .start();

        crate::app::YedMQApp {
            settings,
            plugin_manager,
            session_manager: session_manager.into(),
            topic_manager,
            router_sender: router_sender_once_cell,
            metric: Arc::new(crate::metric::Metric::new()),
            join_handles: Mutex::new(vec![]),
            topic_router: Arc::new(RwLock::new(BTreeMap::new())),
            topic_storage: topic_storage,
            raft_manager: raft_manager,
        }
    }

    #[actix::test]
    pub async fn test_invalid_connect_packet_income() {
        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let tcp_port = random_tcp_port();

        let settings = Settings {
            session: crate::settings::Session {
                qos_expired_secs: 2,
                packet_resend_interval_secs: resend_duration_secs,
            },
            listener: crate::settings::Listener {
                tcp: crate::settings::Tcp {
                    external: format!("127.0.0.1:{}", tcp_port).to_string(),
                },
                tcp_tls: crate::settings::TcpTls {
                    external: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string(),
                },
                ws: crate::settings::Ws {
                    external: "".to_string(),
                },
                wss: crate::settings::Wss {
                    external: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string(),
                },
                api: crate::settings::Api {
                    external: "".to_string(),
                    auth: crate::settings::AuthConfig { users: vec![] },
                },
            },
            plugin: crate::settings::Plugin {
                dir: "test".to_string(),
            },
            mqtt: crate::settings::Mqtt {
                sys_topic_interval_secs: 10,
                max_message_size: yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE,
                default_authentication: crate::settings::DefaultAuthenticationValue::Allow,
                default_authorization: crate::settings::DefaultAuthorizationValue::Allow,
                inflight_retry_interval_secs: 10
            },
            cluster: crate::settings::Cluster::default(),
        };

        let settings = Arc::new(settings);
        let app = Arc::new(mock_app(settings.clone()).await);

        let listener = MqttTcpListener { app };

        spawn(async move {
            listener.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(1000)).await; // ensure listener start

        let mut writer = tokio::net::TcpStream::connect(format!("0.0.0.0:{}", tcp_port))
            .await
            .unwrap();
        let _connect_packet =
            yedmq_mqtt::v3::connect::ConnectPacketBuilder::new("test".to_string())
                .clean_session(true)
                .keep_alive(keep_live_duration_secs)
                .build();

        let invalid_mqtt_packet = &[
            0x10, 0x28, 0x00, 0x08, 0x4D, 0x51, 0x54, 0x54, 0x04, 0xEE, 0x00, 0x00, 0x00, 0x04,
            0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51,
            0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54,
        ];
        writer.write(invalid_mqtt_packet).await.unwrap();
        writer.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = writer.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let (_, connack_packet) = yedmq_mqtt::v3::connack::parse(&buf).unwrap();
            assert_eq!(connack_packet.variable_header.connect_return_code, 0x01); // unsupport protocol
            assert!(true)
        }
    }

    #[actix::test]
    pub async fn test_tcp_listener_connect() {
        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let tcp_port = random_tcp_port();

        let settings = Settings {
            session: crate::settings::Session {
                qos_expired_secs: 2,
                packet_resend_interval_secs: resend_duration_secs,
            },
            listener: crate::settings::Listener {
                tcp: crate::settings::Tcp {
                    external: format!("127.0.0.1:{}", tcp_port).to_string(),
                },
                tcp_tls: crate::settings::TcpTls {
                    external: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string(),
                },
                ws: crate::settings::Ws {
                    external: "".to_string(),
                },
                wss: crate::settings::Wss {
                    external: "".to_string(),
                    cert_file: "".to_string(),
                    key_file: "".to_string(),
                },
                api: crate::settings::Api {
                    external: "".to_string(),
                    auth: crate::settings::AuthConfig { users: vec![] },
                },
            },
            plugin: crate::settings::Plugin {
                dir: "test".to_string(),
            },
            mqtt: crate::settings::Mqtt {
                sys_topic_interval_secs: 10,
                max_message_size: yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE,
                default_authentication: crate::settings::DefaultAuthenticationValue::Allow,
                default_authorization: crate::settings::DefaultAuthorizationValue::Allow,
                inflight_retry_interval_secs: 10
            },
            cluster: crate::settings::Cluster::default(),
        };

        let settings = Arc::new(settings);
        let app = Arc::new(mock_app(settings.clone()).await);

        let listener = MqttTcpListener { app };

        spawn(async move {
            listener.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(1000)).await; // ensure listener start

        let mut writer = tokio::net::TcpStream::connect(format!("0.0.0.0:{}", tcp_port))
            .await
            .unwrap();
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
            let packet = yedmq_mqtt::parse(&buf, yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE)
                .unwrap()
                .1
                 .1;
            match packet {
                MqttPacketV3::Connack(connack_packet) => {
                    assert_eq!(connack_packet.variable_header.connect_return_code, 0x00);
                }
                _ => assert!(false),
            }
        }
    }
}
