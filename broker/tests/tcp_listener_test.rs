use std::collections::BTreeMap;
use std::path::Path;
use std::{env, fs, thread, time};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use yedmq::app::YedMQApp;
use yedmq::metric::Metric;
use yedmq::plugin_manager::PluginManager;
use yedmq::raft::raft_manager::RaftManager;

use actix::Actor;
use tokio::sync::{Mutex, OnceCell, RwLock};
use yedmq::session::session_state_storage::SessionStateStorage;
use yedmq::session::session_actor_map_storage;
use yedmq::session::session_manager_actor::SessionManagerActor;
use yedmq::settings::{Cluster, RPC};
use yedmq::topic::topic_manager::TopicManager;
use yedmq::topic::topic_storage::TopicStorage;
use yedmq::{listener::tcp_listener::MqttTcpListener, router::Router, settings::Settings};
use yedmq_mqtt::{v3::subscribe::TopicFilter, MqttPacketV3};

fn random_tcp_port() -> u16 {
    use rand::Rng;
    rand::thread_rng().gen_range(1024..=65535)
}

async fn mock_raft_manager(topic_storage: Arc<RwLock<TopicStorage>>) -> RaftManager {
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

    let session_actor_map_storage = Arc::new(RwLock::new(
        session_actor_map_storage::SessionActorMapStorage::new(),
    ));

    let session_state_storage = Arc::new(RwLock::new(SessionStateStorage::new()));

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

async fn mock_app(settings: Arc<Settings>) -> YedMQApp {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let plugin_path = PathBuf::from(crate_root_path).join("tests").join("plugins");

    let plugin_manager =
        PluginManager::new(plugin_path.to_str().unwrap().to_string(), settings.clone()).unwrap();
    let plugin_manager = Arc::new(plugin_manager);

    let topic_storage = Arc::new(RwLock::new(TopicStorage::new()));
    let raft_manager = Arc::new(mock_raft_manager(topic_storage.clone()).await);

    let topic_manager = Arc::new(RwLock::new(
        mock_topic_manager(topic_storage.clone(), raft_manager.clone(), 1).await,
    ));
    let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

    let router_sender_once_cell = OnceCell::new();
    let _ = router_sender_once_cell.set(router_sender.clone());

    let session_manager = SessionManagerActor::new(
        plugin_manager.clone(),
        topic_manager.clone(),
        router_sender.clone(),
        settings.clone(),
        raft_manager.clone()
    )
    .start();

    let mut router = Router {
        session_manager_recipient: session_manager.clone().recipient(),
        topic_manager: topic_manager.clone(),
        router_receiver: router_receiver,
        raft_manager: raft_manager.clone(),
    };

    actix::spawn(async move {
        router.run().await;
    });

    YedMQApp {
        settings,
        plugin_manager,
        session_manager: session_manager.into(),
        topic_manager,
        router_sender: router_sender_once_cell,
        metric: Arc::new(Metric::new()),
        join_handles: Mutex::new(vec![]),
        topic_router: Arc::new(RwLock::new(BTreeMap::new())),
        raft_manager,
        topic_storage,
    }
}
fn get_test_settings(qos_expired_secs: u64, resend_duration_sec: u64) -> Settings {
    let tcp_port = random_tcp_port();
    let settings = Settings {
        session: yedmq::settings::Session {
            qos_expired_secs: qos_expired_secs,
            packet_resend_interval_secs: resend_duration_sec,
        },
        listener: yedmq::settings::Listener {
            tcp: yedmq::settings::Tcp {
                external: format!("0.0.0.0:{}", tcp_port).to_string(),
            },
            tcp_tls: yedmq::settings::TcpTls {
                external: "0.0.0.0:18089".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
            },
            ws: yedmq::settings::Ws {
                external: "0.0.0.0:18090".to_string(),
            },
            wss: yedmq::settings::Wss {
                external: "0.0.0.0:18091".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
            },
            api: yedmq::settings::Api {
                external: "".to_string(),
                auth: yedmq::settings::AuthConfig { users: vec![] },
            },
        },
        plugin: yedmq::settings::Plugin {
            dir: "test".to_string(),
        },
        mqtt: yedmq::settings::Mqtt {
            sys_topic_interval_secs: 10,
            max_message_size: yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE,
            default_authentication: yedmq::settings::DefaultAuthenticationValue::Allow,
            default_authorization: yedmq::settings::DefaultAuthorizationValue::Allow,
            inflight_retry_interval_secs: 10
        },
        cluster: yedmq::settings::Cluster::default(),
    };
    settings
}

#[actix::test]
pub async fn test_tcp_listener_connect() {
    let keep_live_duration_secs = 5;

    let resend_duration_secs = 10;

    let settings = Arc::new(get_test_settings(2, resend_duration_secs));

    let app = Arc::new(mock_app(settings.clone()).await);

    let connect_address = settings.listener.tcp.external.clone();

    let listener = MqttTcpListener { app: app.clone() };

    actix::spawn(async move {
        listener.run().await.unwrap();
    });

    // ensure listener start
    let sleep_duration = time::Duration::from_millis(1000);
    thread::sleep(sleep_duration);
    //

    let mut writer = tokio::net::TcpStream::connect(connect_address)
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

#[actix::test]
pub async fn test_tcp_client_subscribe_and_publish() {
    let keep_live_duration_secs = 5;

    let resend_duration_secs = 10;

    let settings = Arc::new(get_test_settings(2, resend_duration_secs));

    let app = Arc::new(mock_app(settings.clone()).await);

    let connect_address = settings.listener.tcp.external.clone();
    let connect_address_cloned = connect_address.clone();

    let listener = MqttTcpListener { app: app.clone() };

    actix::spawn(async move {
        listener.run().await.unwrap();
    });

    // ensure listener start
    let sleep_duration = time::Duration::from_millis(1000);
    thread::sleep(sleep_duration);
    //

    // subscriber process
    let sub_join = actix::spawn(async move {
        let mut subscriber = tokio::net::TcpStream::connect(connect_address)
            .await
            .unwrap();
        let connect_packet =
            yedmq_mqtt::v3::connect::ConnectPacketBuilder::new("test_sub".to_string())
                .clean_session(true)
                .keep_alive(keep_live_duration_secs)
                .build();

        let connect_packet = yedmq_mqtt::MqttPacketV3::Connect(connect_packet);
        subscriber.write(&connect_packet.to_bytes()).await.unwrap();
        subscriber.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = yedmq_mqtt::parse(&buf, yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE)
                .unwrap()
                .1
                 .1;
            match packet {
                MqttPacketV3::Connack(connack_packet) => {
                    println!("connack_packet={:?}", connack_packet);
                    assert_eq!(connack_packet.variable_header.connect_return_code, 0x00);
                }
                _ => assert!(false),
            }
        }

        // subscribe
        let subscribe_packet = yedmq_mqtt::v3::subscribe::SubscribePacketBuilder::new(0x10)
            .add_topic_filter(TopicFilter {
                topic_name: "/a/b".to_string(),
                qos: 0,
            })
            .build();

        let subscribe_packet = yedmq_mqtt::MqttPacketV3::Subscribe(subscribe_packet);

        subscriber
            .write(&subscribe_packet.to_bytes())
            .await
            .unwrap();
        subscriber.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = yedmq_mqtt::parse(&buf, yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE)
                .unwrap()
                .1
                 .1;
            match packet {
                MqttPacketV3::Suback(suback_packet) => {
                    assert_eq!(suback_packet.variable_header.packet_identifier, 0x10);
                }
                _ => assert!(false),
            }
        }
        //

        // wait publish message
        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = yedmq_mqtt::parse(&buf, yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE)
                .unwrap()
                .1
                 .1;
            match packet {
                MqttPacketV3::Publish(publish_packet) => {
                    assert_eq!(publish_packet.payload.payload, vec![0x01, 0x02]);
                }
                _ => assert!(false),
            }
        }
        //
    });

    // ensure subscribe start
    let sleep_duration = time::Duration::from_millis(1000);
    thread::sleep(sleep_duration);
    //

    // publisher process
    let pub_join = actix::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await; // wait subscriber
        let mut publisher = tokio::net::TcpStream::connect(connect_address_cloned)
            .await
            .unwrap();
        let connect_packet =
            yedmq_mqtt::v3::connect::ConnectPacketBuilder::new("test_pub".to_string())
                .clean_session(true)
                .keep_alive(keep_live_duration_secs)
                .build();

        let connect_packet = yedmq_mqtt::MqttPacketV3::Connect(connect_packet);
        publisher.write(&connect_packet.to_bytes()).await.unwrap();
        publisher.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = publisher.read_buf(&mut buf).await.unwrap();
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

        let publish_packet = yedmq_mqtt::v3::publish::PublishPacketBuilder::new(
            "/a/b".to_string(),
            vec![0x01, 0x02],
        )
        .build();
        let publish_packet = yedmq_mqtt::MqttPacketV3::Publish(publish_packet);

        publisher.write(&publish_packet.to_bytes()).await.unwrap();
    });

    sub_join.await.unwrap();
    pub_join.await.unwrap();
}

#[actix::test]
pub async fn test_tcp_client_invalid_connect_packet_should_disconnect() {
    let resend_duration_secs = 10;

    let settings = Arc::new(get_test_settings(2, resend_duration_secs));

    let app = Arc::new(mock_app(settings.clone()).await);

    let connect_address = settings.listener.tcp.external.clone();

    let listener = MqttTcpListener { app: app.clone() };

    actix::spawn(async move {
        listener.run().await.unwrap();
    });

    // subscriber process
    let invalid_connect_join = actix::spawn(async move {
        let variable_header = yedmq_mqtt::v3::connect::VariableHeader {
            protocol_name: "MQT".to_string(), // invalid protocol name
            protocol_level: 0x04,
            username_flag: true,
            password_flag: true,
            will_retain: true,
            will_qos: 1,
            will_flag: true,
            clean_session: true,
            keep_alive: 0,
        };
        let payload = yedmq_mqtt::v3::connect::Payload {
            client_identifier: "MQTT".to_string(),
            will_topic: Some("MQTT".to_string()),
            will_message: Some("MQTT".to_string()),
            username: Some("MQTT".to_string()),
            password: Some("MQTT".to_string()),
        };

        let fix_header = yedmq_mqtt::v3::fixed_header::FixHeader {
            packet_type: yedmq_mqtt::PacketType::CONNECT,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: variable_header.get_length() + payload.get_length(),
        };

        let connect_packet = yedmq_mqtt::v3::connect::ConnectPacket {
            fix_header,
            variable_header,
            payload,
        };

        let mut invalid_connect = tokio::net::TcpStream::connect(connect_address)
            .await
            .unwrap();
        let connect_packet = yedmq_mqtt::MqttPacketV3::Connect(connect_packet);
        invalid_connect
            .write(&connect_packet.to_bytes())
            .await
            .unwrap();
        invalid_connect.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = invalid_connect.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(true)
        } else {
            assert!(false)
        }
    });

    invalid_connect_join.await.unwrap();
}

#[actix::test]
pub async fn test_when_tcp_client_unexpected_disconnect_broker_should_send_will_message() {
    let keep_live_duration_secs = 5;

    let resend_duration_secs = 10;

    let settings = Arc::new(get_test_settings(2, resend_duration_secs));

    let app = Arc::new(mock_app(settings.clone()).await);

    let connect_address = settings.listener.tcp.external.clone();

    let listener = MqttTcpListener { app: app.clone() };

    actix::spawn(async move {
        listener.run().await.unwrap();
    });

    // ensure listener start
    let sleep_duration = time::Duration::from_millis(1000);
    tokio::time::sleep(sleep_duration).await;
    //

    let connect_address_cloned = connect_address.clone();
    let unexpect_disconnect_join = actix::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await; // wait subscriber
        let mut publisher = tokio::net::TcpStream::connect(connect_address)
            .await
            .unwrap();
        let connect_packet =
            yedmq_mqtt::v3::connect::ConnectPacketBuilder::new("test_pub".to_string())
                .clean_session(true)
                .keep_alive(keep_live_duration_secs)
                .will_msg("/last_will".to_string(), "good bye".to_string(), 0, false)
                .build();

        let connect_packet = yedmq_mqtt::MqttPacketV3::Connect(connect_packet);
        publisher.write(&connect_packet.to_bytes()).await.unwrap();
        publisher.flush().await.unwrap();

        // ensure connect succeed
        let mut buf = Vec::new();
        let read_bytes = publisher.read_buf(&mut buf).await.unwrap();
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
        //

        // ensure subscribe connect
        let sleep_duration = time::Duration::from_millis(1000);
        tokio::time::sleep(sleep_duration).await;
        //

        publisher.shutdown().await.unwrap();
    });

    let sub_will_join = actix::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await; // wait subscriber
        let mut subscriber = tokio::net::TcpStream::connect(connect_address_cloned)
            .await
            .unwrap();
        let connect_packet =
            yedmq_mqtt::v3::connect::ConnectPacketBuilder::new("test_sub".to_string())
                .clean_session(true)
                .keep_alive(keep_live_duration_secs)
                .build();

        let connect_packet = yedmq_mqtt::MqttPacketV3::Connect(connect_packet);
        subscriber.write(&connect_packet.to_bytes()).await.unwrap();
        subscriber.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
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

        // subscribe
        let subscribe_packet = yedmq_mqtt::v3::subscribe::SubscribePacketBuilder::new(0x10)
            .add_topic_filter(TopicFilter {
                topic_name: "/last_will".to_string(),
                qos: 0,
            })
            .build();

        let subscribe_packet = yedmq_mqtt::MqttPacketV3::Subscribe(subscribe_packet);

        subscriber
            .write(&subscribe_packet.to_bytes())
            .await
            .unwrap();
        subscriber.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = yedmq_mqtt::parse(&buf, yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE)
                .unwrap()
                .1
                 .1;
            match packet {
                MqttPacketV3::Suback(suback_packet) => {
                    assert_eq!(suback_packet.variable_header.packet_identifier, 0x10);
                }
                _ => assert!(false),
            }
        }
        //

        // wait publish message
        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = yedmq_mqtt::parse(&buf, yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE)
                .unwrap()
                .1
                 .1;
            match packet {
                MqttPacketV3::Publish(publish_packet) => {
                    assert_eq!(
                        std::str::from_utf8(&publish_packet.payload.payload),
                        Ok("good bye")
                    );
                }
                _ => assert!(false),
            }
        }
        //
    });

    unexpect_disconnect_join.await.unwrap();
    sub_will_join.await.unwrap();
}
