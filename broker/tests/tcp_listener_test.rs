use std::{thread, time};
use std::{path::PathBuf, sync::Arc, collections::HashMap, time::Duration};
use samoye::plugin_manager::PluginManager;
use tokio::io::{AsyncWriteExt, AsyncReadExt};

use samoye::{listener::tcp_listener::MqttTcpListener, plugin_service::service::PluginService, router::Router, session::{SessionHandle, SessionManager}, settings::Settings, topic::TopicManager};
use samoye_mqtt::{MqttPacketV3, v3::subscribe::TopicFilter};
use tokio::sync::RwLock;

async fn get_test_plugin_manager() -> Arc<PluginManager> {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let plugin_path = PathBuf::from(crate_root_path).join("tests").join("plugins");

    let plugin_service = PluginManager::new(plugin_path.to_str().unwrap().to_string())
        .unwrap();
    Arc::new(plugin_service)
}

fn get_test_settings(qos_expired_secs: u64, resend_duration_sec: u64) -> Settings {
    let settings = Settings {
        session: samoye::settings::Session { qos_expired_secs: qos_expired_secs, packet_resend_interval_secs: resend_duration_sec },
        listener: samoye::settings::Listener { 
            tcp: samoye::settings::Tcp { external: "0.0.0.0:18088".to_string() },
            tcp_tls: samoye::settings::TcpTls {
                external: "0.0.0.0:18089".to_string(),
                cacert_file: "".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string()
            },
            ws: samoye::settings::Ws { external: "0.0.0.0:18090".to_string() },
            wss: samoye::settings::Wss {
                external: "0.0.0.0:18091".to_string(),
                cacert_file: "".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string()
            }
        },
        plugin: samoye::settings::Plugin { dir: "test".to_string() }
    };
    settings
}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
pub async fn test_tcp_listener_connect() {

    let plugin_manager =get_test_plugin_manager().await;

    let keep_live_duration_secs = 5;

    let resend_duration_secs = 10;

    let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

    let settings = get_test_settings(2, resend_duration_secs);

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

    // ensure listener start
    let sleep_duration = time::Duration::from_millis(1000);
    thread::sleep(sleep_duration);
    //

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



#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
pub async fn test_tcp_client_subscribe_and_publish() {

    let plugin_manager =get_test_plugin_manager().await;
    let session_manager = Arc::new(RwLock::new(SessionManager{ session_table:  HashMap::<String,RwLock<HashMap<String, SessionHandle>>>::new()}));
    let topic_manager = Arc::new(RwLock::new(TopicManager::new()));

    let keep_live_duration_secs = 5;

    let resend_duration_secs = 10;

    let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

    let mut router = Router {
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_receiver: router_receiver,
    };

    tokio::spawn(async move {
        router.run().await;
    });

    let settings = get_test_settings(2, resend_duration_secs);

    let listener = MqttTcpListener {
        plugin_manager,
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender,
        settings: Arc::new(settings),
    };

    tokio::spawn(async move {
        listener.run().await.unwrap();
    });

    // ensure listener start
    let sleep_duration = time::Duration::from_millis(1000);
    thread::sleep(sleep_duration);
    //

    // subscriber process
    let sub_join = tokio::spawn(async move {
        let mut subscriber = tokio::net::TcpStream::connect("0.0.0.0:18088").await.unwrap();
        let connect_packet = samoye_mqtt::v3::connect::ConnectPacketBuilder::new("test_sub".to_string())
            .clean_session(true)
            .keep_alive(keep_live_duration_secs)
            .build();

        let connect_packet = samoye_mqtt::MqttPacketV3::Connect(connect_packet);
        subscriber.write(&connect_packet.to_bytes()).await.unwrap();
        subscriber.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
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

        // subscribe
        let subscribe_packet = samoye_mqtt::v3::subscribe::SubscribePacketBuilder::new(0x10).add_topic_filter(TopicFilter{
            topic_name: "/a/b".to_string(),
            qos: 0
        }).build();

        let subscribe_packet = samoye_mqtt::MqttPacketV3::Subscribe(subscribe_packet);

        subscriber.write(&subscribe_packet.to_bytes()).await.unwrap();
        subscriber.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = samoye_mqtt::parse(&buf).unwrap().1.1;
            match packet {
                MqttPacketV3::Suback(suback_packet) => {
                    assert_eq!(suback_packet.variable_header.packet_identifier, 0x10);
                }
                _ => assert!(false)
            }
        }
        //

        // wait publish message
        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = samoye_mqtt::parse(&buf).unwrap().1.1;
            match packet {
                MqttPacketV3::Publish(publish_packet) => {
                    assert_eq!(publish_packet.payload.payload, vec![0x01, 0x02]);
                }
                _ => assert!(false)
            }
        }
        //
    });

    // ensure subscribe start
    let sleep_duration = time::Duration::from_millis(1000);
    thread::sleep(sleep_duration);
    //

    // publisher process
    let pub_join = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await; // wait subscriber
        let mut publisher = tokio::net::TcpStream::connect("0.0.0.0:18088").await.unwrap();
        let connect_packet = samoye_mqtt::v3::connect::ConnectPacketBuilder::new("test_pub".to_string())
            .clean_session(true)
            .keep_alive(keep_live_duration_secs)
            .build();

        let connect_packet = samoye_mqtt::MqttPacketV3::Connect(connect_packet);
        publisher.write(&connect_packet.to_bytes()).await.unwrap();
        publisher.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = publisher.read_buf(&mut buf).await.unwrap();
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

        let publish_packet = samoye_mqtt::v3::publish::PublishPacketBuilder::new("/a/b".to_string(), vec![0x01,0x02]).build();
        let publish_packet = samoye_mqtt::MqttPacketV3::Publish(publish_packet);

        publisher.write(&publish_packet.to_bytes()).await.unwrap();
    });


    sub_join.await.unwrap();
    pub_join.await.unwrap();

}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
pub async fn test_tcp_client_invalid_connect_packet_should_disconnect() {

    let plugin_manager =get_test_plugin_manager().await;
    let session_manager = Arc::new(RwLock::new(SessionManager{ session_table:  HashMap::<String,RwLock<HashMap<String, SessionHandle>>>::new()}));
    let topic_manager = Arc::new(RwLock::new(TopicManager::new()));

    let keep_live_duration_secs = 5;

    let resend_duration_secs = 10;

    let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

    let mut router = Router {
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_receiver: router_receiver,
    };

    tokio::spawn(async move {
        router.run().await;
    });

    let settings = get_test_settings(2, resend_duration_secs);

    let listener = MqttTcpListener {
        plugin_manager,
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender,
        settings: Arc::new(settings),
    };

    tokio::spawn(async move {
        listener.run().await.unwrap();
    });


    // subscriber process
    let invalid_connect_join = tokio::spawn(async move {

        let variable_header = samoye_mqtt::v3::connect::VariableHeader {
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
        let payload = samoye_mqtt::v3::connect::Payload {
            client_identifier: "MQTT".to_string(),
            will_topic: Some("MQTT".to_string()),
            will_message: Some("MQTT".to_string()),
            username: Some("MQTT".to_string()),
            password: Some("MQTT".to_string()),
        };

        let fix_header = samoye_mqtt::v3::fixed_header::FixHeader{
                packet_type: samoye_mqtt::PacketType::CONNECT,
                qos: None,
                retain: None,
                dup: None,
                remaining_length: variable_header.get_length() + payload.get_length(),
            };

        let connect_packet = samoye_mqtt::v3::connect::ConnectPacket {
            fix_header,
            variable_header,
            payload
        };

        let mut invalid_connect = tokio::net::TcpStream::connect("0.0.0.0:18088").await.unwrap();
        let connect_packet = samoye_mqtt::MqttPacketV3::Connect(connect_packet);
        invalid_connect.write(&connect_packet.to_bytes()).await.unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
pub async fn test_when_tcp_client_unexpected_disconnect_broker_should_send_will_message() {

    let plugin_manager =get_test_plugin_manager().await;
    let session_manager = Arc::new(RwLock::new(SessionManager{ session_table:  HashMap::<String,RwLock<HashMap<String, SessionHandle>>>::new()}));
    let topic_manager = Arc::new(RwLock::new(TopicManager::new()));

    let keep_live_duration_secs = 5;

    let resend_duration_secs = 10;

    let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

    let mut router = Router {
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_receiver: router_receiver,
    };

    tokio::spawn(async move {
        router.run().await;
    });

    let settings = get_test_settings(2, resend_duration_secs);

    let listener = MqttTcpListener {
        plugin_manager,
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender,
        settings: Arc::new(settings),
    };

    tokio::spawn(async move {
        listener.run().await.unwrap();
    });

    // ensure listener start
    let sleep_duration = time::Duration::from_millis(1000);
    thread::sleep(sleep_duration);
    //

    let unexpect_disconnect_join = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await; // wait subscriber
        let mut publisher = tokio::net::TcpStream::connect("0.0.0.0:18088").await.unwrap();
        let connect_packet = samoye_mqtt::v3::connect::ConnectPacketBuilder::new("test_pub".to_string())
            .clean_session(true)
            .keep_alive(keep_live_duration_secs)
            .will_msg("/last_will".to_string(), "good bye".to_string(), 0, false)
            .build();

        let connect_packet = samoye_mqtt::MqttPacketV3::Connect(connect_packet);
        publisher.write(&connect_packet.to_bytes()).await.unwrap();
        publisher.flush().await.unwrap();

        // ensure connect succeed
        let mut buf = Vec::new();
        let read_bytes = publisher.read_buf(&mut buf).await.unwrap();
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
        //

        // ensure subscribe connect
        let sleep_duration = time::Duration::from_millis(1000);
        thread::sleep(sleep_duration);
        //

        publisher.shutdown().await.unwrap();
    });

    let sub_will_join = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await; // wait subscriber
        let mut subscriber = tokio::net::TcpStream::connect("0.0.0.0:18088").await.unwrap();
        let connect_packet = samoye_mqtt::v3::connect::ConnectPacketBuilder::new("test_sub".to_string())
            .clean_session(true)
            .keep_alive(keep_live_duration_secs)
            .build();

        let connect_packet = samoye_mqtt::MqttPacketV3::Connect(connect_packet);
        subscriber.write(&connect_packet.to_bytes()).await.unwrap();
        subscriber.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
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

        // subscribe
        let subscribe_packet = samoye_mqtt::v3::subscribe::SubscribePacketBuilder::new(0x10).add_topic_filter(TopicFilter{
            topic_name: "/last_will".to_string(),
            qos: 0
        }).build();

        let subscribe_packet = samoye_mqtt::MqttPacketV3::Subscribe(subscribe_packet);

        subscriber.write(&subscribe_packet.to_bytes()).await.unwrap();
        subscriber.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = samoye_mqtt::parse(&buf).unwrap().1.1;
            match packet {
                MqttPacketV3::Suback(suback_packet) => {
                    assert_eq!(suback_packet.variable_header.packet_identifier, 0x10);
                }
                _ => assert!(false)
            }
        }
        //

        // wait publish message
        let mut buf = Vec::new();
        let read_bytes = subscriber.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = samoye_mqtt::parse(&buf).unwrap().1.1;
            match packet {
                MqttPacketV3::Publish(publish_packet) => {
                    assert_eq!(std::str::from_utf8(&publish_packet.payload.payload), Ok("good bye"));
                }
                _ => assert!(false)
            }
        }
        //
    });

    unexpect_disconnect_join.await.unwrap();
    sub_will_join.await.unwrap();

}