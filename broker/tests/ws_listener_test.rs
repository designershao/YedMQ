#[allow(dead_code)]
mod common;

use rumqttc::{AsyncClient, Event, LastWill, MqttOptions, Packet, QoS, Transport};
use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tokio_tungstenite::{client_async, tungstenite::client::IntoClientRequest, WebSocketStream};
use yedmq::app::YedMQApp;
use yedmq::settings::Settings;

static ASYNC_SETUP: OnceCell<TestContext> = OnceCell::const_new();

struct TestContext {
    _test_dir: TempDir,

    original_dir: PathBuf,

    settings: Arc<Settings>,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        env::set_current_dir(self.original_dir.clone()).unwrap();
    }
}

async fn setup_instance() -> &'static TestContext {
    ASYNC_SETUP
        .get_or_init(|| async {
            env_logger::builder()
                .filter_level(log::LevelFilter::Info)
                .format_target(false)
                .format_timestamp(None)
                .init();

            let original_dir = env::current_dir().unwrap();

            let temp_dir = TempDir::new().unwrap();

            fs::copy("./yedmq.toml", temp_dir.path().join("yedmq.toml")).unwrap();

            env::set_current_dir(temp_dir.path()).unwrap();

            let test_settings = Arc::new(get_test_settings(2, 10, temp_dir.path()));

            let settings_clone = test_settings.clone();

            std::thread::spawn(move || {
                let rt = actix::System::new();
                rt.block_on(async {
                    let app = Arc::new(YedMQApp::new(settings_clone).await);

                    YedMQApp::start(app.clone()).await;
                });
                rt.run().unwrap();
            });

            tokio::time::sleep(Duration::from_secs(1)).await;

            TestContext {
                settings: test_settings,
                original_dir,
                _test_dir: temp_dir,
            }
        })
        .await
}

fn random_tcp_port() -> u16 {
    use rand::Rng;
    rand::thread_rng().gen_range(1024..=65535)
}

fn get_test_settings(qos_expired_secs: u64, resend_duration_sec: u64, temp_dir: &Path) -> Settings {
    // Generate a random temporary directory
    fs::create_dir_all(temp_dir).unwrap();
    let test_temp_store_dir = temp_dir.to_str().unwrap().to_string();

    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let plugin_path = PathBuf::from(crate_root_path).join("tests").join("plugins");

    let tcp_port = random_tcp_port();

    let rpc_port = random_tcp_port();

    let api_port = random_tcp_port();

    let settings = Settings {
        session: yedmq::settings::Session {
            qos_expired_secs,
            packet_resend_interval_secs: resend_duration_sec,
            session_clock_path: temp_dir.join("clock").to_str().unwrap().to_string(),
        },
        listener: yedmq::settings::Listener {
            tcp: yedmq::settings::Tcp {
                external: format!("127.0.0.1:{}", tcp_port).to_string(),
                rate_limit: Default::default(),
            },
            tcp_tls: yedmq::settings::TcpTls {
                external: format!("127.0.0.1:{}", tcp_port + 1).to_string(),
                cacert_file: "".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            ws: yedmq::settings::Ws {
                external: format!("127.0.0.1:{}", tcp_port + 2).to_string(),
                rate_limit: Default::default(),
            },
            wss: yedmq::settings::Wss {
                external: format!("127.0.0.1:{}", tcp_port + 3).to_string(),
                cacert_file: "".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            api: yedmq::settings::Api {
                external: format!("127.0.0.1:{}", api_port).to_string(),
                auth: yedmq::settings::AuthConfig { users: vec![] },
            },
        },
        plugin: yedmq::settings::Plugin {
            dir: plugin_path.to_str().unwrap().to_string(),
            local_socket_path: format!("{}/yedmq_plugin_host.sock", temp_dir.to_str().unwrap()),
            default_authorize_result: true,
            default_authenticate_result: true,
        },
        mqtt: yedmq::settings::Mqtt {
            sys_topic_interval_secs: 10,
            max_message_size: yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE,
            default_authentication: yedmq::settings::DefaultAuthenticationValue::Allow,
            default_authorization: yedmq::settings::DefaultAuthorizationValue::Allow,
            inflight_retry_interval_secs: 10,
        },
        cluster: yedmq::settings::Cluster {
            node_id: 1001,
            cluster_name: "YedMQTest".to_string(),
            heartbeat_interval: 10,
            store_dir: test_temp_store_dir,
            rpc: yedmq::settings::RPC {
                external: format!("127.0.0.1:{}", rpc_port).to_string(),
            },
            nodes: vec![yedmq::settings::Node {
                id: 1001,
                rpc_address: format!("127.0.0.1:{}", rpc_port).to_string(),
                api_address: format!("127.0.0.1:{}", api_port).to_string(),
            }],
            session_ttl: 10,
            startup_mode: yedmq::settings::ClusterStartupMode::Bootstrap,
        },
    };
    settings
}

async fn connect_mqtt5_ws_client(
    broker_addr: SocketAddr,
    client_id: &str,
) -> WebSocketStream<TcpStream> {
    let stream = TcpStream::connect(broker_addr).await.unwrap();
    let url = format!("ws://{}:{}/mqtt", broker_addr.ip(), broker_addr.port());
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());
    let (mut stream, _) = client_async(request, stream).await.unwrap();

    common::mqtt5::write_websocket_packet(
        &mut stream,
        common::mqtt5::connect_packet(client_id, true, 5),
    )
    .await;

    let connack = common::mqtt5::read_websocket_packet(&mut stream).await;
    let connack = common::mqtt5::parse_connack(&connack).expect("parse MQTT 5 CONNACK");
    assert_eq!(connack.reason_code, 0x00);
    stream
}

#[actix::test]
pub async fn test_ws_listener_connect() {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let keep_live_duration_secs = 5;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .ws
        .external
        .as_str()
        .parse()
        .unwrap();

    let mut options = MqttOptions::new(
        "test_client_for_connect",
        format!("ws://{}:{}/mqtt", broker_addr.ip(), broker_addr.port()),
        broker_addr.port(),
    );
    options.set_keep_alive(std::time::Duration::from_secs(keep_live_duration_secs));
    options.set_transport(Transport::Ws);

    let (client, mut eventloop) = AsyncClient::new(options, 10);

    let connection_handle = tokio::spawn(async move {
        let mut _connected = false;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    _connected = true;
                    client.disconnect().await.unwrap();
                    return Ok(ack.code);
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    return Ok(rumqttc::ConnectReturnCode::Success);
                }
                Ok(_) => continue,
                Err(e) => {
                    if _connected {
                        break;
                    }
                    return Err(e);
                }
            }
        }
        Ok(rumqttc::ConnectReturnCode::Success)
    });

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), connection_handle).await;

    assert!(result.is_ok());
    let connect_result = result.unwrap().unwrap();
    println!("connect result {:?}", connect_result);
    assert!(connect_result.is_ok());
    assert_eq!(connect_result.unwrap(), rumqttc::ConnectReturnCode::Success);
}

#[actix::test]
pub async fn test_ws_listener_mqtt5_publish_subscribe_smoke() {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .ws
        .external
        .as_str()
        .parse()
        .unwrap();

    let suffix = uuid::Uuid::new_v4();
    let topic = format!("test/mqtt5/ws/{suffix}");
    let payload = b"hello mqtt5 ws";

    let mut subscriber = connect_mqtt5_ws_client(broker_addr, "mqtt5-ws-sub").await;
    common::mqtt5::write_websocket_packet(
        &mut subscriber,
        common::mqtt5::subscribe_packet(1, &topic, 1),
    )
    .await;
    let suback = common::mqtt5::read_websocket_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.reason_codes, vec![0x01]);

    let mut publisher = connect_mqtt5_ws_client(broker_addr, "mqtt5-ws-pub").await;
    common::mqtt5::write_websocket_packet(
        &mut publisher,
        common::mqtt5::publish_packet(&topic, payload, 1, false, Some(7)),
    )
    .await;
    let puback = common::mqtt5::read_websocket_packet(&mut publisher).await;
    let puback = common::mqtt5::parse_puback(&puback).expect("parse MQTT 5 PUBACK");
    assert_eq!(puback.packet_id, 7);
    assert_eq!(puback.reason_code, 0x00);

    let publish = common::mqtt5::read_websocket_packet(&mut subscriber).await;
    let publish = common::mqtt5::parse_publish(&publish).expect("parse MQTT 5 PUBLISH");
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert_eq!(publish.qos, 1);
    let subscriber_packet_id = publish.packet_id.expect("QoS 1 packet id");
    common::mqtt5::write_websocket_packet(
        &mut subscriber,
        common::mqtt5::puback_packet(subscriber_packet_id),
    )
    .await;

    common::mqtt5::write_websocket_packet(&mut subscriber, common::mqtt5::disconnect_packet())
        .await;
    common::mqtt5::write_websocket_packet(&mut publisher, common::mqtt5::disconnect_packet()).await;
}

async fn test_publish_subscribe(qos: QoS) {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .ws
        .external
        .as_str()
        .parse()
        .unwrap();

    println!("borker ws addr {:?}", broker_addr.to_string());

    let mut mqtt_options = MqttOptions::new(
        format!("test-pubsub-{:?}", qos),
        format!("ws://{}:{}/mqtt", broker_addr.ip(), broker_addr.port()),
        broker_addr.port(),
    );
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    mqtt_options.set_transport(Transport::Ws);

    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    let topic = format!("test/{:?}", qos);

    let payload = format!("hello {:?}", qos).into_bytes();

    let task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client.subscribe(topic.clone(), qos).await.unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    client
                        .publish(topic.clone(), qos, false, payload.clone())
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, topic);
                    assert_eq!(publish.payload.as_ref(), payload.as_slice());
                    assert_eq!(publish.qos, qos);
                    client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    return;
                }
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Eventloop error: {:?}", e);
                }
                _ => {}
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("Test timed out")
        .unwrap();
}

#[actix::test]
async fn test_ws_publish_subscribe_qos0() {
    test_publish_subscribe(QoS::AtMostOnce).await;
}

#[actix::test]
async fn test_ws_publish_subscribe_qos1() {
    test_publish_subscribe(QoS::AtLeastOnce).await;
}

#[actix::test]
async fn test_ws_publish_subscribe_qos2() {
    test_publish_subscribe(QoS::ExactlyOnce).await;
}

#[actix::test]
async fn test_ws_retained_message() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .ws
        .external
        .as_str()
        .parse()
        .unwrap();
    let topic = "test/retained";
    let payload = b"retained message";

    // Client 1 publishes a retained message and disconnects
    let mut mqtt_options1 = MqttOptions::new(
        "retained-publisher",
        format!("ws://{}:{}/mqtt", broker_addr.ip(), broker_addr.port()),
        broker_addr.port(),
    );
    mqtt_options1.set_keep_alive(Duration::from_secs(5));
    mqtt_options1.set_transport(Transport::Ws);
    let (client1, mut eventloop1) = AsyncClient::new(mqtt_options1, 10);

    let task1 = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop1.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client1
                        .publish(topic, QoS::AtLeastOnce, true, payload.to_vec())
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::PubAck(_))) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    client1.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Eventloop 1 error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task1)
        .await
        .expect("Task 1 timed out")
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client 2 connects and subscribes, should receive the retained message
    let mut mqtt_options2 = MqttOptions::new(
        "retained-subscriber",
        format!("ws://{}:{}/mqtt", broker_addr.ip(), broker_addr.port()),
        broker_addr.port(),
    );
    mqtt_options2.set_keep_alive(Duration::from_secs(5));
    mqtt_options2.set_transport(Transport::Ws);
    let (client2, mut eventloop2) = AsyncClient::new(mqtt_options2, 10);

    let task2 = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop2.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client2.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, topic);
                    assert_eq!(publish.payload.as_ref(), payload);
                    assert!(publish.retain);
                    client2.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Eventloop 2 error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task2)
        .await
        .expect("Task 2 timed out")
        .unwrap();
}

#[actix::test]
async fn test_ws_last_will_message() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .ws
        .external
        .as_str()
        .parse()
        .unwrap();
    let will_topic = "test/will";
    let will_message = b"client disconnected uncleanly";
    let last_will = LastWill::new(will_topic, will_message, QoS::AtLeastOnce, false);

    // Subscriber client
    let mut sub_options = MqttOptions::new(
        "will-subscriber",
        format!("ws://{}:{}/mqtt", broker_addr.ip(), broker_addr.port()),
        broker_addr.port(),
    );
    sub_options.set_keep_alive(Duration::from_secs(5));
    sub_options.set_transport(Transport::Ws);
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_options, 10);

    let (notify_sub_succeed_sender, mut notify_sub_succeed_receiver) =
        tokio::sync::broadcast::channel(1);

    let sub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match sub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    sub_client
                        .subscribe(will_topic, QoS::AtLeastOnce)
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    notify_sub_succeed_sender.send(()).unwrap();
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, will_topic);
                    assert_eq!(publish.payload.as_ref(), will_message);
                    sub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Subscriber eventloop error: {:?}", e)
                }
                _ => {}
            }
        }
    });

    notify_sub_succeed_receiver.recv().await.unwrap();

    // Client with Last Will, will simulate an unclean disconnect
    let mut will_client_options = MqttOptions::new(
        "will-client",
        format!("ws://{}:{}/mqtt", broker_addr.ip(), broker_addr.port()),
        broker_addr.port(),
    );
    will_client_options
        .set_keep_alive(Duration::from_secs(2))
        .set_last_will(last_will);
    will_client_options.set_transport(Transport::Ws);
    let (_will_client, mut will_eventloop) = AsyncClient::new(will_client_options, 10);

    let will_task = tokio::spawn(async move {
        let mut connected = false;
        while !connected {
            match will_eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                    connected = true;
                }
                Ok(_) => {} // Continue processing other initialization events (such as SubAck, etc.)
                Err(_e) => {
                    return;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    // Wait for the will client to connect
    tokio::time::timeout(Duration::from_secs(6), will_task)
        .await
        .expect("Will client task timed out")
        .unwrap();

    // Wait for subscriber to receive the will message
    tokio::time::timeout(Duration::from_secs(10), sub_task)
        .await
        .expect("Subscriber task timed out")
        .unwrap();
}

#[actix::test]
async fn test_ws_max_qos_subscription() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .ws
        .external
        .as_str()
        .parse()
        .unwrap();
    println!("borker ws addr {:?}", broker_addr.to_string());
    let topic = "test/max_qos";

    // Subscriber with QoS 1
    let mut sub_options = MqttOptions::new(
        "max-qos-subscriber",
        format!("ws://{}:{}/mqtt", broker_addr.ip(), broker_addr.port()),
        broker_addr.port(),
    );
    sub_options.set_keep_alive(Duration::from_secs(5));
    sub_options.set_transport(Transport::Ws);
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_options, 10);

    // Publisher
    let mut pub_options = MqttOptions::new(
        "max-qos-publisher",
        format!("ws://{}:{}/mqtt", broker_addr.ip(), broker_addr.port()),
        broker_addr.port(),
    );
    pub_options.set_keep_alive(Duration::from_secs(5));
    pub_options.set_transport(Transport::Ws);
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_options, 10);

    let payload = b"message for qos downgrade";

    let (notify_sub_succeed_sender, mut notify_sub_succeed_receiver) =
        tokio::sync::broadcast::channel(1);

    let sub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match sub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    sub_client.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    notify_sub_succeed_sender.send(()).unwrap();
                    // Ready
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, topic);
                    assert_eq!(publish.payload.as_ref(), payload);
                    // QoS should be downgraded to the subscriber's max QoS (1)
                    assert_eq!(publish.qos, QoS::AtLeastOnce);
                    sub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Subscriber eventloop error: {:?}", e)
                }
                _ => {}
            }
        }
    });

    notify_sub_succeed_receiver.recv().await.unwrap();

    let pub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match pub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    // Publish with QoS 2
                    pub_client
                        .publish(topic, QoS::ExactlyOnce, false, payload.to_vec())
                        .await
                        .unwrap();
                    println!("pub succeed")
                }
                Ok(Event::Incoming(Packet::PubComp(_))) => {
                    pub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Publisher eventloop error: {:?}", e)
                }
                _ => {}
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(5), pub_task)
        .await
        .expect("Publisher task timed out")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), sub_task)
        .await
        .expect("Subscriber task timed out")
        .unwrap();
}

#[actix::test]
async fn test_persistent_session() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .ws
        .external
        .as_str()
        .parse()
        .unwrap();
    let topic = "test/persistent";
    let payload = b"persistent message";
    let url = format!("ws://{}:{}/mqtt", broker_addr.ip(), broker_addr.port());

    // Client 1 (Persistent) connects, subscribes, disconnects
    let mut mqtt_options1 = MqttOptions::new("persistent-client", url.clone(), broker_addr.port());
    mqtt_options1.set_keep_alive(Duration::from_secs(5));
    mqtt_options1.set_transport(Transport::Ws);
    mqtt_options1.set_clean_session(false);
    let (client1, mut eventloop1) = AsyncClient::new(mqtt_options1, 10);

    let task1 = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop1.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client1.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    client1.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Eventloop 1 error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task1)
        .await
        .expect("Task 1 timed out")
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client 2 (Publisher) publishes a message
    let mut mqtt_options_pub =
        MqttOptions::new("persistent-publisher", url.clone(), broker_addr.port());
    mqtt_options_pub.set_keep_alive(Duration::from_secs(5));
    mqtt_options_pub.set_transport(Transport::Ws);
    let (client_pub, mut eventloop_pub) = AsyncClient::new(mqtt_options_pub, 10);

    let task_pub = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop_pub.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client_pub
                        .publish(topic, QoS::AtLeastOnce, false, payload.to_vec())
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::PubAck(_))) => {
                    client_pub.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Publisher eventloop error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task_pub)
        .await
        .expect("Publisher task timed out")
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client 1 Reconnects
    let mut mqtt_options2 = MqttOptions::new("persistent-client", url.clone(), broker_addr.port());
    mqtt_options2.set_keep_alive(Duration::from_secs(5));
    mqtt_options2.set_transport(Transport::Ws);
    mqtt_options2.set_clean_session(false);
    let (client2, mut eventloop2) = AsyncClient::new(mqtt_options2, 10);

    let task2 = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop2.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    connected = true;
                    assert!(ack.session_present); // Expect session present
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, topic);
                    assert_eq!(publish.payload.as_ref(), payload);
                    client2.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Eventloop 2 error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task2)
        .await
        .expect("Task 2 timed out")
        .unwrap();
}
