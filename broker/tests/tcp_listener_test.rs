use std::{env, fs, net::SocketAddr, path::{Path, PathBuf}, sync::Arc, time::Duration};
use yedmq::app::YedMQApp;
use yedmq::settings::Settings;
use tokio::sync::OnceCell;
use rumqttc::{MqttOptions, AsyncClient, Event, Packet, QoS, LastWill };
use tempfile::TempDir;


static ASYNC_SETUP: OnceCell<TestContext> = OnceCell::const_new();

struct TestContext {
    _test_dir: TempDir,

    original_dir: PathBuf,

    settings: Arc<Settings>
}

impl Drop for TestContext {
    fn drop(&mut self) {
        env::set_current_dir(self.original_dir.clone()).unwrap();
    }
}

async fn setup_instance() -> &'static TestContext {
    ASYNC_SETUP.get_or_init(|| async {
        let original_dir = env::current_dir().unwrap();

        let temp_dir = TempDir::new().unwrap();

        fs::copy("./yedmq.toml", &temp_dir.path().join("yedmq.toml")).unwrap();

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

        TestContext { settings: test_settings, original_dir, _test_dir: temp_dir }
    }).await
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
            qos_expired_secs: qos_expired_secs,
            packet_resend_interval_secs: resend_duration_sec,
            session_clock_path: temp_dir.join("clock").to_str().unwrap().to_string(),
        },
        listener: yedmq::settings::Listener {
            tcp: yedmq::settings::Tcp {
                external: format!("0.0.0.0:{}", tcp_port).to_string(),
                rate_limit: Default::default(),
            },
            tcp_tls: yedmq::settings::TcpTls {
                external: format!("0.0.0.0:{}", tcp_port + 1).to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
            },
            ws: yedmq::settings::Ws {
                external: format!("0.0.0.0:{}", tcp_port + 2).to_string(),
            },
            wss: yedmq::settings::Wss {
                external: format!("0.0.0.0:{}", tcp_port + 3).to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
            },
            api: yedmq::settings::Api {
                external: format!("0.0.0.0:{}", api_port).to_string(),
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
            inflight_retry_interval_secs: 10
        },
        cluster: yedmq::settings::Cluster {
            node_id: 1001,
            cluster_name: "YedMQTest".to_string(),
            heartbeat_interval: 10,
            store_dir: test_temp_store_dir,
            rpc: yedmq::settings::RPC {
                external: format!("0.0.0.0:{}", rpc_port).to_string(),
            },
            nodes: vec![
                yedmq::settings::Node { 
                    id: 1001, 
                    rpc_address: format!("0.0.0.0:{}", rpc_port).to_string(),
                    api_address: format!("0.0.0.0:{}", api_port).to_string(), 
                }
            ],
            session_ttl: 10
        }
    };
    settings
}

#[actix::test]
pub async fn test_tcp_listener_connect() {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let keep_live_duration_secs = 5;

    let broker_addr: SocketAddr = context.settings.listener.tcp.external.as_str().parse().unwrap();

    println!("borker tcp addr {:?}", broker_addr.to_string());


    let mut options = MqttOptions::new(
        "test_client_for_connect",
        broker_addr.ip().to_string(),
        broker_addr.port()
    );
    options.set_keep_alive(std::time::Duration::from_secs(keep_live_duration_secs));

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
    
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        connection_handle
    ).await;
    
    assert!(result.is_ok());
    let connect_result = result.unwrap().unwrap();
    assert!(connect_result.is_ok());
    assert_eq!(connect_result.unwrap(), rumqttc::ConnectReturnCode::Success);   
    
}

async fn test_publish_subscribe(qos: QoS) {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context.settings.listener.tcp.external.as_str().parse().unwrap();

    println!("borker tcp addr {:?}", broker_addr.to_string());

    let mut mqtt_options = MqttOptions::new(format!("test-pubsub-{:?}", qos), broker_addr.ip().to_string(), broker_addr.port());
    mqtt_options.set_keep_alive(Duration::from_secs(5));

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
                    client.publish(topic.clone(), qos, false, payload.clone()).await.unwrap();
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

    tokio::time::timeout(Duration::from_secs(5), task).await.expect("Test timed out").unwrap();
}

#[actix::test]
async fn test_publish_subscribe_qos0() {
    test_publish_subscribe(QoS::AtMostOnce).await;
}

#[actix::test]
async fn test_publish_subscribe_qos1() {
    test_publish_subscribe(QoS::AtLeastOnce).await;
}

#[actix::test]
async fn test_publish_subscribe_qos2() {
    test_publish_subscribe(QoS::ExactlyOnce).await;
}

#[actix::test]
async fn test_retained_message() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context.settings.listener.tcp.external.as_str().parse().unwrap();
    let topic = "test/retained";
    let payload = b"retained message";

    // Client 1 publishes a retained message and disconnects
    let mut mqtt_options1 = MqttOptions::new("retained-publisher", broker_addr.ip().to_string(), broker_addr.port());
    mqtt_options1.set_keep_alive(Duration::from_secs(5));
    let (client1, mut eventloop1) = AsyncClient::new(mqtt_options1, 10);

    let task1 = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop1.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client1.publish(topic, QoS::AtLeastOnce, true, payload.to_vec()).await.unwrap();
                }
                Ok(Event::Incoming(Packet::PubAck(_))) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    client1.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return
                    }
                    panic!("Eventloop 1 error: {:?}", e)
                },
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task1).await.expect("Task 1 timed out").unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;


    // Client 2 connects and subscribes, should receive the retained message
    let mut mqtt_options2 = MqttOptions::new("retained-subscriber", broker_addr.ip().to_string(), broker_addr.port());
    mqtt_options2.set_keep_alive(Duration::from_secs(5));
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
                        return
                    }
                    panic!("Eventloop 2 error: {:?}", e)
                },
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task2).await.expect("Task 2 timed out").unwrap();
}

#[actix::test]
async fn test_last_will_message() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context.settings.listener.tcp.external.as_str().parse().unwrap();
    let will_topic = "test/will";
    let will_message = b"client disconnected uncleanly";
    let last_will = LastWill::new(will_topic, will_message, QoS::AtLeastOnce, false);

    // Subscriber client
    let mut sub_options = MqttOptions::new("will-subscriber", broker_addr.ip().to_string(), broker_addr.port());
    sub_options.set_keep_alive(Duration::from_secs(5));
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_options, 10);

    let (notify_sub_succeed_sender,mut notify_sub_succeed_receiver) = tokio::sync::broadcast::channel(1);

    let sub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match sub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    sub_client.subscribe(will_topic, QoS::AtLeastOnce).await.unwrap();
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
                        return
                    }
                    panic!("Subscriber eventloop error: {:?}", e)
                },
                _ => {}
            }
        }
    });

    notify_sub_succeed_receiver.recv().await.unwrap();

    // Client with Last Will, will simulate an unclean disconnect
    let mut will_client_options = MqttOptions::new("will-client", broker_addr.ip().to_string(), broker_addr.port());
    will_client_options.set_keep_alive(Duration::from_secs(2)).set_last_will(last_will);
    let (_will_client, mut will_eventloop) = AsyncClient::new(will_client_options, 10);

    let will_task = tokio::spawn(async move {
        // Connect and then just stop polling, simulating a crash/network failure
        let _ = will_eventloop.poll().await;
    });
    
    // Wait for the will client to connect
    tokio::time::timeout(Duration::from_secs(5), will_task).await.expect("Will client task timed out").unwrap();
    
    // Wait for subscriber to receive the will message
    tokio::time::timeout(Duration::from_secs(10), sub_task).await.expect("Subscriber task timed out").unwrap();
}

#[actix::test]
async fn test_max_qos_subscription() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr: SocketAddr = context.settings.listener.tcp.external.as_str().parse().unwrap();
    println!("broker tcp addr {:?}", broker_addr.to_string());
    let topic = "test/max_qos";

    // Subscriber with QoS 1
    let mut sub_options = MqttOptions::new("max-qos-subscriber", broker_addr.ip().to_string(), broker_addr.port());
    sub_options.set_keep_alive(Duration::from_secs(5));
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_options, 10);

    // Publisher
    let mut pub_options = MqttOptions::new("max-qos-publisher", broker_addr.ip().to_string(), broker_addr.port());
    pub_options.set_keep_alive(Duration::from_secs(5));
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_options, 10);

    let payload = b"message for qos downgrade";

    let (notify_sub_succeed_sender,mut notify_sub_succeed_receiver) = tokio::sync::broadcast::channel(1);

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
                        return
                    }
                    panic!("Subscriber eventloop error: {:?}", e)
                },
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
                    pub_client.publish(topic, QoS::ExactlyOnce, false, payload.to_vec()).await.unwrap();
                    println!("pub succeed")
                }
                Ok(Event::Incoming(Packet::PubComp(_))) => {
                    pub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return
                    }
                    panic!("Publisher eventloop error: {:?}", e)
                },
                _ => {}
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(5), pub_task).await.expect("Publisher task timed out").unwrap();
    tokio::time::timeout(Duration::from_secs(5), sub_task).await.expect("Subscriber task timed out").unwrap();
}

#[actix::test]
async fn test_persistent_session() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context.settings.listener.tcp.external.as_str().parse().unwrap();
    let topic = "test/persistent";
    let payload = b"persistent message";

    // Client 1 (Persistent) connects, subscribes, disconnects
    let mut mqtt_options1 = MqttOptions::new("persistent-client", broker_addr.ip().to_string(), broker_addr.port());
    mqtt_options1.set_keep_alive(Duration::from_secs(5));
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
                    if connected { return }
                    panic!("Eventloop 1 error: {:?}", e)
                },
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task1).await.expect("Task 1 timed out").unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client 2 (Publisher) publishes a message
    let mut mqtt_options_pub = MqttOptions::new("persistent-publisher", broker_addr.ip().to_string(), broker_addr.port());
    mqtt_options_pub.set_keep_alive(Duration::from_secs(5));
    let (client_pub, mut eventloop_pub) = AsyncClient::new(mqtt_options_pub, 10);

    let task_pub = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop_pub.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client_pub.publish(topic, QoS::AtLeastOnce, false, payload.to_vec()).await.unwrap();
                }
                Ok(Event::Incoming(Packet::PubAck(_))) => {
                    client_pub.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected { return }
                    panic!("Publisher eventloop error: {:?}", e)
                },
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task_pub).await.expect("Publisher task timed out").unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client 1 Reconnects
    let mut mqtt_options2 = MqttOptions::new("persistent-client", broker_addr.ip().to_string(), broker_addr.port());
    mqtt_options2.set_keep_alive(Duration::from_secs(5));
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
                    if connected { return }
                    panic!("Eventloop 2 error: {:?}", e)
                },
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task2).await.expect("Task 2 timed out").unwrap();
}
