use rand::Rng;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use std::{env, fs, path::PathBuf, sync::Arc, thread, time::Duration};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use yedmq::{
    app::YedMQApp,
    settings::{Node, Settings},
};

const SESSION_EXPIRY_SWEEP_INTERVAL_SECS: u64 = 30;

async fn wait_for_broker_ready(port: u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let address = format!("127.0.0.1:{port}");

    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("broker tcp listener did not become ready on {}", address);
        }

        match tokio::net::TcpStream::connect(&address).await {
            Ok(stream) => {
                drop(stream);
                return;
            }
            Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
}

async fn wait_for_connect(eventloop: &mut EventLoop) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => return,
            Ok(_) => continue,
            Err(e) => panic!("connection failed: {:?}", e),
        }
    }
}

async fn wait_for_suback(eventloop: &mut EventLoop) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::SubAck(_))) => return,
            Ok(_) => continue,
            Err(e) => panic!("subscribe failed: {:?}", e),
        }
    }
}

async fn wait_for_publish(
    eventloop: &mut EventLoop,
    expected_topic: &str,
    expected_payload: &[u8],
    timeout_duration: Duration,
) -> bool {
    timeout(timeout_duration, async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, expected_topic);
                    assert_eq!(publish.payload.as_ref(), expected_payload);
                    return true;
                }
                Ok(_) => continue,
                Err(e) => panic!("waiting for publish failed: {:?}", e),
            }
        }
    })
    .await
    .is_ok()
}

async fn subscribe_and_disconnect(mqtt_options: MqttOptions, topic: &str) {
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
    wait_for_connect(&mut eventloop).await;
    client.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    wait_for_suback(&mut eventloop).await;
    client.disconnect().await.unwrap();
}

async fn publish_message(host: &str, port: u16, client_id: &str, topic: &str, payload: &[u8]) {
    let mqtt_options = MqttOptions::new(client_id, host, port);
    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
    wait_for_connect(&mut eventloop).await;
    client
        .publish(topic, QoS::AtLeastOnce, false, payload.to_vec())
        .await
        .unwrap();

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::PubAck(_))) => break,
            Ok(_) => continue,
            Err(e) => panic!("publish failed: {:?}", e),
        }
    }

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_persistent_session_expiry() {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = TempDir::new().unwrap();
    let node = start_node(1, None, temp_dir.path(), 10).await;
    let topic = "test/expiry";

    let mut mqtt_options = MqttOptions::new("expiry_test_client", "127.0.0.1", node.tcp_port);
    mqtt_options.set_clean_session(false);
    mqtt_options.set_keep_alive(Duration::from_secs(5));

    subscribe_and_disconnect(mqtt_options.clone(), topic).await;

    // The expiry sweep runs every 30s. With TTL=10s and a fresh broker start,
    // one sweep plus a small buffer is enough to observe session cleanup.
    sleep(Duration::from_secs(SESSION_EXPIRY_SWEEP_INTERVAL_SECS + 5)).await;

    let (client, mut eventloop) = AsyncClient::new(mqtt_options.clone(), 10);
    wait_for_connect(&mut eventloop).await;
    client
        .subscribe("test/expiry/check", QoS::AtLeastOnce)
        .await
        .unwrap();
    wait_for_suback(&mut eventloop).await;

    client.disconnect().await.unwrap();
    node.stop().await;
}

#[tokio::test]
async fn test_persistent_session_no_expiry_on_reconnect() {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = TempDir::new().unwrap();
    let session_ttl_secs = 20;
    let node = start_node(1, None, temp_dir.path(), session_ttl_secs).await;
    let topic = "test/no_expiry";
    let payload = b"persistent-session-still-active";

    let mut mqtt_options = MqttOptions::new("no_expiry_test_client", "127.0.0.1", node.tcp_port);
    mqtt_options.set_clean_session(false);
    mqtt_options.set_keep_alive(Duration::from_secs(5));

    subscribe_and_disconnect(mqtt_options.clone(), topic).await;

    sleep(Duration::from_secs(15)).await;

    {
        let (client, mut eventloop) = AsyncClient::new(mqtt_options.clone(), 10);
        wait_for_connect(&mut eventloop).await;
        client.disconnect().await.unwrap();
    }

    // This crosses the original expiry deadline and the first cleanup sweep,
    // but stays within the renewed TTL window after reconnect.
    sleep(Duration::from_secs(16)).await;

    let (client, mut eventloop) = AsyncClient::new(mqtt_options.clone(), 10);
    wait_for_connect(&mut eventloop).await;

    publish_message(
        "127.0.0.1",
        node.tcp_port,
        "no_expiry_test_publisher",
        topic,
        payload,
    )
    .await;

    let received = wait_for_publish(&mut eventloop, topic, payload, Duration::from_secs(3)).await;
    assert!(
        received,
        "reconnecting before expiry should preserve the persistent session subscription"
    );

    client.disconnect().await.unwrap();
    node.stop().await;
}

fn random_port() -> u16 {
    rand::thread_rng().gen_range(10240..=65535)
}

struct NodeHandle {
    stop_tx: mpsc::Sender<()>,
    join_handle: Option<thread::JoinHandle<()>>,
    pub tcp_port: u16,
}

impl NodeHandle {
    async fn stop(mut self) {
        let _ = self.stop_tx.send(()).await;
        if let Some(handle) = self.join_handle.take() {
            tokio::task::spawn_blocking(move || {
                handle.join().unwrap();
            })
            .await
            .unwrap();
        }
    }
}

async fn start_node(
    node_id: u64,
    ports: Option<(u16, u16, u16, u16, u16, u16)>,
    base_dir: &std::path::Path,
    session_ttl: u64,
) -> NodeHandle {
    let (tcp, tcp_tls, ws, wss, api, rpc) = ports.unwrap_or_else(|| {
        (
            random_port(),
            random_port(),
            random_port(),
            random_port(),
            random_port(),
            random_port(),
        )
    });

    let node_dir = base_dir.join(format!("node_{}", node_id));
    fs::create_dir_all(&node_dir).unwrap();
    let store_dir = node_dir.join("store").to_str().unwrap().to_string();
    let session_clock = node_dir.join("clock").to_str().unwrap().to_string();

    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let plugin_path = PathBuf::from(crate_root_path).join("tests").join("plugins");
    let certs_path = PathBuf::from(crate_root_path).join("tests").join("certs");
    let absolute_certs_path = fs::canonicalize(&certs_path).unwrap();

    let cluster_node_config = Node {
        id: node_id,
        rpc_address: format!("127.0.0.1:{}", rpc),
        api_address: format!("127.0.0.1:{}", api),
    };

    let settings = Settings {
        session: yedmq::settings::Session {
            qos_expired_secs: 2,
            packet_resend_interval_secs: 10,
            session_clock_path: session_clock,
        },
        listener: yedmq::settings::Listener {
            tcp: yedmq::settings::Tcp {
                external: format!("127.0.0.1:{}", tcp),
                rate_limit: Default::default(),
            },
            tcp_tls: yedmq::settings::TcpTls {
                external: format!("127.0.0.1:{}", tcp_tls),
                cacert_file: absolute_certs_path
                    .join("ca.crt")
                    .to_str()
                    .unwrap()
                    .to_string(),
                cert_file: absolute_certs_path
                    .join("server.crt")
                    .to_str()
                    .unwrap()
                    .to_string(),
                key_file: absolute_certs_path
                    .join("server.key")
                    .to_str()
                    .unwrap()
                    .to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            ws: yedmq::settings::Ws {
                external: format!("127.0.0.1:{}", ws),
                rate_limit: Default::default(),
            },
            wss: yedmq::settings::Wss {
                external: format!("127.0.0.1:{}", wss),
                cacert_file: absolute_certs_path
                    .join("ca.crt")
                    .to_str()
                    .unwrap()
                    .to_string(),
                cert_file: absolute_certs_path
                    .join("server.crt")
                    .to_str()
                    .unwrap()
                    .to_string(),
                key_file: absolute_certs_path
                    .join("server.key")
                    .to_str()
                    .unwrap()
                    .to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            api: yedmq::settings::Api {
                external: format!("127.0.0.1:{}", api),
                auth: yedmq::settings::AuthConfig { users: vec![] },
            },
        },
        plugin: yedmq::settings::Plugin {
            dir: plugin_path.to_str().unwrap().to_string(),
            local_socket_path: format!(
                "{}/yedmq_plugin_host_{}.sock",
                base_dir.to_str().unwrap(),
                node_id
            ),
            default_authorize_result: true,
            default_authenticate_result: true,
        },
        mqtt: yedmq::settings::Mqtt {
            sys_topic_interval_secs: 10,
            max_message_size: 1024 * 1024,
            default_authentication: yedmq::settings::DefaultAuthenticationValue::Allow,
            default_authorization: yedmq::settings::DefaultAuthorizationValue::Allow,
            inflight_retry_interval_secs: 10,
        },
        cluster: yedmq::settings::Cluster {
            node_id,
            cluster_name: "YedMQTestCluster".to_string(),
            heartbeat_interval: 200,
            store_dir,
            rpc: yedmq::settings::RPC {
                external: format!("127.0.0.1:{}", rpc),
            },
            nodes: vec![cluster_node_config],
            session_ttl,
            startup_mode: yedmq::settings::ClusterStartupMode::Bootstrap,
        },
    };

    let (stop_tx, mut stop_rx) = mpsc::channel(1);
    let (start_tx, start_rx) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);
    let settings = Arc::new(settings);
    let settings_clone = settings.clone();

    let join_handle = thread::spawn(move || {
        let boot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let rt = actix::System::new();
            rt.block_on(async {
                let app = Arc::new(YedMQApp::new(settings_clone).await.unwrap());
                YedMQApp::start(app.clone()).await;

                let app_clone = app.clone();
                actix::spawn(async move {
                    stop_rx.recv().await;
                    if let Err(e) = app_clone.shutdown().await {
                        log::warn!("app shutdown failed: {}", e);
                    }
                    actix::System::current().stop();
                });
            });
            let _ = start_tx.send(Ok(()));
            rt.run().unwrap();
        }));

        if let Err(err) = boot {
            let message = if let Some(s) = err.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = err.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            let _ = start_tx.send(Err(message));
        }
    });

    let started =
        tokio::task::spawn_blocking(move || start_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .unwrap();
    match started {
        Ok(Ok(())) => {}
        Ok(Err(err)) => panic!("node {} failed to start: {}", node_id, err),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("node {} did not report startup within timeout", node_id)
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("node {} startup thread exited unexpectedly", node_id)
        }
    }

    wait_for_broker_ready(tcp).await;

    NodeHandle {
        stop_tx,
        join_handle: Some(join_handle),
        tcp_port: tcp,
    }
}

#[tokio::test]
async fn test_persistent_session_abnormal_cleanup_after_restart() {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = TempDir::new().unwrap();
    let node_id = 1;
    let ports = (
        random_port(),
        random_port(),
        random_port(),
        random_port(),
        random_port(),
        random_port(),
    );

    println!("Starting broker for the first time...");
    let node = start_node(node_id, Some(ports), temp_dir.path(), 10).await;
    let tcp_port = node.tcp_port;

    let client_id = "test_client_restart_bug";
    let mut mqtt_options = MqttOptions::new(client_id, "127.0.0.1", tcp_port);
    mqtt_options.set_clean_session(false);
    mqtt_options.set_keep_alive(Duration::from_secs(5));

    {
        println!("Connecting client (1st time)...");
        let (client, mut eventloop) = AsyncClient::new(mqtt_options.clone(), 10);
        wait_for_connect(&mut eventloop).await;
        client
            .subscribe("test/topic", QoS::AtLeastOnce)
            .await
            .unwrap();
        wait_for_suback(&mut eventloop).await;

        println!("Disconnecting client (1st time)...");
        client.disconnect().await.unwrap();
    }

    println!("Stopping broker...");
    node.stop().await;

    println!("Restarting broker...");
    let _node = start_node(node_id, Some(ports), temp_dir.path(), 10).await;

    println!("Reconnecting client (check)...");
    let (_client, mut eventloop) = AsyncClient::new(mqtt_options.clone(), 10);

    let disconnect_unexpected = timeout(Duration::from_secs(10), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(connack))) => {
                    println!("ConnAck received: {:?}", connack);
                }
                Ok(Event::Incoming(Packet::Disconnect)) => break true,
                Err(e) => {
                    println!("Error in event loop: {:?}", e);
                    break true;
                }
                Ok(_) => {}
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(
        !disconnect_unexpected,
        "Client should not disconnect unexpectedly"
    );
}
