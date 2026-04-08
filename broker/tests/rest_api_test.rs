use reqwest::StatusCode;
use serde_json::Value;
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tempfile::TempDir;
use tokio::sync::{Mutex, OnceCell};
use yedmq::app::YedMQApp;
use yedmq::settings::{AuthConfig, Settings, User};

static ASYNC_SETUP: OnceCell<TestContext> = OnceCell::const_new();
static PLUGIN_API_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const TEST_PLUGIN_NAME: &str = "mock_plugin_harness";

struct TestContext {
    _test_dir: TempDir,
    original_dir: PathBuf,
    settings: Arc<Settings>,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let _ = env::set_current_dir(self.original_dir.clone());
    }
}

async fn setup_instance() -> &'static TestContext {
    ASYNC_SETUP
        .get_or_init(|| async {
            let _ = env_logger::builder()
                .filter_level(log::LevelFilter::Info)
                .format_target(false)
                .format_timestamp(None)
                .is_test(true)
                .try_init();

            let original_dir = env::current_dir().unwrap();
            let temp_dir = TempDir::new().unwrap();

            // Copy yedmq.toml if it exists, though we might not need it as we override settings
            if Path::new("yedmq.toml").exists() {
                let _ = fs::copy("yedmq.toml", temp_dir.path().join("yedmq.toml"));
            }

            env::set_current_dir(temp_dir.path()).unwrap();

            let test_settings = Arc::new(get_test_settings(temp_dir.path()));
            let settings_clone = test_settings.clone();

            std::thread::spawn(move || {
                let rt = actix::System::new();
                rt.block_on(async {
                    let app = Arc::new(YedMQApp::new(settings_clone).await);
                    YedMQApp::start(app.clone()).await;
                });
                rt.run().unwrap();
            });

            tokio::time::sleep(Duration::from_secs(2)).await;

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
    rand::thread_rng().gen_range(10240..=65535)
}

fn plugin_api_lock() -> &'static Mutex<()> {
    PLUGIN_API_LOCK.get_or_init(|| Mutex::new(()))
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn ensure_mock_plugin_binary() -> PathBuf {
    let workspace_root = workspace_root();
    let binary_path = workspace_root
        .join("target")
        .join("debug")
        .join(if cfg!(windows) {
            "mock_plugin_harness.exe"
        } else {
            "mock_plugin_harness"
        });

    if binary_path.exists() {
        return binary_path;
    }

    let status = Command::new("cargo")
        .args(["build", "-p", "mock_plugin_harness"])
        .current_dir(&workspace_root)
        .status()
        .expect("failed to build mock_plugin_harness");
    assert!(status.success(), "building mock_plugin_harness failed");

    binary_path
}

fn prepare_plugin_directory(temp_dir: &Path) -> PathBuf {
    let plugin_root = temp_dir.join("plugins");
    let plugin_dir = plugin_root.join(TEST_PLUGIN_NAME);
    fs::create_dir_all(&plugin_dir).unwrap();

    let binary_path = ensure_mock_plugin_binary();
    let escaped_binary = binary_path.to_string_lossy().replace('\\', "\\\\");
    let manifest = format!(
        r#"
[plugin]
name = "{plugin_name}"
version = "0.1.0"
description = "REST API test plugin"
author = "YedMQ"

[runtime]
type = "process"
executable = "{binary_path}"
working_dir = "."
"#,
        plugin_name = TEST_PLUGIN_NAME,
        binary_path = escaped_binary,
    );

    fs::write(plugin_dir.join("plugin.toml"), manifest).unwrap();
    plugin_root
}

fn get_test_settings(temp_dir: &Path) -> Settings {
    fs::create_dir_all(temp_dir).unwrap();
    let test_temp_store_dir = temp_dir.to_str().unwrap().to_string();
    let plugin_path = prepare_plugin_directory(temp_dir);

    let tcp_port = random_tcp_port();
    let rpc_port = random_tcp_port();
    let api_port = random_tcp_port();

    Settings {
        session: yedmq::settings::Session {
            qos_expired_secs: 2,
            packet_resend_interval_secs: 10,
            session_clock_path: temp_dir.join("clock").to_str().unwrap().to_string(),
        },
        listener: yedmq::settings::Listener {
            tcp: yedmq::settings::Tcp {
                external: format!("127.0.0.1:{}", tcp_port),
                rate_limit: Default::default(),
            },
            tcp_tls: yedmq::settings::TcpTls {
                external: format!("127.0.0.1:{}", tcp_port + 1),
                cacert_file: "".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            ws: yedmq::settings::Ws {
                external: format!("127.0.0.1:{}", tcp_port + 2),
                rate_limit: Default::default(),
            },
            wss: yedmq::settings::Wss {
                external: format!("127.0.0.1:{}", tcp_port + 3),
                cacert_file: "".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            api: yedmq::settings::Api {
                external: format!("127.0.0.1:{}", api_port),
                auth: AuthConfig {
                    users: vec![User {
                        username: "admin".to_string(),
                        password: "password".to_string(),
                    }],
                },
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
            max_message_size: 1024 * 1024,
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
                external: format!("127.0.0.1:{}", rpc_port),
            },
            nodes: vec![yedmq::settings::Node {
                id: 1001,
                rpc_address: format!("127.0.0.1:{}", rpc_port).to_string(),
                api_address: format!("127.0.0.1:{}", api_port).to_string(),
            }],
            session_ttl: 10,
            startup_mode: yedmq::settings::ClusterStartupMode::Bootstrap,
        },
    }
}

async fn ensure_cluster_initialized(api_addr: &str) {
    let client = reqwest::Client::new();
    let init_url = format!("http://{}/api/v1/cluster/init", api_addr);
    let _ = client
        .post(&init_url)
        .basic_auth("admin", Some("password"))
        .send()
        .await;
    wait_for_cluster_ready(api_addr).await;
}

async fn wait_for_cluster_ready(api_addr: &str) -> Value {
    let client = reqwest::Client::new();
    let ready_url = format!("http://{}/api/v1/cluster/ready", api_addr);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last_error = "cluster readiness endpoint has not returned success yet".to_string();

    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster did not become ready: {}",
            last_error
        );

        match client
            .get(&ready_url)
            .basic_auth("admin", Some("password"))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                return response
                    .json()
                    .await
                    .expect("parse cluster ready response failed");
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                last_error = format!("status {}, body {}", status, body);
            }
            Err(err) => {
                last_error = err.to_string();
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn get_plugin_detail(api_addr: &str, plugin_name: &str) -> Value {
    let client = reqwest::Client::new();
    let url = format!("http://{}/api/v1/plugins/{}", api_addr, plugin_name);
    let response = client
        .get(&url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.json().await.unwrap()
}

async fn wait_for_plugin_state(api_addr: &str, plugin_name: &str, expected_state: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        let body = get_plugin_detail(api_addr, plugin_name).await;
        if body.get("state").and_then(Value::as_str) == Some(expected_state) {
            return body;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "plugin {} did not reach state {} in time; last body: {}",
            plugin_name,
            expected_state,
            body
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn ensure_plugin_running(api_addr: &str, plugin_name: &str) -> Value {
    let detail = get_plugin_detail(api_addr, plugin_name).await;
    if detail.get("state").and_then(Value::as_str) == Some("running") {
        return detail;
    }

    let client = reqwest::Client::new();
    let url = format!("http://{}/api/v1/plugins/{}/start", api_addr, plugin_name);
    let response = client
        .post(&url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success() || response.status() == StatusCode::CONFLICT,
        "unexpected status when starting plugin: {}",
        response.status()
    );

    wait_for_plugin_state(api_addr, plugin_name, "running").await
}

#[actix::test]
async fn test_api_system_info() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let client = reqwest::Client::new();
    let api_addr = &context.settings.listener.api.external;
    let url = format!("http://{}/api/v1/system_info", api_addr);

    let resp = client
        .get(&url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    // Fields are camelCase: clientsConnected, bytesReceived, bytesSent, uptime
    assert!(body.get("clientsConnected").is_some());
    assert!(body.get("uptime").is_some());
}

#[actix::test]
async fn test_api_cluster_ready() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let api_addr = &context.settings.listener.api.external;

    let body = wait_for_cluster_ready(api_addr).await;
    let cluster_node_ids = body
        .get("clusterNodeIds")
        .and_then(Value::as_array)
        .cloned()
        .expect("clusterNodeIds should be an array");

    assert_eq!(body.get("ready").and_then(Value::as_bool), Some(true));
    assert_eq!(body.get("nodeId").and_then(Value::as_u64), Some(1001));
    assert_eq!(cluster_node_ids, vec![Value::from(1001)]);
    assert_eq!(
        body.pointer("/checks/topicRaft/ready")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        body.pointer("/checks/sessionActorMapRaft/ready")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        body.pointer("/checks/sessionStateRaft/ready")
            .and_then(Value::as_bool),
        Some(true)
    );
}

#[actix::test]
async fn test_api_unauthorized() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let client = reqwest::Client::new();
    let api_addr = &context.settings.listener.api.external;
    let url = format!("http://{}/api/v1/system_info", api_addr);

    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = client
        .get(&url)
        .basic_auth("admin", Some("wrong_password"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[actix::test]
async fn test_api_plugins_list() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let _ = ensure_plugin_running(&context.settings.listener.api.external, TEST_PLUGIN_NAME).await;
    let client = reqwest::Client::new();
    let api_addr = &context.settings.listener.api.external;
    let url = format!("http://{}/api/v1/plugins?scope=all", api_addr);

    let resp = client
        .get(&url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert!(body.get("data").is_some());
    assert!(body.get("data").unwrap().is_array());
    let data = body.get("data").unwrap().as_array().unwrap();
    assert!(data
        .iter()
        .any(|item| item.get("name").and_then(Value::as_str) == Some(TEST_PLUGIN_NAME)));
}

#[actix::test]
async fn test_api_plugin_detail_and_logs() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let api_addr = &context.settings.listener.api.external;

    let detail = ensure_plugin_running(api_addr, TEST_PLUGIN_NAME).await;
    assert_eq!(
        detail.get("name").and_then(Value::as_str),
        Some(TEST_PLUGIN_NAME)
    );
    assert_eq!(detail.get("managed").and_then(Value::as_bool), Some(true));
    assert_eq!(
        detail.get("discovered").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        detail.get("runtimeType").and_then(Value::as_str),
        Some("process")
    );
    assert!(detail.get("hookCount").is_some());

    let client = reqwest::Client::new();
    let logs_url = format!(
        "http://{}/api/v1/plugins/{}/logs?tail=50",
        api_addr, TEST_PLUGIN_NAME
    );
    let resp = client
        .get(&logs_url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let data = body.get("data").unwrap().as_array().unwrap();
    assert!(!data.is_empty());
}

#[actix::test]
async fn test_api_plugin_rescan() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let client = reqwest::Client::new();
    let api_addr = &context.settings.listener.api.external;
    let url = format!("http://{}/api/v1/plugins/rescan", api_addr);

    let resp = client
        .post(&url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let discovered = body.get("discovered").unwrap().as_array().unwrap();
    assert!(discovered
        .iter()
        .any(|item| item.as_str() == Some(TEST_PLUGIN_NAME)));
}

#[actix::test]
async fn test_api_plugin_lifecycle() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let client = reqwest::Client::new();
    let api_addr = &context.settings.listener.api.external;

    let _ = ensure_plugin_running(api_addr, TEST_PLUGIN_NAME).await;

    let stop_url = format!(
        "http://{}/api/v1/plugins/{}/stop",
        api_addr, TEST_PLUGIN_NAME
    );
    let stop_resp = client
        .post(&stop_url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();
    assert_eq!(stop_resp.status(), StatusCode::OK);
    let stop_body: Value = stop_resp.json().await.unwrap();
    assert_eq!(
        stop_body.get("state").and_then(Value::as_str),
        Some("stopped")
    );

    let start_url = format!(
        "http://{}/api/v1/plugins/{}/start",
        api_addr, TEST_PLUGIN_NAME
    );
    let start_resp = client
        .post(&start_url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();
    assert_eq!(start_resp.status(), StatusCode::OK);
    let start_body: Value = start_resp.json().await.unwrap();
    assert!(matches!(
        start_body.get("state").and_then(Value::as_str),
        Some("starting") | Some("running")
    ));
    let running_body = wait_for_plugin_state(api_addr, TEST_PLUGIN_NAME, "running").await;
    assert!(running_body
        .get("restartCount")
        .and_then(Value::as_u64)
        .is_some());

    let restart_url = format!(
        "http://{}/api/v1/plugins/{}/restart",
        api_addr, TEST_PLUGIN_NAME
    );
    let restart_resp = client
        .post(&restart_url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();
    assert_eq!(restart_resp.status(), StatusCode::OK);
    let restart_body: Value = restart_resp.json().await.unwrap();
    assert!(matches!(
        restart_body.get("state").and_then(Value::as_str),
        Some("starting") | Some("running")
    ));

    let final_body = wait_for_plugin_state(api_addr, TEST_PLUGIN_NAME, "running").await;
    assert!(
        final_body
            .get("restartCount")
            .and_then(Value::as_u64)
            .unwrap()
            >= 1
    );
}

#[actix::test]
async fn test_api_clients_list() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let api_addr = &context.settings.listener.api.external;

    ensure_cluster_initialized(api_addr).await;

    // Warm up: connect an MQTT client to ensure 'public' tenant exists
    let broker_tcp_addr = &context.settings.listener.tcp.external;
    let port = broker_tcp_addr
        .split(':')
        .next_back()
        .unwrap()
        .parse()
        .unwrap();
    let mut mqtt_options = rumqttc::MqttOptions::new("warmup-client-clients", "127.0.0.1", port);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    let (m_client, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 10);
    tokio::spawn(async move { while (eventloop.poll().await).is_ok() {} });

    // Wait for connection and session registration
    tokio::time::sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();
    // Use 'public' tenant as connection.rs defaults to it
    let url = format!("http://{}/api/v1/public/clients", api_addr);

    let resp = client
        .get(&url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert!(body.get("data").is_some());

    m_client.disconnect().await.unwrap();
}

#[actix::test]
async fn test_api_clients_list_persistent_session() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let api_addr = &context.settings.listener.api.external;

    ensure_cluster_initialized(api_addr).await;

    // Warm up: connect an MQTT client to ensure 'public' tenant exists
    let broker_tcp_addr = &context.settings.listener.tcp.external;
    let port = broker_tcp_addr
        .split(':')
        .next_back()
        .unwrap()
        .parse()
        .unwrap();
    let mut mqtt_options = rumqttc::MqttOptions::new("warmup-client-clients", "127.0.0.1", port);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    mqtt_options.set_clean_session(false);
    let (m_client, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 10);
    tokio::spawn(async move { while (eventloop.poll().await).is_ok() {} });

    // Wait for connection and session registration
    tokio::time::sleep(Duration::from_secs(1)).await;

    m_client.disconnect().await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();
    // Use 'public' tenant as connection.rs defaults to it
    let url = format!("http://{}/api/v1/public/clients", api_addr);

    let resp = client
        .get(&url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert!(body.get("data").is_some());

    let data = body.get("data").unwrap().as_array().unwrap();
    let client_info = data[0].as_object().unwrap();
    println!("Client Info: {:?}", client_info);
    assert_eq!(
        client_info
            .get("clientIdentifier")
            .unwrap()
            .as_str()
            .unwrap(),
        "warmup-client-clients"
    );
    assert!(!client_info.get("connected").unwrap().as_bool().unwrap());
    assert!(client_info.get("disconnectedAt").unwrap().as_f64().unwrap() > 0.0);
}

#[actix::test]
async fn test_api_publish_message_with_plain_payload() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let api_addr = &context.settings.listener.api.external;
    ensure_cluster_initialized(api_addr).await;

    // Connect an MQTT subscriber to receive the message
    let broker_tcp_addr = &context.settings.listener.tcp.external;
    let port = broker_tcp_addr
        .split(':')
        .next_back()
        .unwrap()
        .parse()
        .unwrap();
    let mut mqtt_options = rumqttc::MqttOptions::new("subscriber-api-msg", "127.0.0.1", port);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    let (client_mqtt, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 10);

    let topic = "test/api/publish";
    let expected_payload = b"Hello From API";

    // Channel to signal when message is received
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                    client_mqtt
                        .subscribe(topic, rumqttc::QoS::AtLeastOnce)
                        .await
                        .unwrap();
                }
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                    if p.topic == topic {
                        tx.send(p.payload).await.unwrap();
                        break;
                    }
                }
                Err(_) => break,
                _ => {}
            }
        }
    });

    // Wait for subscription to be established
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Send message via API
    let client_http = reqwest::Client::new();
    let url = format!("http://{}/api/v1/public/messages", api_addr);

    let payload = serde_json::json!({
        "topic": topic,
        "payload": "Hello From API", // "Hello From API" in base64
        "qos": 1,
        "retain": false,
        "payloadEncoding": "plain"
    });

    let resp = client_http
        .post(&url)
        .basic_auth("admin", Some("password"))
        .json(&payload)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // Verify message received
    let received_payload = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("Timeout waiting for message")
        .expect("Channel closed");

    assert_eq!(received_payload, expected_payload.as_ref());
}

#[actix::test]
async fn test_api_publish_message_with_base64_payload() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let api_addr = &context.settings.listener.api.external;
    ensure_cluster_initialized(api_addr).await;

    // Connect an MQTT subscriber to receive the message
    let broker_tcp_addr = &context.settings.listener.tcp.external;
    let port = broker_tcp_addr
        .split(':')
        .next_back()
        .unwrap()
        .parse()
        .unwrap();
    let mut mqtt_options = rumqttc::MqttOptions::new("subscriber-api-msg", "127.0.0.1", port);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    let (client_mqtt, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 10);

    let topic = "test/api/publish";
    let expected_payload = b"Hello From API";

    // Channel to signal when message is received
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                    client_mqtt
                        .subscribe(topic, rumqttc::QoS::AtLeastOnce)
                        .await
                        .unwrap();
                }
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                    if p.topic == topic {
                        tx.send(p.payload).await.unwrap();
                        break;
                    }
                }
                Err(_) => break,
                _ => {}
            }
        }
    });

    // Wait for subscription to be established
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Send message via API
    let client_http = reqwest::Client::new();
    let url = format!("http://{}/api/v1/public/messages", api_addr);

    let payload = serde_json::json!({
        "topic": topic,
        "payload": "SGVsbG8gRnJvbSBBUEk=", // "Hello From API" in base64
        "qos": 1,
        "retain": false,
        "payloadEncoding": "base64"
    });

    let resp = client_http
        .post(&url)
        .basic_auth("admin", Some("password"))
        .json(&payload)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // Verify message received
    let received_payload = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("Timeout waiting for message")
        .expect("Channel closed");

    assert_eq!(received_payload.to_vec(), expected_payload.as_ref());
}

#[actix::test]
async fn test_api_topics_list() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let api_addr = &context.settings.listener.api.external;

    ensure_cluster_initialized(api_addr).await;

    // Warm up: connect and subscribe to ensure 'public' tenant and some topics exist
    let broker_tcp_addr = &context.settings.listener.tcp.external;
    let port = broker_tcp_addr
        .split(':')
        .next_back()
        .unwrap()
        .parse()
        .unwrap();
    let mut mqtt_options = rumqttc::MqttOptions::new("warmup-client-topics", "127.0.0.1", port);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    let (m_client, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 10);
    tokio::spawn(async move { while (eventloop.poll().await).is_ok() {} });

    m_client
        .subscribe("test/topic", rumqttc::QoS::AtLeastOnce)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();
    let url = format!("http://{}/api/v1/public/topics", api_addr);

    let resp = client
        .get(&url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert!(body.get("data").is_some());

    let data = body.get("data").unwrap().as_array().unwrap();
    let found = data.iter().any(|item| {
        item.as_object()
            .unwrap()
            .get("clientId")
            .unwrap()
            .as_str()
            .unwrap()
            == "warmup-client-topics"
    });
    assert!(found, "warmup-client-topics not found in topic list");

    m_client.disconnect().await.unwrap();
}

#[actix::test]
async fn test_api_client_kickoff() {
    let _guard = plugin_api_lock().lock().await;
    let context = setup_instance().await;
    let api_addr = &context.settings.listener.api.external;

    ensure_cluster_initialized(api_addr).await;

    let client_id = "kickoff-test-client";
    let broker_tcp_addr = &context.settings.listener.tcp.external;
    let port = broker_tcp_addr
        .split(':')
        .next_back()
        .unwrap()
        .parse()
        .unwrap();
    let mut mqtt_options = rumqttc::MqttOptions::new(client_id, "127.0.0.1", port);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    let (_m_client, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 10);

    // Channel to detect disconnection
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(event) => {
                    log::info!("Event: {:?}", event);
                }
                Err(e) => {
                    let _ = tx.send(e).await;
                    break;
                }
            }
        }
    });

    // Wait for connection to be established
    tokio::time::sleep(Duration::from_secs(1)).await;

    let client_http = reqwest::Client::new();
    // Path: /api/v1/:tenant_id/clients/:client_id/kickoff
    let url = format!(
        "http://{}/api/v1/public/clients/{}/kickoff",
        api_addr, client_id
    );

    let resp = client_http
        .post(&url)
        .basic_auth("admin", Some("password"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // Verify MQTT client is disconnected
    let disconnect_err = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("Timeout waiting for client disconnection")
        .expect("Channel closed");

    log::info!("Client disconnected as expected: {:?}", disconnect_err);
}
