#[allow(dead_code)]
mod common;

use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpStream;
use yedmq::app::YedMQApp;
use yedmq::settings::{AuthConfig, Settings};

struct TestContext {
    _test_dir: TempDir,
    original_dir: PathBuf,
    settings: Arc<Settings>,
    auth_record_file: PathBuf,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let _ = env::set_current_dir(self.original_dir.clone());
    }
}

async fn setup_instance() -> TestContext {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_target(false)
        .format_timestamp(None)
        .is_test(true)
        .try_init();

    let original_dir = env::current_dir().unwrap();
    let temp_dir = TempDir::new().unwrap();
    let auth_record_file = temp_dir.path().join("mqtt5_auth_records.log");

    if Path::new("yedmq.toml").exists() {
        let _ = fs::copy("yedmq.toml", temp_dir.path().join("yedmq.toml"));
    }

    env::set_current_dir(temp_dir.path()).unwrap();

    let test_settings = Arc::new(get_test_settings(temp_dir.path(), &auth_record_file));
    let settings_clone = test_settings.clone();

    std::thread::spawn(move || {
        let rt = actix::System::new();
        rt.block_on(async {
            let app = Arc::new(YedMQApp::new(settings_clone).await.unwrap());
            YedMQApp::start(app.clone()).await;
        });
        rt.run().unwrap();
    });

    tokio::time::sleep(Duration::from_secs(2)).await;

    TestContext {
        settings: test_settings,
        original_dir,
        auth_record_file,
        _test_dir: temp_dir,
    }
}

fn random_tcp_port() -> u16 {
    use rand::Rng;
    rand::thread_rng().gen_range(10240..=65535)
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn ensure_mock_plugin_binary() -> PathBuf {
    let workspace_root = workspace_root();
    let status = Command::new("cargo")
        .args(["build", "-p", "mock_plugin_harness"])
        .current_dir(&workspace_root)
        .status()
        .expect("failed to build mock_plugin_harness");
    assert!(status.success(), "building mock_plugin_harness failed");

    workspace_root
        .join("target")
        .join("debug")
        .join(if cfg!(windows) {
            "mock_plugin_harness.exe"
        } else {
            "mock_plugin_harness"
        })
}

fn prepare_plugin_directory(temp_dir: &Path, auth_record_file: &Path) -> PathBuf {
    let plugin_root = temp_dir.join("plugins");
    let plugin_dir = plugin_root.join("mqtt5_auth_recorder");
    fs::create_dir_all(&plugin_dir).unwrap();

    let binary_path = ensure_mock_plugin_binary();
    let escaped_binary = binary_path.to_string_lossy().replace('\\', "\\\\");
    let config = json!({
        "initialize": {
            "initialization_fail_mode": "none",
            "status": "ready",
            "hooks": [
                {
                    "name": "Authenticate",
                    "priority": 1
                }
            ],
            "initialization_auth_code_mode": "correct",
            "exit_after_init_delay_secs": null
        },
        "authenticate": {
            "authenticated": true,
            "error_reason": null,
            "tenant_id": "public",
            "continue_chain": false,
            "delay_secs": null,
            "record_file": auth_record_file.to_string_lossy()
        }
    });
    let config_arg = serde_json::to_string(&config).unwrap();
    let manifest = format!(
        r#"
[plugin]
name = "mqtt5_auth_recorder"
version = "0.1.0"
description = "MQTT 5 auth recording test plugin"
author = "YedMQ"

[runtime]
type = "process"
executable = "{binary_path}"
args = ["--config", {config_arg:?}]
working_dir = "."
timeout_secs = 12
"#,
        binary_path = escaped_binary,
        config_arg = config_arg,
    );

    fs::write(plugin_dir.join("plugin.toml"), manifest).unwrap();
    plugin_root
}

fn get_test_settings(temp_dir: &Path, auth_record_file: &Path) -> Settings {
    fs::create_dir_all(temp_dir).unwrap();
    let test_temp_store_dir = temp_dir.to_str().unwrap().to_string();
    let plugin_path = prepare_plugin_directory(temp_dir, auth_record_file);

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
                auth: AuthConfig { users: vec![] },
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
                rpc_address: format!("127.0.0.1:{}", rpc_port),
                api_address: format!("127.0.0.1:{}", api_port),
            }],
            session_ttl: 10,
            startup_mode: yedmq::settings::ClusterStartupMode::Bootstrap,
        },
    }
}

fn tcp_broker_addr(context: &TestContext) -> SocketAddr {
    context
        .settings
        .listener
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap()
}

async fn wait_for_auth_record(record_file: &Path) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    loop {
        if let Ok(records) = fs::read_to_string(record_file) {
            if records.contains("protocol_version=5.0") {
                return records;
            }
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for MQTT 5 authenticate record"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[actix::test]
async fn test_mqtt5_connect_authenticate_hook_receives_protocol_and_properties() {
    let context = setup_instance().await;
    let broker_addr = tcp_broker_addr(&context);
    let session_expiry = common::mqtt5::session_expiry_interval_property(45);

    let mut stream = TcpStream::connect(broker_addr).await.unwrap();
    common::mqtt5::write_packet_to_writer(
        &mut stream,
        &common::mqtt5::connect_packet_with_properties(
            "mqtt5-plugin-auth",
            true,
            30,
            &session_expiry,
        ),
    )
    .await;

    let connack = common::mqtt5::read_packet_from_reader(&mut stream).await;
    let connack = common::mqtt5::parse_connack(&connack).expect("parse MQTT 5 CONNACK");
    assert_eq!(connack.reason_code, 0x00);

    let records = wait_for_auth_record(&context.auth_record_file).await;
    assert!(records.contains("client_id=mqtt5-plugin-auth"));
    assert!(records.contains("session_expiry_interval=45"));
    assert!(records.contains("property_keys=session_expiry_interval"));

    common::mqtt5::write_packet_to_writer(&mut stream, &common::mqtt5::disconnect_packet()).await;
}
