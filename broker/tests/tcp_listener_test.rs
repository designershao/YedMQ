use std::{env, fs, net::SocketAddr, path::{Path, PathBuf}, sync::Arc, time::Duration};
use yedmq::app::YedMQApp;
use yedmq::settings::Settings;
use tokio::sync::OnceCell;
use rumqttc::{MqttOptions, AsyncClient, Event, Packet };


static ASYNC_SETUP: OnceCell<TestContext> = OnceCell::const_new();

struct TestContext {

    settings: Arc<Settings>
}

async fn setup_instance() -> &'static TestContext {
    ASYNC_SETUP.get_or_init(|| async {
        let test_settings = Arc::new(get_test_settings(2, 10));
        let app = Arc::new(YedMQApp::new(test_settings.clone()).await);

        YedMQApp::start(app.clone()).await;

        TestContext { settings: test_settings }
    }).await
}


fn random_tcp_port() -> u16 {
    use rand::Rng;
    rand::thread_rng().gen_range(1024..=65535)
}

fn get_test_settings(qos_expired_secs: u64, resend_duration_sec: u64) -> Settings {
    // Generate a random temporary directory
    let tmp_dir = env::temp_dir();
    let random_dir = Path::new(&tmp_dir).join(uuid::Uuid::new_v4().to_string());
    fs::create_dir_all(&random_dir).unwrap();
    let test_temp_store_dir = random_dir.to_str().unwrap().to_string();

    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let plugin_path = PathBuf::from(crate_root_path).join("tests").join("plugins");

    let tcp_port = random_tcp_port();

    let rpc_port = random_tcp_port();

    let api_port = random_tcp_port();

    let settings = Settings {
        session: yedmq::settings::Session {
            qos_expired_secs: qos_expired_secs,
            packet_resend_interval_secs: resend_duration_sec,
            session_clock_path: format!("{}/clock", random_dir.to_str().unwrap()),
        },
        listener: yedmq::settings::Listener {
            tcp: yedmq::settings::Tcp {
                external: format!("0.0.0.0:{}", tcp_port).to_string(),
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
            local_socket_path: format!("{}/yedmq_plugin_host.sock", random_dir.to_str().unwrap()),
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

    let mut options = MqttOptions::new(
        "test_client",
        broker_addr.ip().to_string(),
        broker_addr.port()
    );
    options.set_keep_alive(std::time::Duration::from_secs(keep_live_duration_secs));

    let (_, mut eventloop) = AsyncClient::new(options, 10);

    let connection_handle = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    connected = true;
                    return Ok(ack.code);
                }
                Ok(_) => continue,
                Err(e) => {
                    if connected {
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
