use env_logger;
use rand::Rng;
use std::{env, fs, path::PathBuf, sync::Arc, thread, time::Duration};
use tempfile::TempDir;
use tokio::sync::OnceCell;
use yedmq::app::YedMQApp;
use yedmq::settings::{Node, Settings};

static ASYNC_SETUP: OnceCell<TestClusterContext> = OnceCell::const_new();

pub struct TestClusterContext {
    pub _test_dir: TempDir,
    pub original_dir: PathBuf,
    pub nodes: Vec<Arc<Settings>>,
}

impl Drop for TestClusterContext {
    fn drop(&mut self) {
        let _ = env::set_current_dir(self.original_dir.clone());
    }
}

pub async fn setup_cluster() -> &'static TestClusterContext {
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

            // Try to copy yedmq.toml if it exists in current dir (which should be crate root during test)
            /*
            if Path::new("yedmq.toml").exists() {
                 let _ = fs::copy("yedmq.toml", temp_dir.path().join("yedmq.toml"));
            }
            */

            env::set_current_dir(temp_dir.path()).unwrap();

            let mut nodes_settings = Vec::new();
            let mut cluster_nodes_config = Vec::new();

            let node_ids = vec![1001, 1002, 1003];
            let mut ports = Vec::new();
            for _ in 0..3 {
                ports.push((
                    random_tcp_port(), // tcp
                    random_tcp_port(), // tcp_tls
                    random_tcp_port(), // ws
                    random_tcp_port(), // wss
                    random_tcp_port(), // api
                    random_tcp_port(), // rpc
                ));
            }

            // Build the shared nodes list
            for (i, &id) in node_ids.iter().enumerate() {
                let (_, _, _, _, api_port, rpc_port) = ports[i];
                cluster_nodes_config.push(Node {
                    id,
                    rpc_address: format!("0.0.0.0:{}", rpc_port),
                    api_address: format!("0.0.0.0:{}", api_port),
                });
            }

            // Start nodes
            for (i, &id) in node_ids.iter().enumerate() {
                let (tcp, tcp_tls, ws, wss, api, rpc) = ports[i];

                let node_dir = temp_dir.path().join(format!("node_{}", id));
                fs::create_dir_all(&node_dir).unwrap();
                let store_dir = node_dir.join("store").to_str().unwrap().to_string();
                let session_clock = node_dir.join("clock").to_str().unwrap().to_string();

                let crate_root_path = env!("CARGO_MANIFEST_DIR");
                let plugin_path = PathBuf::from(crate_root_path).join("tests").join("plugins");
                let certs_path = PathBuf::from(crate_root_path).join("tests").join("certs");
                // Ensure certs exist or handle error? tests should have them.
                let absolute_certs_path =
                    fs::canonicalize(&certs_path).expect("Failed to canonicalize certs path");

                let settings = Settings {
                    session: yedmq::settings::Session {
                        qos_expired_secs: 2,
                        packet_resend_interval_secs: 10,
                        session_clock_path: session_clock,
                    },
                    listener: yedmq::settings::Listener {
                        tcp: yedmq::settings::Tcp {
                            external: format!("0.0.0.0:{}", tcp),
                            rate_limit: Default::default(),
                        },
                        tcp_tls: yedmq::settings::TcpTls {
                            external: format!("0.0.0.0:{}", tcp_tls),
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
                            rate_limit: Default::default(),
                        },
                        ws: yedmq::settings::Ws {
                            external: format!("0.0.0.0:{}", ws),
                            rate_limit: Default::default(),
                        },
                        wss: yedmq::settings::Wss {
                            external: format!("0.0.0.0:{}", wss),
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
                            rate_limit: Default::default(),
                        },
                        api: yedmq::settings::Api {
                            external: format!("0.0.0.0:{}", api),
                            auth: yedmq::settings::AuthConfig { users: vec![] },
                        },
                    },
                    plugin: yedmq::settings::Plugin {
                        dir: plugin_path.to_str().unwrap().to_string(),
                        local_socket_path: format!(
                            "{}/yedmq_plugin_host_{}.sock",
                            temp_dir.path().to_str().unwrap(),
                            id
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
                        node_id: id,
                        cluster_name: "YedMQTestCluster".to_string(),
                        heartbeat_interval: 200,
                        store_dir,
                        rpc: yedmq::settings::RPC {
                            external: format!("0.0.0.0:{}", rpc),
                        },
                        nodes: cluster_nodes_config.clone(),
                        session_ttl: 60,
                    },
                };

                let settings = Arc::new(settings);
                nodes_settings.push(settings.clone());

                let settings_clone = settings.clone();

                thread::spawn(move || {
                    let rt = actix::System::new();
                    rt.block_on(async {
                        let app = Arc::new(YedMQApp::new(settings_clone).await);
                        YedMQApp::start(app.clone()).await;
                    });
                    rt.run().unwrap();
                });
            }

            tokio::time::sleep(Duration::from_secs(10)).await;

            TestClusterContext {
                _test_dir: temp_dir,
                original_dir,
                nodes: nodes_settings,
            }
        })
        .await
}

fn random_tcp_port() -> u16 {
    rand::thread_rng().gen_range(10240..=65535)
}
