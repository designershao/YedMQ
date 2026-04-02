use serde_json::Value;
use std::{
    env, fs, net::TcpListener as StdTcpListener, path::PathBuf, sync::Arc, thread, time::Duration,
};
use tempfile::TempDir;
use tokio::sync::OnceCell;
use yedmq::app::YedMQApp;
use yedmq::settings::{Node, Settings, User};

static ASYNC_SETUP: OnceCell<TestClusterContext> = OnceCell::const_new();
const API_USERNAME: &str = "admin";
const API_PASSWORD: &str = "password";
const CLUSTER_READY_TIMEOUT_SECS: u64 = 60;
const CLUSTER_READY_POLL_INTERVAL_MILLIS: u64 = 500;

pub struct TestClusterContext {
    pub _test_dir: TempDir,
    pub original_dir: PathBuf,
    pub nodes: Vec<Arc<Settings>>,
}

struct ReservedNodePorts {
    tcp: StdTcpListener,
    tcp_tls: StdTcpListener,
    ws: StdTcpListener,
    wss: StdTcpListener,
    api: StdTcpListener,
    rpc: StdTcpListener,
}

impl ReservedNodePorts {
    fn new() -> Self {
        Self {
            tcp: reserve_tcp_listener(),
            tcp_tls: reserve_tcp_listener(),
            ws: reserve_tcp_listener(),
            wss: reserve_tcp_listener(),
            api: reserve_tcp_listener(),
            rpc: reserve_tcp_listener(),
        }
    }

    fn tcp_port(&self) -> u16 {
        self.tcp.local_addr().unwrap().port()
    }

    fn tcp_tls_port(&self) -> u16 {
        self.tcp_tls.local_addr().unwrap().port()
    }

    fn ws_port(&self) -> u16 {
        self.ws.local_addr().unwrap().port()
    }

    fn wss_port(&self) -> u16 {
        self.wss.local_addr().unwrap().port()
    }

    fn api_port(&self) -> u16 {
        self.api.local_addr().unwrap().port()
    }

    fn rpc_port(&self) -> u16 {
        self.rpc.local_addr().unwrap().port()
    }
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

            let node_ids = [1001, 1002, 1003];
            let reserved_ports: Vec<ReservedNodePorts> =
                (0..3).map(|_| ReservedNodePorts::new()).collect();

            // Build the shared nodes list
            for (i, &id) in node_ids.iter().enumerate() {
                cluster_nodes_config.push(Node {
                    id,
                    rpc_address: format!("127.0.0.1:{}", reserved_ports[i].rpc_port()),
                    api_address: format!("127.0.0.1:{}", reserved_ports[i].api_port()),
                });
            }

            // Build node settings first so reserved ports can be released before bind.
            for (i, &id) in node_ids.iter().enumerate() {
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
                            external: format!("127.0.0.1:{}", reserved_ports[i].tcp_port()),
                            rate_limit: Default::default(),
                        },
                        tcp_tls: yedmq::settings::TcpTls {
                            external: format!("127.0.0.1:{}", reserved_ports[i].tcp_tls_port()),
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
                            external: format!("127.0.0.1:{}", reserved_ports[i].ws_port()),
                            rate_limit: Default::default(),
                        },
                        wss: yedmq::settings::Wss {
                            external: format!("127.0.0.1:{}", reserved_ports[i].wss_port()),
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
                            external: format!("127.0.0.1:{}", reserved_ports[i].api_port()),
                            auth: yedmq::settings::AuthConfig {
                                users: vec![User {
                                    username: API_USERNAME.to_string(),
                                    password: API_PASSWORD.to_string(),
                                }],
                            },
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
                            external: format!("127.0.0.1:{}", reserved_ports[i].rpc_port()),
                        },
                        nodes: cluster_nodes_config.clone(),
                        session_ttl: 60,
                        startup_mode: yedmq::settings::ClusterStartupMode::Bootstrap,
                    },
                };

                let settings = Arc::new(settings);
                nodes_settings.push(settings.clone());
            }

            drop(reserved_ports);

            for settings in &nodes_settings {
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

            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .expect("Failed to build readiness HTTP client");
            let api_addrs: Vec<String> = nodes_settings
                .iter()
                .map(|settings| settings.listener.api.external.clone())
                .collect();
            wait_for_cluster_ready(&client, &api_addrs, &node_ids).await;

            TestClusterContext {
                _test_dir: temp_dir,
                original_dir,
                nodes: nodes_settings,
            }
        })
        .await
}

fn reserve_tcp_listener() -> StdTcpListener {
    StdTcpListener::bind("127.0.0.1:0").expect("failed to reserve tcp port")
}

async fn wait_for_cluster_ready(client: &reqwest::Client, api_addrs: &[String], node_ids: &[u64]) {
    for api_addr in api_addrs {
        assert!(
            wait_api_ready(client, api_addr).await,
            "node api is not ready: {}",
            api_addr
        );
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(CLUSTER_READY_TIMEOUT_SECS);
    let mut last_failure = "cluster readiness checks have not succeeded yet".to_string();

    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "cluster did not become ready within {}s: {}",
                CLUSTER_READY_TIMEOUT_SECS, last_failure
            );
        }

        let mut cluster_ready = true;

        for api_addr in api_addrs {
            match fetch_cluster_metrics(client, api_addr).await {
                Ok(metrics) if cluster_metrics_ready(&metrics, node_ids) => {
                    continue;
                }
                Ok(metrics) => {
                    cluster_ready = false;
                    last_failure =
                        format!("cluster metrics not ready on {}: {}", api_addr, metrics);
                    break;
                }
                Err(err) => {
                    cluster_ready = false;
                    last_failure =
                        format!("failed to fetch cluster metrics from {}: {}", api_addr, err);
                    break;
                }
            }
        }

        if cluster_ready {
            return;
        }

        tokio::time::sleep(Duration::from_millis(CLUSTER_READY_POLL_INTERVAL_MILLIS)).await;
    }
}

async fn wait_api_ready(client: &reqwest::Client, api_addr: &str) -> bool {
    let url = format!("http://{}/api/v1/system_info", api_addr);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(CLUSTER_READY_TIMEOUT_SECS);

    loop {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }

        let response = client
            .get(&url)
            .basic_auth(API_USERNAME, Some(API_PASSWORD))
            .send()
            .await;

        if let Ok(response) = response {
            if response.status().is_success() {
                return true;
            }
        }

        tokio::time::sleep(Duration::from_millis(CLUSTER_READY_POLL_INTERVAL_MILLIS)).await;
    }
}

async fn fetch_cluster_metrics(client: &reqwest::Client, api_addr: &str) -> Result<Value, String> {
    let url = format!("http://{}/api/v1/cluster/metrics", api_addr);
    let response = client
        .get(&url)
        .basic_auth(API_USERNAME, Some(API_PASSWORD))
        .send()
        .await
        .map_err(|err| err.to_string())?;

    if !response.status().is_success() {
        return Err(format!("status {}", response.status()));
    }

    response.json().await.map_err(|err| err.to_string())
}

fn cluster_metrics_ready(metrics: &Value, node_ids: &[u64]) -> bool {
    const RAFT_GROUP_KEYS: [&str; 3] = [
        "topic_raft",
        "session_actor_map_raft",
        "session_state_map_raft",
    ];

    RAFT_GROUP_KEYS
        .iter()
        .all(|raft_key| raft_group_ready(metrics, raft_key, node_ids))
}

fn raft_group_ready(metrics: &Value, raft_key: &str, node_ids: &[u64]) -> bool {
    let Some(raft_metrics) = metrics.get(raft_key) else {
        return false;
    };

    let has_leader = raft_metrics
        .get("current_leader")
        .or_else(|| raft_metrics.get("currentLeader"))
        .is_some_and(|leader| !leader.is_null());

    has_leader && membership_contains_all_nodes(raft_metrics, node_ids)
}

fn membership_contains_all_nodes(raft_metrics: &Value, node_ids: &[u64]) -> bool {
    let Some(nodes_obj) = raft_metrics
        .get("membership_config")
        .or_else(|| raft_metrics.get("membershipConfig"))
        .and_then(|membership| {
            membership
                .get("membership")
                .and_then(|inner| inner.get("nodes"))
                .or_else(|| membership.get("nodes"))
        })
        .and_then(|nodes| nodes.as_object())
    else {
        return false;
    };

    node_ids
        .iter()
        .all(|node_id| nodes_obj.contains_key(&node_id.to_string()))
}
