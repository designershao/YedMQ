use std::{
    fs,
    net::TcpListener as StdTcpListener,
    path::PathBuf,
    sync::{mpsc, Arc, OnceLock},
    thread,
    time::Duration,
};
use tempfile::TempDir;
use tokio::sync::{oneshot, Mutex, OwnedMutexGuard};
use yedmq::app::YedMQApp;
use yedmq::settings::{Node, Settings, User};

const API_USERNAME: &str = "admin";
const API_PASSWORD: &str = "password";
const CLUSTER_READY_TIMEOUT_SECS: u64 = 60;
const CLUSTER_READY_POLL_INTERVAL_MILLIS: u64 = 500;

static CLUSTER_TEST_MUTEX: OnceLock<Arc<Mutex<()>>> = OnceLock::new();

pub struct TestClusterContext {
    _serial_guard: OwnedMutexGuard<()>,
    node_handles: Vec<TestNodeHandle>,
    pub _test_dir: TempDir,
    pub nodes: Vec<Arc<Settings>>,
}

struct StartedCluster {
    node_handles: Vec<TestNodeHandle>,
    temp_dir: TempDir,
    nodes: Vec<Arc<Settings>>,
}

struct TestNodeHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl TestNodeHandle {
    fn shutdown(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(thread_handle) = self.thread_handle.take() {
            let _ = thread_handle.join();
        }
    }
}

impl Drop for TestNodeHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Drop for TestClusterContext {
    fn drop(&mut self) {
        for node_handle in &mut self.node_handles {
            node_handle.shutdown();
        }
    }
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

pub async fn setup_cluster() -> TestClusterContext {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_target(false)
        .format_timestamp(None)
        .is_test(true)
        .try_init();

    let serial_guard = cluster_test_mutex().lock_owned().await;
    let started_cluster = build_cluster_with_retries().await;

    TestClusterContext {
        _serial_guard: serial_guard,
        node_handles: started_cluster.node_handles,
        _test_dir: started_cluster.temp_dir,
        nodes: started_cluster.nodes,
    }
}

fn cluster_test_mutex() -> Arc<Mutex<()>> {
    CLUSTER_TEST_MUTEX
        .get_or_init(|| Arc::new(Mutex::new(())))
        .clone()
}

async fn build_cluster_with_retries() -> StartedCluster {
    const MAX_START_ATTEMPTS: usize = 3;

    let mut last_error = String::new();

    for attempt in 1..=MAX_START_ATTEMPTS {
        match try_build_cluster().await {
            Ok(cluster) => return cluster,
            Err(err) => {
                last_error = err;
                if attempt < MAX_START_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    panic!(
        "failed to start test cluster after {} attempts: {}",
        MAX_START_ATTEMPTS, last_error
    );
}

async fn try_build_cluster() -> Result<StartedCluster, String> {
    let temp_dir = TempDir::new().map_err(|err| err.to_string())?;

    let mut nodes_settings = Vec::new();
    let mut cluster_nodes_config = Vec::new();

    let node_ids = [1001, 1002, 1003];
    let reserved_ports: Vec<ReservedNodePorts> = (0..3).map(|_| ReservedNodePorts::new()).collect();

    for (i, &id) in node_ids.iter().enumerate() {
        cluster_nodes_config.push(Node {
            id,
            rpc_address: format!("127.0.0.1:{}", reserved_ports[i].rpc_port()),
            api_address: format!("127.0.0.1:{}", reserved_ports[i].api_port()),
        });
    }

    for (i, &id) in node_ids.iter().enumerate() {
        let node_dir = temp_dir.path().join(format!("node_{}", id));
        fs::create_dir_all(&node_dir).map_err(|err| err.to_string())?;
        let store_dir = node_dir.join("store").to_str().unwrap().to_string();
        let session_clock = node_dir.join("clock").to_str().unwrap().to_string();

        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests").join("plugins");
        let certs_path = PathBuf::from(crate_root_path).join("tests").join("certs");
        let absolute_certs_path = fs::canonicalize(&certs_path)
            .map_err(|err| format!("failed to canonicalize certs path: {}", err))?;

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

        nodes_settings.push(Arc::new(settings));
    }

    drop(reserved_ports);

    let mut node_handles = Vec::new();
    for settings in &nodes_settings {
        node_handles.push(spawn_test_node(settings.clone())?);
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|err| format!("failed to build readiness HTTP client: {}", err))?;
    let api_addrs: Vec<String> = nodes_settings
        .iter()
        .map(|settings| settings.listener.api.external.clone())
        .collect();
    wait_for_cluster_ready(&client, &api_addrs).await;

    Ok(StartedCluster {
        node_handles,
        temp_dir,
        nodes: nodes_settings,
    })
}

fn spawn_test_node(settings: Arc<Settings>) -> Result<TestNodeHandle, String> {
    let (ready_tx, ready_rx) = mpsc::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let node_id = settings.cluster.node_id;

    let thread_handle = thread::spawn(move || {
        let system = actix::System::new();
        system.block_on(async move {
            let app = Arc::new(YedMQApp::new(settings).await);
            YedMQApp::start(app.clone()).await;

            let system = actix::System::current();
            actix::spawn(async move {
                let _ = shutdown_rx.await;
                let _ = app.shutdown().await;
                system.stop();
            });

            ready_tx
                .send(())
                .expect("failed to notify test node readiness");
        });
        system.run().unwrap();
    });

    let mut node_handle = TestNodeHandle {
        shutdown_tx: Some(shutdown_tx),
        thread_handle: Some(thread_handle),
    };

    match ready_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(()) => Ok(node_handle),
        Err(mpsc::RecvTimeoutError::Timeout) => Err(format!(
            "timed out waiting for test node {} startup",
            node_id
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            node_handle.shutdown();
            Err(format!(
                "test node {} startup thread exited unexpectedly",
                node_id
            ))
        }
    }
}

fn reserve_tcp_listener() -> StdTcpListener {
    StdTcpListener::bind("127.0.0.1:0").expect("failed to reserve tcp port")
}

async fn wait_for_cluster_ready(client: &reqwest::Client, api_addrs: &[String]) {
    for api_addr in api_addrs {
        assert!(
            wait_api_ready(client, api_addr).await,
            "node api is not ready: {}",
            api_addr
        );
    }

    for api_addr in api_addrs {
        match wait_cluster_ready(client, api_addr).await {
            Ok(()) => {}
            Err(err) => {
                panic!(
                    "cluster did not become ready on {} within {}s: {}",
                    api_addr, CLUSTER_READY_TIMEOUT_SECS, err
                );
            }
        }
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

async fn wait_cluster_ready(client: &reqwest::Client, api_addr: &str) -> Result<(), String> {
    let url = format!("http://{}/api/v1/cluster/ready", api_addr);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(CLUSTER_READY_TIMEOUT_SECS);
    let mut last_failure = "cluster readiness endpoint has not returned success yet".to_string();

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(last_failure);
        }

        let response = client
            .get(&url)
            .basic_auth(API_USERNAME, Some(API_PASSWORD))
            .send()
            .await;

        match response {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                last_failure = format!("status {}, body {}", status, body);
            }
            Err(err) => {
                last_failure = err.to_string();
            }
        }

        tokio::time::sleep(Duration::from_millis(CLUSTER_READY_POLL_INTERVAL_MILLIS)).await;
    }
}
