use reqwest::StatusCode;
use serde_json::{json, Value};
use std::{
    fs,
    net::TcpListener as StdTcpListener,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::Duration,
};
use tempfile::TempDir;
use tokio::sync::oneshot;
use yedmq::{
    app::YedMQApp,
    settings::{AuthConfig, ClusterStartupMode, Node, Settings, User},
};

struct NodeHandle {
    api_port: u16,
    shutdown_tx: Option<oneshot::Sender<()>>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl NodeHandle {
    fn api_addr(&self) -> String {
        format!("127.0.0.1:{}", self.api_port)
    }

    fn shutdown(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(thread_handle) = self.thread_handle.take() {
            let _ = thread_handle.join();
        }
    }
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        self.shutdown();
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

    fn ports(&self) -> (u16, u16, u16, u16, u16, u16) {
        (
            self.tcp.local_addr().unwrap().port(),
            self.tcp_tls.local_addr().unwrap().port(),
            self.ws.local_addr().unwrap().port(),
            self.wss.local_addr().unwrap().port(),
            self.api.local_addr().unwrap().port(),
            self.rpc.local_addr().unwrap().port(),
        )
    }
}

fn reserve_tcp_listener() -> StdTcpListener {
    StdTcpListener::bind("127.0.0.1:0").expect("failed to reserve tcp port")
}

fn build_settings(
    node_id: u64,
    ports: (u16, u16, u16, u16, u16, u16),
    base_dir: &Path,
    auth_users: Vec<User>,
    startup_mode: ClusterStartupMode,
) -> Settings {
    let (tcp, tcp_tls, ws, wss, api, rpc) = ports;

    let node_dir = base_dir.join(format!("node_{}", node_id));
    fs::create_dir_all(&node_dir).expect("create node dir failed");

    let store_dir = node_dir.join("store").to_string_lossy().to_string();
    let session_clock = node_dir.join("clock").to_string_lossy().to_string();

    let crate_root_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let plugin_path = crate_root_path.join("tests").join("plugins");

    Settings {
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
                cacert_file: "".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            ws: yedmq::settings::Ws {
                external: format!("127.0.0.1:{}", ws),
                rate_limit: Default::default(),
            },
            wss: yedmq::settings::Wss {
                external: format!("127.0.0.1:{}", wss),
                cacert_file: "".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            api: yedmq::settings::Api {
                external: format!("127.0.0.1:{}", api),
                auth: AuthConfig { users: auth_users },
            },
        },
        plugin: yedmq::settings::Plugin {
            dir: plugin_path.to_string_lossy().to_string(),
            local_socket_path: format!("{}/yedmq_plugin_host_{}.sock", base_dir.display(), node_id),
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
            cluster_name: "YedMQTestDynamicAddNode".to_string(),
            heartbeat_interval: 200,
            store_dir,
            rpc: yedmq::settings::RPC {
                external: format!("127.0.0.1:{}", rpc),
            },
            nodes: vec![Node {
                id: node_id,
                rpc_address: format!("127.0.0.1:{}", rpc),
                api_address: format!("127.0.0.1:{}", api),
            }],
            session_ttl: 60,
            startup_mode,
        },
    }
}

async fn start_node(
    node_id: u64,
    ports: (u16, u16, u16, u16, u16, u16),
    base_dir: &Path,
    startup_mode: ClusterStartupMode,
) -> NodeHandle {
    let auth_users = vec![User {
        username: "admin".to_string(),
        password: "password".to_string(),
    }];

    let settings = Arc::new(build_settings(
        node_id,
        ports,
        base_dir,
        auth_users,
        startup_mode,
    ));
    let api_port = ports.4;
    let (start_tx, start_rx) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let settings_clone = settings.clone();
    let thread_handle = thread::spawn(move || {
        let boot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let rt = actix::System::new();
            rt.block_on(async move {
                let app: Arc<YedMQApp> = Arc::new(YedMQApp::new(settings_clone).await);
                YedMQApp::start(app.clone()).await;

                let system = actix::System::current();
                actix::spawn(async move {
                    let _ = shutdown_rx.await;
                    let _ = app.shutdown().await;
                    system.stop();
                });
            });
            let _ = start_tx.send(Ok(()));
            rt.run().expect("actix runtime run failed");
        }));
        if let Err(e) = boot {
            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            let _ = start_tx.send(Err(msg));
        }
    });

    let started =
        tokio::task::spawn_blocking(move || start_rx.recv_timeout(Duration::from_secs(10)))
            .await
            .expect("wait start result task failed");
    match started {
        Ok(Ok(())) => {}
        Ok(Err(err)) => panic!("node {} failed to start: {}", node_id, err),
        Err(_) => panic!("node {} did not report startup within timeout", node_id),
    }

    let api_addr = format!("127.0.0.1:{}", api_port);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("build reqwest client failed");
    assert!(
        wait_api_ready(&client, &api_addr).await,
        "node {} api is not ready: {}",
        node_id,
        api_addr
    );

    NodeHandle {
        api_port,
        shutdown_tx: Some(shutdown_tx),
        thread_handle: Some(thread_handle),
    }
}

async fn wait_api_ready(client: &reqwest::Client, api_addr: &str) -> bool {
    let url = format!("http://{}/api/v1/system_info", api_addr);
    for _ in 0..60 {
        let res = client
            .get(&url)
            .basic_auth("admin", Some("password"))
            .send()
            .await;
        if let Ok(res) = res {
            if res.status() == StatusCode::OK {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    false
}

async fn wait_cluster_ready(client: &reqwest::Client, api_addr: &str) -> Result<Value, String> {
    let ready_url = format!("http://{}/api/v1/cluster/ready", api_addr);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut last_error = "cluster readiness endpoint has not returned success yet".to_string();

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(last_error);
        }

        match client
            .get(&ready_url)
            .basic_auth("admin", Some("password"))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                let body: Value = response
                    .json()
                    .await
                    .map_err(|err| format!("parse ready response failed: {}", err))?;
                return Ok(body);
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

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::test]
async fn test_add_second_node_via_rest_api() {
    let temp_dir = TempDir::new().expect("create temp dir failed");

    let node1_reserved_ports = ReservedNodePorts::new();
    let node2_reserved_ports = ReservedNodePorts::new();
    let node1_ports = node1_reserved_ports.ports();
    let node2_ports = node2_reserved_ports.ports();
    drop(node1_reserved_ports);
    drop(node2_reserved_ports);

    let _node1 = start_node(
        1,
        node1_ports,
        temp_dir.path(),
        ClusterStartupMode::Bootstrap,
    )
    .await;
    let node2 = start_node(2, node2_ports, temp_dir.path(), ClusterStartupMode::Join).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("build reqwest client failed");
    let node1_api = _node1.api_addr();
    let node2_api = node2.api_addr();
    let node2_rpc = format!("127.0.0.1:{}", node2_ports.5);

    let add_node_url = format!("http://{}/api/v1/cluster/nodes", node1_api);

    let payload = json!({
        "node_id": 2,
        "node": {
            "rpc_addr": node2_rpc,
            "api_addr": node2_api
        },
        "members": [1, 2]
    });

    let mut add_ok = false;
    let mut last_status = StatusCode::INTERNAL_SERVER_ERROR;
    let mut last_body = String::new();

    for _ in 0..12 {
        let resp = client
            .post(&add_node_url)
            .basic_auth("admin", Some("password"))
            .json(&payload)
            .send()
            .await;
        let Ok(resp) = resp else {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        };

        last_status = resp.status();
        last_body = resp.text().await.unwrap_or_default();
        if last_status == StatusCode::OK {
            add_ok = true;
            break;
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let add_err = if add_ok {
        None
    } else {
        Some(format!(
            "add node failed, last status: {}, body: {}",
            last_status, last_body
        ))
    };

    if let Some(add_err) = add_err {
        panic!("{}", add_err);
    }

    let ready_body = wait_cluster_ready(&client, &node1_api)
        .await
        .unwrap_or_else(|err| panic!("cluster did not become ready after adding node: {}", err));
    let cluster_node_ids = ready_body
        .get("clusterNodeIds")
        .and_then(Value::as_array)
        .cloned()
        .expect("clusterNodeIds should be an array");

    assert_eq!(cluster_node_ids, vec![Value::from(1), Value::from(2)]);
    assert_eq!(
        ready_body
            .pointer("/checks/topicRaft/membershipNodeIds")
            .and_then(Value::as_array)
            .cloned(),
        Some(vec![Value::from(1), Value::from(2)])
    );
    assert_eq!(
        ready_body
            .pointer("/checks/sessionActorMapRaft/membershipNodeIds")
            .and_then(Value::as_array)
            .cloned(),
        Some(vec![Value::from(1), Value::from(2)])
    );
    assert_eq!(
        ready_body
            .pointer("/checks/sessionStateRaft/membershipNodeIds")
            .and_then(Value::as_array)
            .cloned(),
        Some(vec![Value::from(1), Value::from(2)])
    );
}
