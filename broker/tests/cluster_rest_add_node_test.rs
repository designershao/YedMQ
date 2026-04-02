use reqwest::StatusCode;
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::Duration,
};
use tempfile::TempDir;
use yedmq::{
    app::YedMQApp,
    settings::{AuthConfig, ClusterStartupMode, Node, Settings, User},
};

struct NodeHandle {
    api_port: u16,
}

impl NodeHandle {
    fn api_addr(&self) -> String {
        format!("127.0.0.1:{}", self.api_port)
    }
}

fn random_port() -> u16 {
    use rand::Rng;
    rand::thread_rng().gen_range(10240..=65535)
}

fn random_unique_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let p = random_port();
        if used.insert(p) {
            return p;
        }
    }
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

    let settings_clone = settings.clone();
    thread::spawn(move || {
        let boot = std::panic::catch_unwind(|| {
            let rt = actix::System::new();
            rt.block_on(async move {
                let app = Arc::new(YedMQApp::new(settings_clone).await);
                YedMQApp::start(app).await;
            });
            let _ = start_tx.send(Ok(()));
            rt.run().expect("actix runtime run failed");
        });
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

    // We intentionally don't try to join test node threads, consistent with existing cluster tests.
    NodeHandle { api_port }
}

async fn wait_api_ready(client: &reqwest::Client, api_addr: &str) -> bool {
    let url = format!("http://{}/api/v1/system_info", api_addr);
    for _ in 0..30 {
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

fn has_two_nodes(metrics: &Value, raft_key: &str) -> bool {
    let Some(nodes_obj) = metrics
        .get(raft_key)
        .and_then(|v| {
            v.get("membership_config")
                .or_else(|| v.get("membershipConfig"))
        })
        .and_then(|v| {
            v.get("membership")
                .and_then(|m| m.get("nodes"))
                .or_else(|| v.get("nodes"))
        })
        .and_then(|v| v.as_object())
    else {
        return false;
    };
    println!("{} nodes: {:?}", raft_key, nodes_obj.keys());
    nodes_obj.contains_key("1") && nodes_obj.contains_key("2")
}

#[tokio::test]
async fn test_add_second_node_via_rest_api() {
    let temp_dir = TempDir::new().expect("create temp dir failed");

    let mut used_ports = HashSet::new();
    let node1_ports = (
        random_unique_port(&mut used_ports),
        random_unique_port(&mut used_ports),
        random_unique_port(&mut used_ports),
        random_unique_port(&mut used_ports),
        random_unique_port(&mut used_ports),
        random_unique_port(&mut used_ports),
    );
    let node2_ports = (
        random_unique_port(&mut used_ports),
        random_unique_port(&mut used_ports),
        random_unique_port(&mut used_ports),
        random_unique_port(&mut used_ports),
        random_unique_port(&mut used_ports),
        random_unique_port(&mut used_ports),
    );

    let node1 = start_node(
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
    let node1_api = node1.api_addr();
    let node2_api = node2.api_addr();
    let node2_rpc = format!("127.0.0.1:{}", node2_ports.5);

    assert!(
        wait_api_ready(&client, &node1_api).await,
        "node1 api is not ready: {}",
        node1_api
    );
    assert!(
        wait_api_ready(&client, &node2_api).await,
        "node2 api is not ready: {}",
        node2_api
    );

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

    let metrics_url = format!("http://{}/api/v1/cluster/metrics", node1_api);
    let mut converged = false;
    let mut last_metrics: Option<Value> = None;

    for _ in 0..20 {
        let resp = client
            .get(&metrics_url)
            .basic_auth("admin", Some("password"))
            .send()
            .await;
        let Ok(resp) = resp else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        if resp.status() != StatusCode::OK {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        let body: Value = resp.json().await.expect("parse metrics json failed");
        let ok = has_two_nodes(&body, "topic_raft")
            && has_two_nodes(&body, "session_actor_map_raft")
            && has_two_nodes(&body, "session_state_map_raft");

        last_metrics = Some(body);
        if ok {
            converged = true;
            break;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    if let Some(add_err) = add_err {
        panic!("{}", add_err);
    }

    assert!(
        converged,
        "cluster metrics did not show two nodes in all raft groups, last metrics: {}",
        last_metrics.unwrap_or(Value::Null)
    );
}
