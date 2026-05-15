use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

use crate::app::YedMQApp;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatusResponse {
    pub node_id: u64,
    pub version: String,
    pub cluster_name: String,
    pub health: String,
    pub uptime_seconds: u64,
    pub listeners: NodeListeners,
    pub cluster_ready: bool,
    pub role: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeListeners {
    pub tcp: String,
    pub tcp_tls: String,
    pub ws: String,
    pub wss: String,
    pub api: String,
    pub rpc: String,
}

pub async fn status(State(app): State<Arc<YedMQApp>>) -> (StatusCode, Json<NodeStatusResponse>) {
    let ready = super::cluster::build_cluster_ready_response(&app).await;
    let settings = app.settings.clone();
    let node_id = settings.cluster.node_id;
    let response = NodeStatusResponse {
        node_id,
        version: env!("CARGO_PKG_VERSION").to_string(),
        cluster_name: settings.cluster.cluster_name.clone(),
        health: if ready.ready {
            "healthy".to_string()
        } else {
            "degraded".to_string()
        },
        uptime_seconds: app.metric.get_uptime(),
        listeners: NodeListeners {
            tcp: settings.listener.tcp.external.clone(),
            tcp_tls: settings.listener.tcp_tls.external.clone(),
            ws: settings.listener.ws.external.clone(),
            wss: settings.listener.wss.external.clone(),
            api: settings.listener.api.external.clone(),
            rpc: settings.cluster.rpc.external.clone(),
        },
        cluster_ready: ready.ready,
        role: derive_role(node_id, &ready),
    };

    let status = if response.cluster_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(response))
}

fn derive_role(node_id: u64, ready: &super::cluster::ClusterReadyResponse) -> String {
    let leaders = [
        ready.checks.topic_raft.leader_id,
        ready.checks.session_actor_map_raft.leader_id,
        ready.checks.session_state_raft.leader_id,
    ];

    if leaders.iter().any(Option::is_none) {
        return "unknown".to_string();
    }

    let local_leader_count = leaders
        .iter()
        .filter(|leader_id| **leader_id == Some(node_id))
        .count();
    match local_leader_count {
        3 => "leader".to_string(),
        0 => "follower".to_string(),
        _ => "mixed".to_string(),
    }
}
