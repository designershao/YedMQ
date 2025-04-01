use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use openraft::{docs::cluster_control::node_lifecycle, RaftMetrics};
use serde::{Deserialize, Serialize};

use crate::{app::YedMQApp, raft::Node};

#[derive(Debug, Serialize)]
pub struct RaftMetricsResponse {
    pub topic_raft: RaftMetrics<u64, Node>,
    pub session_actor_map_raft: RaftMetrics<u64, Node>,
}

#[derive(Debug, Deserialize)]
pub struct AddNodeRequest {
    node_id: u64,
    node: Node,
}

#[derive(Debug, Deserialize)]
pub struct ChangeMembersRequest {
    members: Vec<u64>,
}

pub async fn init_cluster(
    State(app_state): State<Arc<YedMQApp>>,
) -> (StatusCode, String) {

    let _ = app_state.raft_manager.topic_raft().init_cluster().await;
    let _ = app_state.raft_manager.session_actor_map_raft().init_cluster().await;
    let _ = app_state.raft_manager.session_state_raft().init_cluster().await;

    (StatusCode::OK, format!(""))
}

pub async fn add_learner(
    State(app_state): State<Arc<YedMQApp>>,
    Json(payload): Json<AddNodeRequest>,
) -> (StatusCode, String) {
    let node_id= payload.node_id;
    let node = payload.node;
    let res = app_state.raft_manager.topic_raft().raft.add_learner(node_id, node.clone(), true).await;
    let res = app_state.raft_manager.session_actor_map_raft().raft().add_learner(node_id, node.clone(), true).await;
    let res = app_state.raft_manager.session_state_raft().raft.add_learner(node_id, node.clone(), true).await;


    (StatusCode::OK, format!("{:?}", res))
}

pub async fn change_membership(
    State(app_state): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {
    let res = app_state.raft_manager.topic_raft().raft.change_membership(payload.members.clone(), true).await;
    let res = app_state.raft_manager.session_actor_map_raft().raft().change_membership(payload.members.clone(), true).await;
    let res = app_state.raft_manager.session_state_raft().raft.change_membership(payload.members.clone(), true).await;

    (StatusCode::OK, format!(""))
}


pub async fn metrics(
    State(app_state): State<Arc<YedMQApp>>,
) -> (StatusCode, Json<RaftMetricsResponse>) {
    let topic_metrics = app_state
        .raft_manager
        .topic_raft()
        .raft
        .metrics()
        .borrow()
        .clone();
    let session_actor_map_metrics = app_state
        .raft_manager
        .session_actor_map_raft()
        .raft()
        .metrics()
        .borrow()
        .clone();

    let response = RaftMetricsResponse {
        topic_raft: topic_metrics,
        session_actor_map_raft: session_actor_map_metrics,
    };

    (StatusCode::OK, Json(response))
}
