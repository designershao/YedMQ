use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use openraft::RaftMetrics;
use serde::Serialize;

use crate::{app::YedMQApp, raft::Node};

#[derive(Debug, Serialize)]
pub struct RaftMetricsResponse {
    pub topic_raft: RaftMetrics<u64, Node>,
    pub session_actor_map_raft: RaftMetrics<u64, Node>,
}

pub async fn metrics (State(app_state): State<Arc<YedMQApp>>) -> (StatusCode, Json<RaftMetricsResponse>){
    let topic_metrics = app_state.raft_manager.topic_raft.raft.metrics().borrow().clone();
    let session_actor_map_metrics = app_state.raft_manager.session_actor_map_raft.raft.metrics().borrow().clone();

    let response = RaftMetricsResponse {
        topic_raft: topic_metrics,
        session_actor_map_raft: session_actor_map_metrics,
    };

    (StatusCode::OK, Json(response))
}