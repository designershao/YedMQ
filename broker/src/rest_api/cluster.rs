use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use openraft::RaftMetrics;

use crate::{app::YedMQApp, raft::Node};

pub async fn metrics (State(app_state): State<Arc<YedMQApp>>) -> (StatusCode, Json<RaftMetrics<u64, Node>>){
    let metrics = app_state.raft_manager.raft.metrics().borrow().clone();
    (StatusCode::OK, Json(metrics))
}