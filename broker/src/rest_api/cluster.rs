use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use log::{info, warn};
use openraft::RaftMetrics;
use serde::{Deserialize, Serialize};

use crate::{
    app::YedMQApp,
    raft::Node,
};

#[derive(Debug, Serialize)]
pub struct RaftMetricsResponse {
    pub topic_raft: RaftMetrics<u64, Node>,
    pub session_actor_map_raft: RaftMetrics<u64, Node>,
    pub session_state_map_raft: RaftMetrics<u64, Node>,
}

#[derive(Debug, Deserialize)]
pub struct AddNodeRequest {
    node_id: u64,
    node: Node,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ChangeMembersRequest {
    members: Vec<u64>,
}

pub async fn init_cluster(State(app_state): State<Arc<YedMQApp>>) -> (StatusCode, String) {
    let _ = app_state.raft_manager.topic_raft().init_cluster().await;
    let _ = app_state
        .raft_manager
        .session_actor_map_raft()
        .init_cluster()
        .await;
    let _ = app_state
        .raft_manager
        .session_state_raft()
        .init_cluster()
        .await;

    (StatusCode::OK, format!(""))
}

pub async fn add_learner(
    State(app_state): State<Arc<YedMQApp>>,
    Json(payload): Json<AddNodeRequest>,
) -> (StatusCode, String) {
    let node_id = payload.node_id;
    let node = payload.node;
    let res = app_state
        .raft_manager
        .topic_raft()
        .raft()
        .add_learner(node_id, node.clone(), true)
        .await;
    if let Err(e) = res {
        warn!("topic raft add learner error: {}", e);
    }
    let res = app_state
        .raft_manager
        .session_actor_map_raft()
        .raft()
        .add_learner(node_id, node.clone(), true)
        .await;
    if let Err(e) = res {
        warn!("session actor map raft add learner error: {}", e);
    }

    let res = app_state
        .raft_manager
        .session_state_raft()
        .raft()
        .add_learner(node_id, node.clone(), true)
        .await;

    if let Err(e) = res {
        warn!("session state raft add learner error: {}", e);
    }

    (StatusCode::OK, format!(""))
}

pub async fn topic_raft_change_membership(
    State(app_state): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {

    info!("topic change member ship payload: {:#?}", payload.members.clone());
    let res = app_state
        .raft_manager
        .topic_raft()
        .raft()
        .change_membership(payload.members.clone(), true)
        .await;
    if let Err(e) = res {
        warn!("topic raft change membership error: {}", e);
        return (StatusCode::BAD_REQUEST, format!("{}", e));
    } else {
        return (StatusCode::OK, format!(""));
    }
}

pub async fn session_actor_map_raft_change_membership(
    State(app_state): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {

    let res = app_state
        .raft_manager
        .session_actor_map_raft()
        .raft()
        .change_membership(payload.members.clone(), true)
        .await;
    if let Err(e) = res {
        return (StatusCode::BAD_REQUEST, format!("{}", e));
    } else {
        return (StatusCode::OK, format!(""));
    }
}

pub async fn session_state_raft_change_membership(
    State(app_state): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {
    let res = app_state
        .raft_manager
        .session_state_raft()
        .raft()
        .change_membership(payload.members.clone(), true)
        .await;
    if let Err(e) = res {
        return (StatusCode::BAD_REQUEST, format!("{}", e));
    } else {
        return (StatusCode::OK, format!(""));
    }
}

pub async fn change_membership(
    State(app_state): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {
    info!("start change topic membership");

    let auth_info = &app_state.settings.listener.api.auth.users[0];

    let res = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/cluster/topic/membership",
            app_state
                .raft_manager
                .topic_raft()
                .get_leader().await
                .unwrap()
                .api_addr
        ))
        .json(&payload)
        .basic_auth(auth_info.username.clone(), Some(auth_info.password.clone()))
        .send()
        .await;
    if let Err(e) = res {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("update topic cluster membership error: {}", e));
    } else {
        info!("update topic cluster membership status: {}", res.unwrap().status());
    }

    info!("start change session actor map membership");
    let res = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/cluster/session_actor_map/membership",
            app_state
                .raft_manager
                .session_actor_map_raft()
                .get_leader().await
                .unwrap()
                .api_addr
        ))
        .json(&payload)
        .basic_auth(auth_info.username.clone(), Some(auth_info.password.clone()))
        .send()
        .await;
    if let Err(e) = res {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("update session state actor map cluster membership error: {}", e));
    }

    info!("start change session state membership");
    let res = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/cluster/session_state/membership",
            app_state
                .raft_manager
                .session_state_raft()
                .get_leader().await
                .unwrap()
                .api_addr
        ))
        .json(&payload)
        .basic_auth(auth_info.username.clone(), Some(auth_info.password.clone()))
        .send()
        .await;
    if let Err(e) = res {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("update session state raft cluster membership error: {}", e));
    }

    (StatusCode::OK, format!(""))
}

pub async fn metrics(
    State(app_state): State<Arc<YedMQApp>>,
) -> (StatusCode, Json<RaftMetricsResponse>) {
    let topic_metrics = app_state
        .raft_manager
        .topic_raft()
        .raft()
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
    let session_state_metrics = app_state
        .raft_manager
        .session_state_raft()
        .raft()
        .metrics()
        .borrow()
        .clone();

    let response = RaftMetricsResponse {
        topic_raft: topic_metrics,
        session_actor_map_raft: session_actor_map_metrics,
        session_state_map_raft: session_state_metrics,
    };

    (StatusCode::OK, Json(response))
}
