use std::sync::Arc;

use actix::SystemService;
use axum::{extract::State, http::StatusCode, Json};
use log::{error, info, warn};
use openraft::RaftMetrics;
use serde::{Deserialize, Serialize};
use tokio::try_join;

use crate::{
    app::YedMQApp,
    raft::{
        session_actor_map::session_actor_map_raft_actor, session_state::session_state_raft_actor,
        Node,
    },
};

#[derive(Debug, Serialize)]
pub struct RaftMetricsResponse {
    pub topic_raft: RaftMetrics<u64, Node>,
    pub session_actor_map_raft: RaftMetrics<u64, Node>,
    pub session_state_map_raft: RaftMetrics<u64, Node>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AddNodeRequest {
    node_id: u64,
    node: Node,
}

#[derive(Debug, Deserialize)]
pub struct AddClusterNodeRequest {
    node_id: u64,
    node: Node,
    members: Vec<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ChangeMembersRequest {
    members: Vec<u64>,
}

async fn post_json_with_auth<T: Serialize>(
    url: String,
    payload: &T,
    username: &str,
    password: &str,
    err_prefix: &str,
) -> Result<(), (StatusCode, String)> {
    let res = reqwest::Client::new()
        .post(url)
        .json(payload)
        .basic_auth(username.to_string(), Some(password.to_string()))
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("{}: {}", err_prefix, e),
            )
        })?;

    let status = res.status();
    if !status.is_success() {
        let body = res.text().await.unwrap_or_default();
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{}: status {}, body: {}", err_prefix, status, body),
        ));
    }

    Ok(())
}

pub async fn init_topic_raft(State(_): State<Arc<YedMQApp>>) -> (StatusCode, String) {
    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();

    match topic_raft_actor_addr
        .send(crate::raft::topic::topic_raft_actor::InitRaftClusterMessage {})
        .await
    {
        Ok(Ok(())) => {
            info!("init topic raft cluster success");
            (StatusCode::OK, String::new())
        }
        Ok(Err(err)) => {
            warn!("init topic raft cluster failed: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("init topic raft cluster failed: {}", err),
            )
        }
        Err(_) => {
            warn!("init topic raft cluster failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "send init topic raft cluster message failed to the topic raft actor".to_string(),
            )
        }
    }
}

pub async fn init_session_actor_map_raft(State(_): State<Arc<YedMQApp>>) -> (StatusCode, String) {
    let session_actor_map_raft_actor_addr =
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();

    match session_actor_map_raft_actor_addr
        .send(
            crate::raft::session_actor_map::session_actor_map_raft_actor::InitRaftClusterMessage {},
        )
        .await
    {
        Ok(Ok(())) => {
            info!("init session actor map raft cluster success");
            (StatusCode::OK, String::new())
        }
        Ok(Err(err)) => {
            warn!("init session actor map raft cluster failed: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("init session actor map raft cluster failed: {}", err),
            )
        }
        Err(_) => {
            warn!("init session actor map raft cluster failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "send init session actor map raft cluster message failed to the session actor map raft actor".to_string())
        }
    }
}

pub async fn init_session_state_raft(State(_): State<Arc<YedMQApp>>) -> (StatusCode, String) {
    let session_state_raft_actor_addr =
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry(
        );

    match session_state_raft_actor_addr
        .send(crate::raft::session_state::session_state_raft_actor::InitRaftClusterMessage {})
        .await
    {
        Ok(Ok(())) => {
            info!("init session state raft cluster success");
            (StatusCode::OK, String::new())
        }
        Ok(Err(err)) => {
            warn!("init session state raft cluster failed: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("init session state raft cluster failed: {}", err),
            )
        }
        Err(_) => {
            warn!("init session state raft cluster failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "send init session state raft cluster message failed to the session state raft actor".to_string())
        }
    }
}

pub async fn init_cluster(State(_): State<Arc<YedMQApp>>) -> (StatusCode, String) {
    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let session_actor_map_raft_actor_addr =
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
    let session_state_raft_actor_addr =
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry(
        );

    let init_result = try_join!(
        topic_raft_actor_addr.send(crate::raft::topic::topic_raft_actor::InitRaftClusterMessage {}),
        session_actor_map_raft_actor_addr.send(
            crate::raft::session_actor_map::session_actor_map_raft_actor::InitRaftClusterMessage {}
        ),
        session_state_raft_actor_addr
            .send(crate::raft::session_state::session_state_raft_actor::InitRaftClusterMessage {}),
    );

    match init_result {
        Ok((topic_result, map_result, state_result)) => {
            let all_success = topic_result.is_ok() && map_result.is_ok() && state_result.is_ok();
            if all_success {
                (StatusCode::OK, String::new())
            } else {
                let mut error_messages = Vec::new();
                if let Err(err) = topic_result {
                    error_messages.push(format!("Topic raft init error: {}", err));
                }
                if let Err(err) = map_result {
                    error_messages.push(format!("Session actor map raft init error: {}", err));
                }
                if let Err(err) = state_result {
                    error_messages.push(format!("Session state raft init error: {}", err));
                }
                (StatusCode::INTERNAL_SERVER_ERROR, error_messages.join(","))
            }
        }
        Err(_) => {
            warn!("init cluster failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "send init cluster message failed to raft actors".to_string(),
            )
        }
    }
}

pub async fn add_learner(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<AddNodeRequest>,
) -> (StatusCode, String) {
    let node_id = payload.node_id;
    let node = payload.node;
    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let session_actor_map_raft_actor_addr =
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
    let session_state_raft_actor_addr =
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry(
        );

    let add_learner_result = try_join!(
        topic_raft_actor_addr.send(crate::raft::topic::topic_raft_actor::AddLearnerMessage {
            node_id,
            node: node.clone(),
        }),
        session_actor_map_raft_actor_addr.send(
            crate::raft::session_actor_map::session_actor_map_raft_actor::AddLearnerMessage {
                node_id,
                node: node.clone(),
            }
        ),
        session_state_raft_actor_addr.send(
            crate::raft::session_state::session_state_raft_actor::AddLearnerMessage {
                node_id,
                node: node.clone(),
            }
        ),
    );

    match add_learner_result {
        Ok((topic_result, map_result, state_result)) => {
            let all_success = topic_result.is_ok() && map_result.is_ok() && state_result.is_ok();
            if all_success {
                (StatusCode::OK, String::new())
            } else {
                let mut error_messages = Vec::new();
                if let Err(err) = topic_result {
                    error_messages.push(format!("Topic raft add learner error: {}", err));
                }
                if let Err(err) = map_result {
                    error_messages
                        .push(format!("Session actor map raft add learner error: {}", err));
                }
                if let Err(err) = state_result {
                    error_messages.push(format!("Session state raft add learner error: {}", err));
                }
                (StatusCode::INTERNAL_SERVER_ERROR, error_messages.join(","))
            }
        }
        Err(_) => {
            warn!("add learner to cluster failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "send add learner message failed to raft actors".to_string(),
            )
        }
    }
}

pub async fn topic_raft_add_learner(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<AddNodeRequest>,
) -> (StatusCode, String) {
    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let res = topic_raft_actor_addr
        .send(crate::raft::topic::topic_raft_actor::AddLearnerMessage {
            node_id: payload.node_id,
            node: payload.node,
        })
        .await;

    match res {
        Ok(Ok(_)) => (StatusCode::OK, String::new()),
        Ok(Err(e)) => {
            warn!("topic raft add learner error: {}", e);
            (StatusCode::BAD_REQUEST, format!("{}", e))
        }
        Err(e) => {
            error!("TopicRaftActor unavailable: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("TopicRaftActor unavailable: {}", e),
            )
        }
    }
}

pub async fn session_actor_map_raft_add_learner(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<AddNodeRequest>,
) -> (StatusCode, String) {
    let session_actor_map_raft_actor_addr =
        session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
    let res = session_actor_map_raft_actor_addr
        .send(session_actor_map_raft_actor::AddLearnerMessage {
            node_id: payload.node_id,
            node: payload.node,
        })
        .await;

    match res {
        Ok(Ok(_)) => (StatusCode::OK, String::new()),
        Ok(Err(e)) => {
            warn!("session actor map raft add learner error: {}", e);
            (StatusCode::BAD_REQUEST, format!("{}", e))
        }
        Err(e) => {
            error!("SessionActorMapRaftActor unavailable: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("SessionActorMapRaftActor unavailable: {}", e),
            )
        }
    }
}

pub async fn session_state_raft_add_learner(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<AddNodeRequest>,
) -> (StatusCode, String) {
    let session_state_raft_actor_addr =
        session_state_raft_actor::SessionStateRaftActor::from_registry();
    let res = session_state_raft_actor_addr
        .send(session_state_raft_actor::AddLearnerMessage {
            node_id: payload.node_id,
            node: payload.node,
        })
        .await;

    match res {
        Ok(Ok(_)) => (StatusCode::OK, String::new()),
        Ok(Err(e)) => {
            warn!("session state raft add learner error: {}", e);
            (StatusCode::BAD_REQUEST, format!("{}", e))
        }
        Err(e) => {
            error!("SessionStateRaftActor unavailable: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("SessionStateRaftActor unavailable: {}", e),
            )
        }
    }
}

pub async fn add_node(
    State(app_state): State<Arc<YedMQApp>>,
    Json(payload): Json<AddClusterNodeRequest>,
) -> (StatusCode, String) {
    let auth_info = &app_state.settings.listener.api.auth.users[0];
    let add_payload = AddNodeRequest {
        node_id: payload.node_id,
        node: payload.node,
    };
    let change_payload = ChangeMembersRequest {
        members: payload.members,
    };

    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let session_state_raft_actor_addr =
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry(
        );
    let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();

    let topic_raft_leader_node_res = topic_raft_actor_addr
        .send(crate::raft::topic::topic_raft_actor::GetLeader {})
        .await;
    let topic_raft_leader_node = match topic_raft_leader_node_res {
        Ok(Ok(Some(node))) => node,
        Ok(Ok(None)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "topic raft leader unavailable".to_string(),
            );
        }
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("get topic raft leader error: {}", e),
            );
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("TopicRaftActor unavailable: {}", e),
            );
        }
    };

    let session_actor_map_raft_leader_node_res = session_actor_map_raft_actor_addr
        .send(crate::raft::session_actor_map::session_actor_map_raft_actor::GetLeader {})
        .await;
    let session_actor_map_raft_leader_node = match session_actor_map_raft_leader_node_res {
        Ok(Ok(Some(node))) => node,
        Ok(Ok(None)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "session actor map raft leader unavailable".to_string(),
            );
        }
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("get session actor map raft leader error: {}", e),
            );
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("SessionActorMapRaftActor unavailable: {}", e),
            );
        }
    };

    let session_state_raft_leader_node_res = session_state_raft_actor_addr
        .send(crate::raft::session_state::session_state_raft_actor::GetLeader {})
        .await;
    let session_state_raft_leader_node = match session_state_raft_leader_node_res {
        Ok(Ok(Some(node))) => node,
        Ok(Ok(None)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "session state raft leader unavailable".to_string(),
            );
        }
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("get session state raft leader error: {}", e),
            );
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("SessionStateRaftActor unavailable: {}", e),
            );
        }
    };

    let add_topic_url = format!(
        "http://{}/api/v1/cluster/topic/learners",
        topic_raft_leader_node.api_addr
    );
    if let Err(e) = post_json_with_auth(
        add_topic_url,
        &add_payload,
        &auth_info.username,
        &auth_info.password,
        "add topic raft learner error",
    )
    .await
    {
        return e;
    }

    let add_map_url = format!(
        "http://{}/api/v1/cluster/session_actor_map/learners",
        session_actor_map_raft_leader_node.api_addr
    );
    if let Err(e) = post_json_with_auth(
        add_map_url,
        &add_payload,
        &auth_info.username,
        &auth_info.password,
        "add session actor map raft learner error",
    )
    .await
    {
        return e;
    }

    let add_state_url = format!(
        "http://{}/api/v1/cluster/session_state/learners",
        session_state_raft_leader_node.api_addr
    );
    if let Err(e) = post_json_with_auth(
        add_state_url,
        &add_payload,
        &auth_info.username,
        &auth_info.password,
        "add session state raft learner error",
    )
    .await
    {
        return e;
    }

    let change_topic_url = format!(
        "http://{}/api/v1/cluster/topic/membership",
        topic_raft_leader_node.api_addr
    );
    if let Err(e) = post_json_with_auth(
        change_topic_url,
        &change_payload,
        &auth_info.username,
        &auth_info.password,
        "change topic raft membership error",
    )
    .await
    {
        return e;
    }

    let change_map_url = format!(
        "http://{}/api/v1/cluster/session_actor_map/membership",
        session_actor_map_raft_leader_node.api_addr
    );
    if let Err(e) = post_json_with_auth(
        change_map_url,
        &change_payload,
        &auth_info.username,
        &auth_info.password,
        "change session actor map raft membership error",
    )
    .await
    {
        return e;
    }

    let change_state_url = format!(
        "http://{}/api/v1/cluster/session_state/membership",
        session_state_raft_leader_node.api_addr
    );
    if let Err(e) = post_json_with_auth(
        change_state_url,
        &change_payload,
        &auth_info.username,
        &auth_info.password,
        "change session state raft membership error",
    )
    .await
    {
        return e;
    }

    (StatusCode::OK, String::new())
}

pub async fn topic_raft_change_membership(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {
    info!(
        "topic change member ship payload: {:#?}",
        payload.members.clone()
    );
    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let res = topic_raft_actor_addr
        .send(
            crate::raft::topic::topic_raft_actor::ChangeMembershipMessage {
                members: payload.members.clone(),
            },
        )
        .await;

    match res {
        Ok(Ok(_)) => (StatusCode::OK, String::new()),
        Ok(Err(e)) => {
            warn!("topic raft change membership error: {}", e);
            (StatusCode::BAD_REQUEST, format!("{}", e))
        }
        Err(e) => {
            error!("TopicRaftActor unavailable: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("TopicRaftActor unavailable: {}", e),
            )
        }
    }
}

pub async fn session_actor_map_raft_change_membership(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {
    let session_actor_map_raft_actor_addr =
        session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();

    let res = session_actor_map_raft_actor_addr
        .send(session_actor_map_raft_actor::ChangeMembershipMessage {
            members: payload.members.clone(),
        })
        .await;

    match res {
        Ok(Ok(_)) => (StatusCode::OK, String::new()),
        Ok(Err(e)) => {
            warn!("session actor map raft change membership error: {}", e);
            (StatusCode::BAD_REQUEST, format!("{}", e))
        }
        Err(e) => {
            error!("SessionActorMapRaftActor unavailable: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("SessionActorMapRaftActor unavailable: {}", e),
            )
        }
    }
}

pub async fn session_state_raft_change_membership(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {
    let session_state_raft_actor_addr =
        session_state_raft_actor::SessionStateRaftActor::from_registry();
    let res = session_state_raft_actor_addr
        .send(session_state_raft_actor::ChangeMembershipMessage {
            members: payload.members.clone(),
        })
        .await;
    match res {
        Ok(Ok(_)) => (StatusCode::OK, String::new()),
        Ok(Err(e)) => {
            warn!("session state raft change membership error: {}", e);
            (StatusCode::BAD_REQUEST, format!("{}", e))
        }
        Err(e) => {
            error!("SessionStateRaftActor unavailable: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("SessionStateRaftActor unavailable: {}", e),
            )
        }
    }
}

pub async fn change_membership(
    State(app_state): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {
    info!("start change topic membership");

    let auth_info = &app_state.settings.listener.api.auth.users[0];

    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();

    let topic_raft_leader_node_res = topic_raft_actor_addr
        .send(crate::raft::topic::topic_raft_actor::GetLeader {})
        .await;

    let topic_raft_leader_node = match topic_raft_leader_node_res {
        Ok(Ok(Some(node))) => node,
        Ok(Ok(None)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "topic raft leader unavailable".to_string(),
            );
        }
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("get topic raft leader error: {}", e),
            );
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("TopicRaftActor unavailable: {}", e),
            );
        }
    };

    let session_state_raft_actor_addr =
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry(
        );

    let session_state_raft_leader_node_res = session_state_raft_actor_addr
        .send(crate::raft::session_state::session_state_raft_actor::GetLeader {})
        .await;

    let session_state_raft_leader_node = match session_state_raft_leader_node_res {
        Ok(Ok(Some(node))) => node,
        Ok(Ok(None)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "session state raft leader unavailable".to_string(),
            );
        }
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("get session state raft leader error: {}", e),
            );
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("SessionStateRaftActor unavailable: {}", e),
            );
        }
    };

    let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();

    let session_actor_map_raft_leader_node_res = session_actor_map_raft_actor_addr
        .send(crate::raft::session_actor_map::session_actor_map_raft_actor::GetLeader {})
        .await;

    let session_actor_map_raft_leader_node = match session_actor_map_raft_leader_node_res {
        Ok(Ok(Some(node))) => node,
        Ok(Ok(None)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "session actor map raft leader unavailable".to_string(),
            );
        }
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("get session actor map raft leader error: {}", e),
            );
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("SessionActorMapRaftActor unavailable: {}", e),
            );
        }
    };

    let res = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/cluster/topic/membership",
            topic_raft_leader_node.api_addr
        ))
        .json(&payload)
        .basic_auth(auth_info.username.clone(), Some(auth_info.password.clone()))
        .send()
        .await;
    match res {
        Ok(resp) => {
            info!("update topic cluster membership status: {}", resp.status());
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("update topic cluster membership error: {}", e),
            );
        }
    }

    info!("start change session actor map membership");
    let res = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/cluster/session_actor_map/membership",
            session_actor_map_raft_leader_node.api_addr
        ))
        .json(&payload)
        .basic_auth(auth_info.username.clone(), Some(auth_info.password.clone()))
        .send()
        .await;
    if let Err(e) = res {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "update session state actor map cluster membership error: {}",
                e
            ),
        );
    }

    info!("start change session state membership");
    let res = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/cluster/session_state/membership",
            session_state_raft_leader_node.api_addr
        ))
        .json(&payload)
        .basic_auth(auth_info.username.clone(), Some(auth_info.password.clone()))
        .send()
        .await;
    if let Err(e) = res {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("update session state raft cluster membership error: {}", e),
        );
    }

    (StatusCode::OK, String::new())
}

pub async fn metrics(
    State(_): State<Arc<YedMQApp>>,
) -> Result<Json<RaftMetricsResponse>, (StatusCode, String)> {
    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
    let session_state_raft_actor_addr =
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry(
        );

    let topic_metrics = match topic_raft_actor_addr
        .send(crate::raft::topic::topic_raft_actor::GetRaftMetrics {})
        .await
    {
        Ok(Ok(metrics)) => metrics,
        Ok(Err(e)) => {
            warn!("topic raft get metrics error: {}", e);
            return Err((StatusCode::BAD_REQUEST, format!("{}", e)));
        }
        Err(e) => {
            error!("TopicRaftActor unavailable: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("TopicRaftActor unavailable: {}", e),
            ));
        }
    };

    let session_actor_map_metrics = match session_actor_map_raft_actor_addr
        .send(crate::raft::session_actor_map::session_actor_map_raft_actor::GetRaftMetrics {})
        .await
    {
        Ok(Ok(metrics)) => metrics,
        Ok(Err(e)) => {
            warn!("session actor map raft get metrics error: {}", e);
            return Err((StatusCode::BAD_REQUEST, format!("{}", e)));
        }
        Err(e) => {
            error!("SessionActorMapRaftActor unavailable: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("SessionActorMapRaftActor unavailable: {}", e),
            ));
        }
    };

    let session_state_metrics = match session_state_raft_actor_addr
        .send(crate::raft::session_state::session_state_raft_actor::GetRaftMetrics {})
        .await
    {
        Ok(Ok(metrics)) => metrics,
        Ok(Err(e)) => {
            warn!("session state raft get metrics error: {}", e);
            return Err((StatusCode::BAD_REQUEST, format!("{}", e)));
        }
        Err(e) => {
            error!("SessionStateRaftActor unavailable: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("SessionStateRaftActor unavailable: {}", e),
            ));
        }
    };

    let response = RaftMetricsResponse {
        topic_raft: topic_metrics,
        session_actor_map_raft: session_actor_map_metrics,
        session_state_map_raft: session_state_metrics,
    };

    Ok(Json(response))
}
