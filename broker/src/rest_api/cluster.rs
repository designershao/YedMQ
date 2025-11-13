use std::sync::Arc;

use actix::SystemService;
use axum::{extract::State, http::StatusCode, Json};
use log::{info, warn};
use openraft::RaftMetrics;
use serde::{Deserialize, Serialize};
use tokio::try_join;

use crate::{
    app::YedMQApp,
    raft::{Node, session_actor_map::session_actor_map_raft_actor, session_state::session_state_raft_actor},
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

pub async fn init_topic_raft(State(_): State<Arc<YedMQApp>>) -> (StatusCode, String) {
    let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();

    match topic_raft_actor_addr.send(
        crate::raft::topic::topic_raft_actor::InitRaftClusterMessage {}
    ).await {
        Ok(Ok(())) => {
            info!("init topic raft cluster success");
            return (StatusCode::OK, format!(""));
        },
        Ok(Err(err)) => {
            warn!("init topic raft cluster failed: {}", err);
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("init topic raft cluster failed: {}", err));
        },
        Err(_) => {
            warn!("init topic raft cluster failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("send init topic raft cluster message failed to the topic raft actor"));
        }
    }
}

pub async fn init_session_actor_map_raft(State(_): State<Arc<YedMQApp>>) -> (StatusCode, String) {
    let session_actor_map_raft_actor_addr =
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();

    match session_actor_map_raft_actor_addr.send(
        crate::raft::session_actor_map::session_actor_map_raft_actor::InitRaftClusterMessage {}
    ).await {
        Ok(Ok(())) => {
            info!("init session actor map raft cluster success");
            return (StatusCode::OK, format!(""));
        },
        Ok(Err(err)) => {
            warn!("init session actor map raft cluster failed: {}", err);
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("init session actor map raft cluster failed: {}", err));
        },
        Err(_) => {
            warn!("init session actor map raft cluster failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("send init session actor map raft cluster message failed to the session actor map raft actor"));
        }
    }
}

pub async fn init_session_state_raft(State(_): State<Arc<YedMQApp>>) -> (StatusCode, String) {
    let session_state_raft_actor_addr =
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

    match session_state_raft_actor_addr.send(
        crate::raft::session_state::session_state_raft_actor::InitRaftClusterMessage {}
    ).await {
        Ok(Ok(())) => {
            info!("init session state raft cluster success");
            return (StatusCode::OK, format!(""));
        },
        Ok(Err(err)) => {
            warn!("init session state raft cluster failed: {}", err);
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("init session state raft cluster failed: {}", err));
        },
        Err(_) => {
            warn!("init session state raft cluster failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("send init session state raft cluster message failed to the session state raft actor"));
        }
    }
}

pub async fn init_cluster(State(_): State<Arc<YedMQApp>>) -> (StatusCode, String) {
    let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let session_actor_map_raft_actor_addr =
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
    let session_state_raft_actor_addr =
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

    
    let init_result = try_join!(
        topic_raft_actor_addr.send(
            crate::raft::topic::topic_raft_actor::InitRaftClusterMessage {
            }
        ),
        session_actor_map_raft_actor_addr.send(
            crate::raft::session_actor_map::session_actor_map_raft_actor::InitRaftClusterMessage {
            }
        ),
        session_state_raft_actor_addr.send(
            crate::raft::session_state::session_state_raft_actor::InitRaftClusterMessage {
            }
        ),
    );

    match init_result {
        Ok((topic_result, map_result, state_result)) => {
            let all_success = topic_result.is_ok() && map_result.is_ok() && state_result.is_ok();
            if all_success {
                return (StatusCode::OK, format!(""));
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
                return (StatusCode::INTERNAL_SERVER_ERROR, error_messages.join(","));
            }
        }
        Err(_) => {
            warn!("init cluster failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("send init cluster message failed to raft actors"));
        },
    }

}

pub async fn add_learner(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<AddNodeRequest>,
) -> (StatusCode, String) {
    let node_id = payload.node_id;
    let node = payload.node;
    let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let session_actor_map_raft_actor_addr =
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
    let session_state_raft_actor_addr =
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

    let add_learner_result = try_join!(
        topic_raft_actor_addr.send(
            crate::raft::topic::topic_raft_actor::AddLearnerMessage {
                node_id,
                node: node.clone(),
            }
        ),
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
                return (StatusCode::OK, format!(""));
            } else {
                let mut error_messages = Vec::new();
                if let Err(err) = topic_result {
                    error_messages.push(format!("Topic raft add learner error: {}", err));
                }
                if let Err(err) = map_result {
                    error_messages.push(format!("Session actor map raft add learner error: {}", err));
                }
                if let Err(err) = state_result {
                    error_messages.push(format!("Session state raft add learner error: {}", err));
                }
                return (StatusCode::INTERNAL_SERVER_ERROR, error_messages.join(","));
            }
        }
        Err(_) => {
            warn!("add learner to cluster failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("send add learner message failed to raft actors"));
        }
    }
}

pub async fn topic_raft_change_membership(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {

    info!("topic change member ship payload: {:#?}", payload.members.clone());
    let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let res = topic_raft_actor_addr.send(
        crate::raft::topic::topic_raft_actor::ChangeMembershipMessage {
            members: payload.members.clone(),
        }
    ).await.unwrap();

    if let Err(e) = res {
        warn!("topic raft change membership error: {}", e);
        return (StatusCode::BAD_REQUEST, format!("{}", e));
    } else {
        return (StatusCode::OK, format!(""));
    }
}

pub async fn session_actor_map_raft_change_membership(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {
    let session_actor_map_raft_actor_addr = session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();

    let res = session_actor_map_raft_actor_addr.send(
        session_actor_map_raft_actor::ChangeMembershipMessage {
            members: payload.members.clone(),
        }
    ).await.unwrap();

    if let Err(e) = res {
        return (StatusCode::BAD_REQUEST, format!("{}", e));
    } else {
        return (StatusCode::OK, format!(""));
    }
}

pub async fn session_state_raft_change_membership(
    State(_): State<Arc<YedMQApp>>,
    Json(payload): Json<ChangeMembersRequest>,
) -> (StatusCode, String) {
    let session_state_raft_actor_addr = session_state_raft_actor::SessionStateRaftActor::from_registry();
    let res = session_state_raft_actor_addr.send(
        session_state_raft_actor::ChangeMembershipMessage {
            members: payload.members.clone(),
        }
    ).await.unwrap();
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

    let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();

    let topic_raft_leader_node = topic_raft_actor_addr.send(
        crate::raft::topic::topic_raft_actor::GetLeader{}
    ).await.unwrap();

    if let Err(e) = topic_raft_leader_node {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("get topic raft leader error: {}", e));
    }

    let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

    let session_state_raft_leader_node = session_state_raft_actor_addr.send(
        crate::raft::session_state::session_state_raft_actor::GetLeader{}
    ).await.unwrap();

    if let Err(e) = session_state_raft_leader_node {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("get session state raft leader error: {}", e));
    }

    let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();

    let session_actor_map_raft_leader_node = session_actor_map_raft_actor_addr.send(
        crate::raft::session_actor_map::session_actor_map_raft_actor::GetLeader{}
    ).await.unwrap();

    if let Err(e) = session_actor_map_raft_leader_node {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("get session actor map raft leader error: {}", e));
    }

    let res = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/cluster/topic/membership",
            topic_raft_leader_node.unwrap().unwrap().api_addr
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
            session_actor_map_raft_leader_node.unwrap().unwrap().api_addr
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
            session_state_raft_leader_node.unwrap().unwrap().api_addr
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
    State(_): State<Arc<YedMQApp>>,
) -> (StatusCode, Json<RaftMetricsResponse>) {
    let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
    let session_state_raft_actor_addr = crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry();

    let topic_metrics = topic_raft_actor_addr
        .send(crate::raft::topic::topic_raft_actor::GetRaftMetrics{})
        .await
        .unwrap().unwrap();
    let session_actor_map_metrics =session_actor_map_raft_actor_addr
        .send(crate::raft::session_actor_map::session_actor_map_raft_actor::GetRaftMetrics{})
        .await
        .unwrap().unwrap();
    let session_state_metrics = session_state_raft_actor_addr
        .send(crate::raft::session_state::session_state_raft_actor::GetRaftMetrics{})
        .await
        .unwrap().unwrap();

    let response = RaftMetricsResponse {
        topic_raft: topic_metrics,
        session_actor_map_raft: session_actor_map_metrics,
        session_state_map_raft: session_state_metrics,
    };

    (StatusCode::OK, Json(response))
}
