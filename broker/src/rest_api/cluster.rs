use std::{collections::BTreeSet, sync::Arc};

use actix::SystemService;
use axum::{extract::State, http::StatusCode, Json};
use log::{error, info, warn};
use openraft::RaftMetrics;
use serde::{Deserialize, Serialize};
use tokio::try_join;

use crate::{
    app::YedMQApp,
    protobuf::{cluster_service_client::ClusterServiceClient, GetSessionInfoRequest},
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterReadyResponse {
    pub ready: bool,
    pub node_id: u64,
    pub cluster_node_ids: Vec<u64>,
    pub checks: ClusterReadyChecks,
    pub reasons: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterReadyChecks {
    pub topic_raft: RaftGroupReadyStatus,
    pub session_actor_map_raft: RaftGroupReadyStatus,
    pub session_state_raft: SessionStateReadyStatus,
    pub cluster_rpc: ClusterRpcReadyStatus,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftGroupReadyStatus {
    pub ready: bool,
    pub leader_id: Option<u64>,
    pub membership_node_ids: Vec<u64>,
    pub missing_cluster_node_ids: Vec<u64>,
    pub extra_cluster_node_ids: Vec<u64>,
    pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionStateReadyStatus {
    pub ready: bool,
    pub leader_id: Option<u64>,
    pub membership_node_ids: Vec<u64>,
    pub missing_cluster_node_ids: Vec<u64>,
    pub extra_cluster_node_ids: Vec<u64>,
    pub payload_ready: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRpcReadyStatus {
    pub ready: bool,
    pub rpc_addr: String,
    pub reason: Option<String>,
}

fn build_not_ready_status(reason: impl Into<String>) -> RaftGroupReadyStatus {
    RaftGroupReadyStatus {
        ready: false,
        leader_id: None,
        membership_node_ids: Vec::new(),
        missing_cluster_node_ids: Vec::new(),
        extra_cluster_node_ids: Vec::new(),
        reason: Some(reason.into()),
    }
}

fn evaluate_raft_metrics(metrics: &RaftMetrics<u64, Node>) -> RaftGroupReadyStatus {
    let mut membership_node_ids: Vec<u64> = metrics
        .membership_config
        .nodes()
        .map(|(node_id, _)| *node_id)
        .collect();
    membership_node_ids.sort_unstable();
    membership_node_ids.dedup();

    let reason = if metrics.current_leader.is_none() {
        Some("no leader elected".to_string())
    } else if membership_node_ids.is_empty() {
        Some("membership is empty".to_string())
    } else {
        None
    };

    RaftGroupReadyStatus {
        ready: reason.is_none(),
        leader_id: metrics.current_leader,
        membership_node_ids,
        missing_cluster_node_ids: Vec::new(),
        extra_cluster_node_ids: Vec::new(),
        reason,
    }
}

fn combine_reason(base: Option<String>, extra: Option<String>) -> Option<String> {
    match (base, extra) {
        (Some(base), Some(extra)) => Some(format!("{base}; {extra}")),
        (Some(base), None) => Some(base),
        (None, Some(extra)) => Some(extra),
        (None, None) => None,
    }
}

fn derive_cluster_node_ids(statuses: &[&RaftGroupReadyStatus]) -> Vec<u64> {
    statuses
        .iter()
        .find_map(|status| {
            if status.membership_node_ids.is_empty() {
                None
            } else {
                Some(status.membership_node_ids.clone())
            }
        })
        .unwrap_or_default()
}

fn apply_cluster_node_ids(
    mut status: RaftGroupReadyStatus,
    cluster_node_ids: &[u64],
) -> RaftGroupReadyStatus {
    let membership_node_set: BTreeSet<u64> = status.membership_node_ids.iter().copied().collect();
    let cluster_node_set: BTreeSet<u64> = cluster_node_ids.iter().copied().collect();

    let missing_cluster_node_ids: Vec<u64> = cluster_node_ids
        .iter()
        .copied()
        .filter(|node_id| !membership_node_set.contains(node_id))
        .collect();
    let extra_cluster_node_ids: Vec<u64> = status
        .membership_node_ids
        .iter()
        .copied()
        .filter(|node_id| !cluster_node_set.contains(node_id))
        .collect();

    let membership_reason = if cluster_node_ids.is_empty() {
        Some("cluster membership is unavailable".to_string())
    } else if !missing_cluster_node_ids.is_empty() || !extra_cluster_node_ids.is_empty() {
        Some(format!(
            "membership differs from cluster nodes; missing {:?}, extra {:?}",
            missing_cluster_node_ids, extra_cluster_node_ids
        ))
    } else {
        None
    };

    status.ready = status.ready
        && !cluster_node_ids.is_empty()
        && missing_cluster_node_ids.is_empty()
        && extra_cluster_node_ids.is_empty();
    status.missing_cluster_node_ids = missing_cluster_node_ids;
    status.extra_cluster_node_ids = extra_cluster_node_ids;
    status.reason = combine_reason(status.reason, membership_reason);
    status
}

async fn probe_local_cluster_rpc(app: &YedMQApp) -> ClusterRpcReadyStatus {
    let rpc_addr = app.settings.cluster.rpc.external.clone();
    let channel = match crate::rpc::grpc_client::connected_channel(
        &rpc_addr,
        std::time::Duration::from_millis(500),
    )
    .await
    {
        Ok(channel) => channel,
        Err(err) => {
            return ClusterRpcReadyStatus {
                ready: false,
                rpc_addr,
                reason: Some(format!("failed to create cluster rpc client: {}", err)),
            };
        }
    };

    let mut client = ClusterServiceClient::new(channel);
    let request = tonic::Request::new(GetSessionInfoRequest {
        tenant_id: "__yedmq_ready_probe__".to_string(),
        client_id: "__yedmq_ready_probe__".to_string(),
    });

    match tokio::time::timeout(
        std::time::Duration::from_millis(800),
        client.get_session_info(request),
    )
    .await
    {
        Ok(Ok(_)) => ClusterRpcReadyStatus {
            ready: true,
            rpc_addr,
            reason: None,
        },
        Ok(Err(status)) => {
            let parsed = crate::rpc::grpc_status::decode_status(&status);
            let message = status.to_string();
            let transport_unavailable = matches!(
                status.code(),
                tonic::Code::Unavailable | tonic::Code::Unknown
            ) && (message.contains("transport error")
                || message.contains("tcp connect error")
                || message.contains("Service was not ready"));
            let application_not_ready =
                parsed.error_kind.as_deref() == Some(crate::rpc::grpc_status::ERROR_KIND_NOT_READY);

            if transport_unavailable || application_not_ready {
                ClusterRpcReadyStatus {
                    ready: false,
                    rpc_addr,
                    reason: Some(format!("cluster rpc probe failed: {}", status)),
                }
            } else {
                // Any non-transport application response proves the ClusterService is reachable.
                ClusterRpcReadyStatus {
                    ready: true,
                    rpc_addr,
                    reason: None,
                }
            }
        }
        Err(_) => ClusterRpcReadyStatus {
            ready: false,
            rpc_addr,
            reason: Some("cluster rpc probe timed out".to_string()),
        },
    }
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

pub async fn ready(State(app): State<Arc<YedMQApp>>) -> (StatusCode, Json<ClusterReadyResponse>) {
    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let session_actor_map_raft_actor_addr = crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
    let session_state_raft_actor_addr =
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor::from_registry(
        );

    let topic_raft_base = match topic_raft_actor_addr
        .send(crate::raft::topic::topic_raft_actor::GetRaftMetrics {})
        .await
    {
        Ok(Ok(metrics)) => evaluate_raft_metrics(&metrics),
        Ok(Err(err)) => build_not_ready_status(format!("topic raft unavailable: {}", err)),
        Err(err) => build_not_ready_status(format!("TopicRaftActor unavailable: {}", err)),
    };

    let session_actor_map_raft_base = match session_actor_map_raft_actor_addr
        .send(crate::raft::session_actor_map::session_actor_map_raft_actor::GetRaftMetrics {})
        .await
    {
        Ok(Ok(metrics)) => evaluate_raft_metrics(&metrics),
        Ok(Err(err)) => {
            build_not_ready_status(format!("session actor map raft unavailable: {}", err))
        }
        Err(err) => {
            build_not_ready_status(format!("SessionActorMapRaftActor unavailable: {}", err))
        }
    };

    let session_state_raft_base = match session_state_raft_actor_addr
        .send(crate::raft::session_state::session_state_raft_actor::GetRaftMetrics {})
        .await
    {
        Ok(Ok(metrics)) => evaluate_raft_metrics(&metrics),
        Ok(Err(err)) => build_not_ready_status(format!("session state raft unavailable: {}", err)),
        Err(err) => build_not_ready_status(format!("SessionStateRaftActor unavailable: {}", err)),
    };

    let cluster_node_ids = derive_cluster_node_ids(&[
        &topic_raft_base,
        &session_actor_map_raft_base,
        &session_state_raft_base,
    ]);

    let topic_raft = apply_cluster_node_ids(topic_raft_base, &cluster_node_ids);
    let session_actor_map_raft =
        apply_cluster_node_ids(session_actor_map_raft_base, &cluster_node_ids);
    let session_state_raft_base =
        apply_cluster_node_ids(session_state_raft_base, &cluster_node_ids);

    let (payload_ready, payload_reason) = match session_state_raft_actor_addr
        .send(crate::raft::session_state::session_state_raft_actor::GetPayloadReady {})
        .await
    {
        Ok(true) => (true, None),
        Ok(false) => (
            false,
            Some("session state payload store is not ready".to_string()),
        ),
        Err(err) => (
            false,
            Some(format!("SessionStateRaftActor unavailable: {}", err)),
        ),
    };

    let session_state_raft = SessionStateReadyStatus {
        ready: session_state_raft_base.ready && payload_ready,
        leader_id: session_state_raft_base.leader_id,
        membership_node_ids: session_state_raft_base.membership_node_ids.clone(),
        missing_cluster_node_ids: session_state_raft_base.missing_cluster_node_ids.clone(),
        extra_cluster_node_ids: session_state_raft_base.extra_cluster_node_ids.clone(),
        payload_ready,
        reason: combine_reason(session_state_raft_base.reason.clone(), payload_reason),
    };
    let cluster_rpc = probe_local_cluster_rpc(&app).await;

    let mut reasons = Vec::new();
    if let Some(reason) = &topic_raft.reason {
        reasons.push(format!("topic_raft: {}", reason));
    }
    if let Some(reason) = &session_actor_map_raft.reason {
        reasons.push(format!("session_actor_map_raft: {}", reason));
    }
    if let Some(reason) = &session_state_raft.reason {
        reasons.push(format!("session_state_raft: {}", reason));
    }
    if let Some(reason) = &cluster_rpc.reason {
        reasons.push(format!("cluster_rpc: {}", reason));
    }

    let response = ClusterReadyResponse {
        ready: topic_raft.ready
            && session_actor_map_raft.ready
            && session_state_raft.ready
            && cluster_rpc.ready,
        node_id: app.settings.cluster.node_id,
        cluster_node_ids,
        checks: ClusterReadyChecks {
            topic_raft,
            session_actor_map_raft,
            session_state_raft,
            cluster_rpc,
        },
        reasons,
    };

    let status = if response.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(response))
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
