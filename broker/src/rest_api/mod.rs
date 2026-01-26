use crate::app::YedMQApp;
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use base64::{engine::general_purpose, Engine as _};
use log::info;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod client;
mod cluster;
mod message;
mod plugin;
mod system;
mod topic;

#[derive(Deserialize, Debug)]
struct Pagination {
    offset: Option<u64>,

    limit: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginationMeta {
    pub offset: u64,
    pub limit: u64,
    pub total: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub code: i32,
    pub message: String,
}

#[derive(Serialize)]
pub struct PaginationListResult<T> {
    pub meta: PaginationMeta,
    pub data: Vec<T>,
}

async fn basic_auth_middleware(
    State(state): State<Arc<YedMQApp>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let auth_header = req.headers().get("Authorization");

    match auth_header {
        Some(value) => {
            // Extract basic auth value from the header
            if let Ok(auth_value) = value.to_str() {
                if let Some(credentials) = auth_value.strip_prefix("Basic ") {
                    let decoded = general_purpose::STANDARD.decode(credentials).map_err(|_| {
                        (
                            StatusCode::UNAUTHORIZED,
                            "Invalid authorization header".to_string(),
                        )
                    })?;

                    if let Ok(decoded_str) = std::str::from_utf8(&decoded) {
                        let parts: Vec<&str> = decoded_str.split(":").collect();
                        if parts.len() == 2 {
                            let username = parts[0];
                            let password = parts[1];

                            if state
                                .settings
                                .listener
                                .api
                                .auth
                                .users
                                .iter()
                                .any(|user| user.username == username && user.password == password)
                            {
                                return Ok(next.run(req).await);
                            }
                        }
                    }
                }
            }
            Err((StatusCode::UNAUTHORIZED, "Invalid credentials".to_string()))
        }
        None => Err((
            StatusCode::UNAUTHORIZED,
            "Authorization header missing".to_string(),
        )),
    }
}

pub async fn run_rest_api_task(
    listen_address: &str,
    app: Arc<crate::app::YedMQApp>,
) -> anyhow::Result<()> {
    let state = app.clone();
    let state_for_basic_auth = app.clone();

    let app = axum::Router::new()
        .route("/api/v1/plugins", axum::routing::get(plugin::plugin_list))
        .route(
            "/api/v1/:tenant_id/topics",
            axum::routing::get(topic::topic_list),
        )
        .route(
            "/api/v1/:tenant_id/messages/retained",
            axum::routing::get(message::retain_message_list),
        )
        .route(
            "/api/v1/:tenant_id/messages/retained/*topic_filter",
            axum::routing::delete(message::clean_retain_message),
        )
        .route(
            "/api/v1/:tenant_id/messages",
            axum::routing::post(message::publish_message),
        )
        .route(
            "/api/v1/:tenant_id/clients",
            axum::routing::get(client::client_list),
        )
        .route(
            "/api/v1/:tenant_id/clients/:client_id/kickoff",
            axum::routing::post(client::kickoff_client),
        )
        .route(
            "/api/v1/system_info",
            axum::routing::get(system::system_info),
        )
        .route(
            "/api/v1/cluster/metrics",
            axum::routing::get(cluster::metrics),
        )
        .route(
            "/api/v1/cluster/learners",
            axum::routing::post(cluster::add_learner),
        )
        .route(
            "/api/v1/cluster/membership",
            axum::routing::post(cluster::change_membership),
        )
        .route(
            "/api/v1/cluster/topic/membership",
            axum::routing::post(cluster::topic_raft_change_membership),
        )
        .route(
            "/api/v1/cluster/session_actor_map/membership",
            axum::routing::post(cluster::session_actor_map_raft_change_membership),
        )
        .route(
            "/api/v1/cluster/session_state/membership",
            axum::routing::post(cluster::session_state_raft_change_membership),
        )
        .route(
            "/api/v1/cluster/init",
            axum::routing::post(cluster::init_cluster),
        )
        .route(
            "/api/v1/cluster/raft/topic/init",
            axum::routing::post(cluster::init_topic_raft),
        )
        .route(
            "/api/v1/cluster/raft/session_actor_map/init",
            axum::routing::post(cluster::init_session_actor_map_raft),
        )
        .route(
            "/api/v1/cluster/raft/session_state/init",
            axum::routing::post(cluster::init_session_state_raft),
        )
        .layer(axum::middleware::from_fn_with_state(
            state_for_basic_auth,
            basic_auth_middleware,
        ))
        .with_state(state);

    info!("start listening on {}", listen_address);

    let listener = tokio::net::TcpListener::bind(listen_address).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
