use std::sync::Arc;
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
use tokio::sync::RwLock;
use crate::{
    plugin_manager,
    session::session_manager::{self},
    settings::Settings,
};

mod topic;
mod plugin;
mod message;
mod client;
mod system;

#[derive(Deserialize, Debug)]
struct Pagination {
    offset: Option<u64>,

    limit: Option<u64>,
}

#[derive(Clone)]
struct AppState {
    pub plugin_manager: Arc<crate::plugin_manager::PluginManager>,
    pub session_manager: Arc<RwLock<crate::session::session_manager::SessionManager>>,
    pub topic_manager: Arc<RwLock<crate::topic::TopicManager>>,
    pub metric: Arc<crate::metric::Metric>,
    pub settings: Arc<Settings>,
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
    pub code: String,
    pub message: String,
}

#[derive(Serialize)]
pub struct PaginationListResult<T> {
    pub meta: PaginationMeta,
    pub data: Vec<T>,
}

async fn basic_auth_middleware(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let auth_header = req.headers().get("Authorization");

    match auth_header {
        Some(value) => {
            // Extract basic auth value from the header
            if let Some(auth_value) = value.to_str().ok() {
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
    plugin_manager: Arc<plugin_manager::PluginManager>,
    session_manager: Arc<RwLock<session_manager::SessionManager>>,
    topic_manager: Arc<RwLock<crate::topic::TopicManager>>,
    metric: Arc<crate::metric::Metric>,
    settings: Arc<Settings>,
) -> anyhow::Result<()> {
    let state = AppState {
        plugin_manager,
        session_manager,
        topic_manager,
        metric,
        settings,
    };

    let state_for_basic_auth = state.clone();

    let app = axum::Router::new()
        .route("/api/v1/plugins", axum::routing::get(plugin::plugin_list))
        .route("/api/v1/:tenant_id/topics", axum::routing::get(topic::topic_list))
        .route("/api/v1/:tenant_id/messages/retained", axum::routing::get(message::retain_message_list))
        .route("/api/v1/:tenant_id/messages/retained/*topic_filter", axum::routing::delete(message::clean_retain_message))
        .route(
            "/api/v1/:tenant_id/clients",
            axum::routing::get(client::client_list),
        )
        .route(
            "/api/v1/:tenant_id/clients/:client_id/kickoff",
            axum::routing::post(client::kickoff_client),
        )
        .route("/api/v1/system_info", axum::routing::get(system::system_info))
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
