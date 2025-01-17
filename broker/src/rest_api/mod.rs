use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
    Json,
};
use base64::{engine::general_purpose, Engine as _};
use log::{error, info};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    plugin_manager,
    session::session_manager::{self, SessionManagerError, SessionState},
    settings::Settings,
    topic,
};

#[derive(Serialize)]
struct SystemInfo {
    clients_connected: u64,

    bytes_received: u64,

    bytes_sent: u64,

    uptime: u64,
}

#[derive(Serialize)]
struct Plugin {
    name: String,

    version: String,

    description: String,

    entry: String,

    priority: i64,

    author: String,
}

#[derive(Serialize)]
struct Client {
    tenant_identifier: String,

    client_id: String,

    subscription_topics: Vec<String>,

    session_state: SessionState,
}

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
pub struct PaginationMeta {
    pub offset: u64,
    pub limit: u64,
    pub total: u64,
}

#[derive(Serialize)]
pub struct PaginationListResult<T> {
    pub meta: PaginationMeta,
    pub data: Vec<T>,
}

#[derive(Serialize)]
pub struct Topic {
    pub topic: String,
    pub client_id: String,
    pub qos: u8,
}

#[derive(Serialize)]
pub struct RetainMessage {
    pub topic: String,
    pub qos: u8,
    pub client_identifier: String,
}

async fn topic_list(
    State(app_state): State<AppState>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> (StatusCode, Json<PaginationListResult<Topic>>) {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let topic_list_result = app_state
        .topic_manager
        .read()
        .await
        .get_topic_list_with_pagination(tenant_id, offset_param, limit_param);

    if let Err(err) = topic_list_result {
        error!("get topic list error: {}", err);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PaginationListResult {
                meta: PaginationMeta {
                    offset: offset_param,
                    limit: limit_param,
                    total: 0,
                },
                data: Vec::new(),
            }),
        );
    } else {
        let topic_list = topic_list_result.unwrap();
        let mut result = Vec::<Topic>::new();

        for (topic, client_id, qos) in topic_list.1 {
            let topic = Topic {
                topic,
                client_id,
                qos,
            };
            result.push(topic);
        }
        let meta = PaginationMeta {
            offset: offset_param,
            limit: limit_param,
            total: topic_list.0,
        };

        let result = PaginationListResult { meta, data: result };

        (StatusCode::OK, Json(result))
    }
}

async fn retain_message_list(
    State(app_state): State<AppState>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> (StatusCode, Json<PaginationListResult<RetainMessage>>) {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let r = app_state
        .topic_manager
        .read()
        .await
        .get_retain_message_list_with_pagination(tenant_id.as_str(), offset_param, limit_param);

    if let Err(err) = r {
        error!("get retain message list error: {}", err);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PaginationListResult {
                meta: PaginationMeta {
                    offset: offset_param,
                    limit: limit_param,
                    total: 0,
                },
                data: Vec::new(),
            }),
        );
    } else {
        let retain_message_list = r.unwrap();
        let mut result = Vec::<RetainMessage>::new();

        for (topic, client_identifier, qos) in retain_message_list.1 {
            let retain_message = RetainMessage {
                topic,
                qos,
                client_identifier,
            };
            result.push(retain_message);
        }
        let meta = PaginationMeta {
            offset: offset_param,
            limit: limit_param,
            total: retain_message_list.0,
        };

        let result = PaginationListResult { meta, data: result };
        (StatusCode::OK, Json(result))
    }
}

async fn plugin_list(
    State(app_state): State<AppState>,
    pagination: Query<Pagination>,
) -> (StatusCode, Json<PaginationListResult<Plugin>>) {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);
    let plugin_metadata_list = app_state
        .plugin_manager
        .get_plugin_metadata_list_with_pagination(offset_param, limit_param);
    let mut result = Vec::<Plugin>::new();

    for plugin_metadata in plugin_metadata_list.1 {
        let plugin = Plugin {
            name: plugin_metadata.name.clone(),
            version: plugin_metadata.version.clone(),
            description: plugin_metadata.description.clone(),
            entry: plugin_metadata.entry.clone(),
            priority: plugin_metadata.priority,
            author: plugin_metadata.author.clone(),
        };
        result.push(plugin);
    }

    let meta = PaginationMeta {
        offset: offset_param,
        limit: limit_param,
        total: plugin_metadata_list.0,
    };

    let result = PaginationListResult { meta, data: result };

    (StatusCode::OK, Json(result))
}

async fn system_info(State(app_state): State<AppState>) -> (StatusCode, Json<SystemInfo>) {
    let metric = app_state.metric.clone();
    let system_info = SystemInfo {
        clients_connected: metric
            .clients_connected
            .load(std::sync::atomic::Ordering::SeqCst),
        bytes_received: metric
            .bytes_received
            .load(std::sync::atomic::Ordering::SeqCst),
        bytes_sent: metric.bytes_sent.load(std::sync::atomic::Ordering::SeqCst),
        uptime: metric.get_uptime(),
    };
    (StatusCode::OK, Json(system_info))
}

async fn kickoff_client(
    State(app_state): State<AppState>,
    Path((tenant_id, client_id)): Path<(String, String)>,
) -> (StatusCode, ()) {
    let session_manager = app_state.session_manager.clone();
    let (session_quit_sender, mut session_quit_receiver) = tokio::sync::mpsc::channel(1);
    let kickoff_result = session_manager
        .read()
        .await
        .kickoff(&tenant_id, &client_id, &session_quit_sender.clone())
        .await;
    if let Err(e) = kickoff_result {
        let response = match e.downcast_ref::<session_manager::SessionManagerError>() {
            Some(e) => match e {
                session_manager::SessionManagerError::TenantNotExisted(_) => {
                    (StatusCode::NOT_FOUND, ())
                }
                session_manager::SessionManagerError::SessionNotExisted(_) => {
                    (StatusCode::NOT_FOUND, ())
                }
                _ => {
                    error!("kickoff client error: {}", e);
                    (StatusCode::INTERNAL_SERVER_ERROR, ())
                }
            },
            None => (StatusCode::INTERNAL_SERVER_ERROR, ()),
        };
        response
    } else {
        session_quit_receiver.recv().await;
        (StatusCode::OK, ())
    }
}

async fn client_list(
    State(app_state): State<AppState>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> (StatusCode, Json<PaginationListResult<Client>>) {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let session_manager = app_state.session_manager.clone();
    let session_list_result = session_manager
        .read()
        .await
        .get_session_info_list_with_pagination(&tenant_id, offset_param, limit_param)
        .await;
    if let Ok(session_list) = session_list_result {
        let mut result = Vec::<Client>::new();

        for session in session_list.1 {
            let client = Client {
                tenant_identifier: session.tenant_identifier,
                client_id: session.client_identifier,
                subscription_topics: session.subscription_topics,
                session_state: session.session_state,
            };
            result.push(client);
        }

        let meta = PaginationMeta {
            offset: offset_param,
            limit: limit_param,
            total: session_list.0,
        };

        let result = PaginationListResult { meta, data: result };
        (StatusCode::OK, Json(result))
    } else {
        let response = match session_list_result
            .err()
            .unwrap()
            .downcast_ref::<SessionManagerError>()
        {
            Some(e) => match e {
                SessionManagerError::TenantNotExisted(_) => (
                    StatusCode::NOT_FOUND,
                    Json(PaginationListResult {
                        meta: PaginationMeta {
                            offset: 0,
                            limit: 0,
                            total: 0,
                        },
                        data: Vec::new(),
                    }),
                ),
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(PaginationListResult {
                        meta: PaginationMeta {
                            offset: 0,
                            limit: 0,
                            total: 0,
                        },
                        data: Vec::new(),
                    }),
                ),
            },
            None => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PaginationListResult {
                    meta: PaginationMeta {
                        offset: 0,
                        limit: 0,
                        total: 0,
                    },
                    data: Vec::new(),
                }),
            ),
        };
        response
    }
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
    topic_manager: Arc<RwLock<topic::TopicManager>>,
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
        .route("/api/v1/plugins", axum::routing::get(plugin_list))
        .route("/api/v1/:tenant_id/topics", axum::routing::get(topic_list))
        .route("/api/v1/:tenant_id/messages/retained", axum::routing::get(retain_message_list))
        .route(
            "/api/v1/:tenant_id/clients",
            axum::routing::get(client_list),
        )
        .route(
            "/api/v1/:tenant_id/clients/:client_id/kickoff",
            axum::routing::post(kickoff_client),
        )
        .route("/api/v1/system_info", axum::routing::get(system_info))
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
