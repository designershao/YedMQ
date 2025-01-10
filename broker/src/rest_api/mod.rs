use std::sync::Arc;

use axum::{body::Body, extract::{Path, State}, http::{Request,  StatusCode}, middleware::Next, response::Response, Json};
use log::{error, info};
use serde::Serialize;
use tokio::sync::RwLock;
use base64::{Engine as _, engine::general_purpose};

use crate::{plugin_manager, session::session_manager::{self, SessionManagerError, SessionState}, settings::{self, Settings}};

#[derive(Serialize)]
struct SystemInfo {

    clients_connected: u64,

    bytes_received: u64,

    bytes_sent: u64,

    uptime: u64

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

    session_state: SessionState

}

#[derive(Clone)]
struct AppState {
   pub plugin_manager: Arc<crate::plugin_manager::PluginManager>,
   pub session_manager: Arc<RwLock<crate::session::session_manager::SessionManager>>,
   pub metric: Arc<crate::metric::Metric>,
   pub settings: Arc<Settings>
}

async fn plugin_list(
    State(app_state): State<AppState>,
) -> (StatusCode, Json<Vec<Plugin>>) {
    let plugin_metadata_list = app_state.plugin_manager.get_plugin_metadata_list();
    let mut result = Vec::<Plugin>::new();

    for plugin_metadata in plugin_metadata_list {
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

    (StatusCode::OK, Json(result))
}

async fn system_info(
    State(app_state): State<AppState>,
) -> (StatusCode, Json<SystemInfo>) {
    let metric = app_state.metric.clone();
    let system_info = SystemInfo{
        clients_connected: metric.clients_connected.load(std::sync::atomic::Ordering::SeqCst),
        bytes_received: metric.bytes_received.load(std::sync::atomic::Ordering::SeqCst),
        bytes_sent: metric.bytes_sent.load(std::sync::atomic::Ordering::SeqCst),
        uptime: metric.get_uptime()
    };
    (StatusCode::OK, Json(system_info))
}

async fn kickoff_client(
    State(app_state): State<AppState>,
    Path((tenant_id, client_id)):Path<(String,String)>,
) -> (StatusCode, ()) {
    let session_manager = app_state.session_manager.clone();
    let (session_quit_sender, mut session_quit_receiver) = tokio::sync::mpsc::channel(1);
    let kickoff_result = session_manager.read().await.kickoff(&tenant_id, &client_id, &session_quit_sender.clone()).await;
    if let Err(e) = kickoff_result {
        let response = match e.downcast_ref::<session_manager::SessionManagerError>() {
            Some(e) => {
                match e {
                    session_manager::SessionManagerError::TenantNotExisted(_) => {
                        (StatusCode::NOT_FOUND, ())
                    }
                    session_manager::SessionManagerError::SessionNotExisted(_) => {
                        (StatusCode::NOT_FOUND, ())
                    }
                    _ => {
                        error!("kickoff client error: {}", e);
                        (StatusCode::INTERNAL_SERVER_ERROR, ())
                    },
                }
            },
            None => (StatusCode::INTERNAL_SERVER_ERROR, ())
        };
        response
    } else {
        session_quit_receiver.recv().await;
        (StatusCode::OK, ())
    }
}

async fn client_list(
    State(app_state): State<AppState>,
    Path(tenant_id):Path<String>
) -> (StatusCode, Json<Vec<Client>>) {

    let session_manager = app_state.session_manager.clone();
    let session_list_result = session_manager.read().await.get_session_info_list(&tenant_id).await;
    if let Ok(session_list) = session_list_result {
        let mut result = Vec::<Client>::new();

        for session in session_list {
            let client = Client {
                tenant_identifier: session.tenant_identifier,
                client_id: session.client_identifier,
                subscription_topics: session.subscription_topics,
                session_state: session.session_state
            };
            result.push(client);
        }
        (StatusCode::OK, Json(result))
    } else {
        let response = match session_list_result.err().unwrap().downcast_ref::<SessionManagerError>() {
            Some(e) => {
                match e {
                    SessionManagerError::TenantNotExisted(_) => {
                        (StatusCode::NOT_FOUND, Json(Vec::new()))
                    }
                    _ => {
                        (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::new()))
                    },
                }
            },
            None => (StatusCode::INTERNAL_SERVER_ERROR, Json(Vec::new()))
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

                            if state.settings.listener.api.auth.users.iter().any(|user| {
                               user.username == username && user.password == password 
                            }) {
                                return Ok(next.run(req).await);
                            }
                        }
                    }
                }
            }
            Err((
                StatusCode::UNAUTHORIZED,
                "Invalid credentials".to_string(),
            ))
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
    metric: Arc<crate::metric::Metric>,
    settings: Arc<Settings>
) -> anyhow::Result<()> {
    let state = AppState {
        plugin_manager,
        session_manager,
        metric,
        settings
    };

    let state_for_basic_auth = state.clone();
    
    let app= axum::Router::new()
        .route("/api/v1/plugins", axum::routing::get(plugin_list))
        .route("/api/v1/:tenant_id/clients", axum::routing::get(client_list))
        .route("/api/v1/:tenant_id/clients/:client_id/kickoff", axum::routing::post(kickoff_client))
        .route("/api/v1/system_info", axum::routing::get(system_info))
        .layer(axum::middleware::from_fn_with_state(state_for_basic_auth, basic_auth_middleware))
        .with_state(state);

    info!("start listening on {}", listen_address);

    let listener = tokio::net::TcpListener::bind(listen_address).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}