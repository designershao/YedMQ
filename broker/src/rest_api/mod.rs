use std::sync::Arc;

use axum::{extract::{State, Path}, http::StatusCode, Json};
use log::info;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{plugin_manager, session::session_manager::{self, SessionManagerError, SessionState}};

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
   pub metric: Arc<crate::metric::Metric>
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

pub async fn run_rest_api_task(
    listen_address: &str,
    plugin_manager: Arc<plugin_manager::PluginManager>,
    session_manager: Arc<RwLock<session_manager::SessionManager>>,
    metric: Arc<crate::metric::Metric>
) -> anyhow::Result<()> {
    let state = AppState {
        plugin_manager,
        session_manager,
        metric
    };

    let app= axum::Router::new()
        .route("/api/v1/plugins", axum::routing::get(plugin_list))
        .route("/api/v1/:tenant_id/clients", axum::routing::get(client_list))
        .route("/api/v1/system_info", axum::routing::get(system_info))
        .with_state(state);

    info!("start listening on {}", listen_address);

    let listener = tokio::net::TcpListener::bind(listen_address).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}