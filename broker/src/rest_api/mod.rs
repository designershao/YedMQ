use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use log::info;
use serde::Serialize;

use crate::plugin_manager;

#[derive(Serialize)]
struct Plugin {
    name: String,
    version: String,
    description: String,
    entry: String,
    priority: i64,
    author: String,
}

#[derive(Clone)]
struct AppState {
   pub plugin_manager: Arc<crate::plugin_manager::PluginManager>,
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

pub async fn run_rest_api_task(
    listen_address: &str,
    plugin_manager: Arc<plugin_manager::PluginManager>,
) -> anyhow::Result<()> {
    let state = AppState {
        plugin_manager,
    };

    let app= axum::Router::new()
        .route("/api/v1/plugins", axum::routing::get(plugin_list))
        .with_state(state);

    info!("start listening on {}", listen_address);

    let listener = tokio::net::TcpListener::bind(listen_address).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}