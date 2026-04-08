use std::sync::Arc;

use crate::app::YedMQApp;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use yedmq_plugin_host::{
    loader::{InvalidPluginManifest, PluginManifest, RuntimeType},
    plugin_manager::{
        PluginControlError, PluginListScope, PluginLogsPage, PluginSnapshot, PluginState,
    },
};

use super::{ErrorResponse, PaginationListResult, PaginationMeta};

const INVALID_REQUEST_CODE: i32 = 5;
const PLUGIN_NOT_FOUND_CODE: i32 = 6;
const PLUGIN_STATE_CONFLICT_CODE: i32 = 7;

#[derive(Deserialize, Debug)]
pub struct PluginListQuery {
    offset: Option<u64>,
    limit: Option<u64>,
    scope: Option<String>,
    state: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct PluginLogsQuery {
    offset: Option<u64>,
    limit: Option<u64>,
    tail: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginHook {
    name: String,
    priority: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginSummary {
    name: String,
    version: String,
    description: String,
    author: String,
    discovered: bool,
    managed: bool,
    state: String,
    healthy: Option<bool>,
    runtime_type: String,
    hook_count: usize,
    restart_count: u32,
    uptime_secs: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginDetail {
    name: String,
    version: String,
    description: String,
    author: String,
    license: Option<String>,
    homepage: Option<String>,
    repository: Option<String>,
    discovered: bool,
    managed: bool,
    state: String,
    healthy: Option<bool>,
    runtime_type: String,
    executable: Option<String>,
    args: Option<Vec<String>>,
    working_dir: Option<String>,
    timeout_secs: Option<u64>,
    env_keys: Vec<String>,
    hook_count: usize,
    hooks: Vec<PluginHook>,
    capabilities: Vec<String>,
    initialize_status: Option<String>,
    restart_count: u32,
    uptime_secs: Option<u64>,
    last_health_check_ago_secs: Option<u64>,
    ping_response_timeout_count: u32,
    last_error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginLogLine {
    index: u64,
    message: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvalidPlugin {
    path: String,
    error: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginRescanResult {
    discovered: Vec<String>,
    removed: Vec<String>,
    invalid: Vec<InvalidPlugin>,
}

fn runtime_type_to_string(runtime_type: &RuntimeType) -> String {
    match runtime_type {
        RuntimeType::Process => "process".to_string(),
    }
}

fn env_keys(manifest: &PluginManifest) -> Vec<String> {
    let mut keys = manifest
        .runtime
        .env
        .as_ref()
        .map(|env| env.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    keys.sort();
    keys
}

fn build_plugin_hooks(snapshot: &PluginSnapshot) -> Vec<PluginHook> {
    snapshot
        .hooks
        .iter()
        .map(|hook| PluginHook {
            name: hook.name.clone(),
            priority: hook.priority,
        })
        .collect()
}

fn build_plugin_summary(snapshot: PluginSnapshot) -> PluginSummary {
    PluginSummary {
        name: snapshot.name,
        version: snapshot.manifest.plugin.version.clone(),
        description: snapshot.manifest.plugin.description.clone(),
        author: snapshot.manifest.plugin.author.clone(),
        discovered: snapshot.discovered,
        managed: snapshot.managed,
        state: snapshot.state.as_str().to_string(),
        healthy: snapshot.healthy,
        runtime_type: runtime_type_to_string(&snapshot.manifest.runtime.runtime_type),
        hook_count: snapshot.hooks.len(),
        restart_count: snapshot.restart_count,
        uptime_secs: snapshot.uptime.map(|uptime| uptime.as_secs()),
    }
}

fn build_plugin_detail(snapshot: PluginSnapshot) -> PluginDetail {
    let hooks = build_plugin_hooks(&snapshot);
    PluginDetail {
        name: snapshot.name,
        version: snapshot.manifest.plugin.version.clone(),
        description: snapshot.manifest.plugin.description.clone(),
        author: snapshot.manifest.plugin.author.clone(),
        license: snapshot.manifest.plugin.license.clone(),
        homepage: snapshot.manifest.plugin.homepage.clone(),
        repository: snapshot.manifest.plugin.repository.clone(),
        discovered: snapshot.discovered,
        managed: snapshot.managed,
        state: snapshot.state.as_str().to_string(),
        healthy: snapshot.healthy,
        runtime_type: runtime_type_to_string(&snapshot.manifest.runtime.runtime_type),
        executable: snapshot.manifest.runtime.executable.clone(),
        args: snapshot.manifest.runtime.args.clone(),
        working_dir: snapshot.manifest.runtime.working_dir.clone(),
        timeout_secs: snapshot.manifest.runtime.timeout_secs,
        env_keys: env_keys(&snapshot.manifest),
        hook_count: hooks.len(),
        hooks,
        capabilities: snapshot.capabilities,
        initialize_status: snapshot.initialize_status,
        restart_count: snapshot.restart_count,
        uptime_secs: snapshot.uptime.map(|uptime| uptime.as_secs()),
        last_health_check_ago_secs: snapshot
            .last_health_check_ago
            .map(|elapsed| elapsed.as_secs()),
        ping_response_timeout_count: snapshot.ping_response_timeout_count,
        last_error: snapshot.last_error,
    }
}

fn build_invalid_plugins(items: Vec<InvalidPluginManifest>) -> Vec<InvalidPlugin> {
    items
        .into_iter()
        .map(|item| InvalidPlugin {
            path: item.path,
            error: item.error,
        })
        .collect()
}

fn error_response(status: StatusCode, code: i32, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            code,
            message: message.into(),
        }),
    )
        .into_response()
}

fn invalid_request_response(message: impl Into<String>) -> Response {
    error_response(StatusCode::BAD_REQUEST, INVALID_REQUEST_CODE, message)
}

fn plugin_control_error_response(error: PluginControlError) -> Response {
    match error {
        PluginControlError::NotFound(message) => {
            error_response(StatusCode::NOT_FOUND, PLUGIN_NOT_FOUND_CODE, message)
        }
        PluginControlError::InvalidState(plugin_name, state) => error_response(
            StatusCode::CONFLICT,
            PLUGIN_STATE_CONFLICT_CODE,
            format!("plugin '{}' is already {}", plugin_name, state),
        ),
        PluginControlError::Internal(message) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, 101, message)
        }
    }
}

fn parse_scope(value: Option<&str>) -> Result<PluginListScope, String> {
    match value.unwrap_or("managed") {
        "managed" => Ok(PluginListScope::Managed),
        "discovered" => Ok(PluginListScope::Discovered),
        "all" => Ok(PluginListScope::All),
        other => Err(format!(
            "invalid plugin scope '{}', expected one of: managed, discovered, all",
            other
        )),
    }
}

fn parse_state(value: Option<&str>) -> Result<Option<PluginState>, String> {
    match value {
        None => Ok(None),
        Some("discovered") => Ok(Some(PluginState::Discovered)),
        Some("starting") => Ok(Some(PluginState::Starting)),
        Some("running") => Ok(Some(PluginState::Running)),
        Some("stopping") => Ok(Some(PluginState::Stopping)),
        Some("stopped") => Ok(Some(PluginState::Stopped)),
        Some("failed") => Ok(Some(PluginState::Failed)),
        Some(other) => Err(format!(
            "invalid plugin state '{}', expected one of: discovered, starting, running, stopping, stopped, failed",
            other
        )),
    }
}

fn plugin_detail_response(app_state: &Arc<YedMQApp>, plugin_name: &str) -> Response {
    match app_state.plugin_manager.get_plugin_snapshot(plugin_name) {
        Some(snapshot) => (StatusCode::OK, Json(build_plugin_detail(snapshot))).into_response(),
        None => error_response(
            StatusCode::NOT_FOUND,
            PLUGIN_NOT_FOUND_CODE,
            format!("plugin '{}' not found", plugin_name),
        ),
    }
}

pub async fn plugin_list(
    State(app_state): State<Arc<YedMQApp>>,
    Query(query): Query<PluginListQuery>,
) -> Response {
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(10);

    let scope = match parse_scope(query.scope.as_deref()) {
        Ok(scope) => scope,
        Err(message) => return invalid_request_response(message),
    };
    let state_filter = match parse_state(query.state.as_deref()) {
        Ok(state) => state,
        Err(message) => return invalid_request_response(message),
    };

    let snapshots = app_state
        .plugin_manager
        .list_plugin_snapshots(scope, state_filter);
    let total = snapshots.len() as u64;
    let data = snapshots
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .map(build_plugin_summary)
        .collect::<Vec<_>>();
    let result = PaginationListResult {
        meta: PaginationMeta {
            offset,
            limit,
            total,
        },
        data,
    };

    (StatusCode::OK, Json(result)).into_response()
}

pub async fn plugin_detail(
    State(app_state): State<Arc<YedMQApp>>,
    Path(plugin_name): Path<String>,
) -> Response {
    plugin_detail_response(&app_state, &plugin_name)
}

pub async fn plugin_rescan(State(app_state): State<Arc<YedMQApp>>) -> Response {
    match app_state.plugin_manager.rescan_plugins() {
        Ok(report) => (
            StatusCode::OK,
            Json(PluginRescanResult {
                discovered: report.discovered,
                removed: report.removed,
                invalid: build_invalid_plugins(report.invalid),
            }),
        )
            .into_response(),
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, 101, error.to_string()),
    }
}

pub async fn plugin_start(
    State(app_state): State<Arc<YedMQApp>>,
    Path(plugin_name): Path<String>,
) -> Response {
    match app_state.plugin_manager.start_plugin(&plugin_name).await {
        Ok(()) => plugin_detail_response(&app_state, &plugin_name),
        Err(error) => plugin_control_error_response(error),
    }
}

pub async fn plugin_stop(
    State(app_state): State<Arc<YedMQApp>>,
    Path(plugin_name): Path<String>,
) -> Response {
    match app_state.plugin_manager.stop_plugin(&plugin_name).await {
        Ok(()) => plugin_detail_response(&app_state, &plugin_name),
        Err(error) => plugin_control_error_response(error),
    }
}

pub async fn plugin_restart(
    State(app_state): State<Arc<YedMQApp>>,
    Path(plugin_name): Path<String>,
) -> Response {
    match app_state.plugin_manager.restart_plugin(&plugin_name).await {
        Ok(()) => plugin_detail_response(&app_state, &plugin_name),
        Err(error) => plugin_control_error_response(error),
    }
}

pub async fn plugin_logs(
    State(app_state): State<Arc<YedMQApp>>,
    Path(plugin_name): Path<String>,
    Query(query): Query<PluginLogsQuery>,
) -> Response {
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(100);

    match app_state
        .plugin_manager
        .get_plugin_logs(&plugin_name, offset, limit, query.tail)
        .await
    {
        Ok(PluginLogsPage {
            offset,
            limit,
            total,
            lines,
        }) => {
            let data = lines
                .into_iter()
                .enumerate()
                .map(|(index, message)| PluginLogLine {
                    index: offset + index as u64,
                    message,
                })
                .collect::<Vec<_>>();
            let result = PaginationListResult {
                meta: PaginationMeta {
                    offset,
                    limit,
                    total,
                },
                data,
            };
            (StatusCode::OK, Json(result)).into_response()
        }
        Err(error) => plugin_control_error_response(error),
    }
}
