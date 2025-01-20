use axum::{
    extract::State,
    http::StatusCode,
    Json,
};
use serde::Serialize;
use super::AppState;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemInfo {
    clients_connected: u64,

    bytes_received: u64,

    bytes_sent: u64,

    uptime: u64,
}


pub async fn system_info(State(app_state): State<AppState>) -> (StatusCode, Json<SystemInfo>) {
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
