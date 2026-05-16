use std::sync::{atomic::Ordering, Arc};

use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

use crate::app::YedMQApp;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerStatsResponse {
    pub clients_connected: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub packets_received: u64,
    pub packets_sent: u64,
    pub messages_received: u64,
    pub messages_sent: u64,
    pub messages_dropped: u64,
    pub subscriptions_count: u64,
    pub uptime_seconds: u64,
    pub sessions: Option<u64>,
    pub retained_messages: Option<u64>,
    pub inflight_packets: Option<u64>,
}

pub async fn stats(State(app): State<Arc<YedMQApp>>) -> (StatusCode, Json<BrokerStatsResponse>) {
    let metric = app.metric.clone();
    let response = BrokerStatsResponse {
        clients_connected: metric.clients_connected.load(Ordering::SeqCst),
        bytes_received: metric.bytes_received.load(Ordering::SeqCst),
        bytes_sent: metric.bytes_sent.load(Ordering::SeqCst),
        packets_received: metric.packets_received.load(Ordering::SeqCst),
        packets_sent: metric.packets_sent.load(Ordering::SeqCst),
        messages_received: metric.messages_received.load(Ordering::SeqCst),
        messages_sent: metric.messages_sent.load(Ordering::SeqCst),
        messages_dropped: metric.messages_dropped.load(Ordering::SeqCst),
        subscriptions_count: metric.subscriptions_count.load(Ordering::SeqCst),
        uptime_seconds: metric.get_uptime(),
        sessions: None,
        retained_messages: None,
        inflight_packets: None,
    };

    (StatusCode::OK, Json(response))
}
