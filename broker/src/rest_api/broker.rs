use std::sync::{atomic::Ordering, Arc};

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};
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

pub async fn metrics(State(app): State<Arc<YedMQApp>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        render_openmetrics(&app),
    )
}

fn render_openmetrics(app: &YedMQApp) -> String {
    let metric = app.metric.as_ref();
    let cluster = escape_label_value(&app.settings.cluster.cluster_name);
    let node_id = app.settings.cluster.node_id.to_string();
    let labels = format!("cluster=\"{cluster}\",node_id=\"{node_id}\"");
    let mut out = String::new();

    push_metric(
        &mut out,
        "yedmq_node_info",
        "Current YedMQ node identity. The value is always 1 for the scraped node.",
        "gauge",
        &labels,
        1,
    );
    push_metric(
        &mut out,
        "yedmq_clients_connected",
        "Current MQTT client connections on this YedMQ node.",
        "gauge",
        &labels,
        metric.clients_connected.load(Ordering::SeqCst),
    );
    push_metric(
        &mut out,
        "yedmq_bytes_received_total",
        "Total bytes received by this YedMQ node.",
        "counter",
        &labels,
        metric.bytes_received.load(Ordering::SeqCst),
    );
    push_metric(
        &mut out,
        "yedmq_bytes_sent_total",
        "Total bytes sent by this YedMQ node.",
        "counter",
        &labels,
        metric.bytes_sent.load(Ordering::SeqCst),
    );
    push_metric(
        &mut out,
        "yedmq_packets_received_total",
        "Total MQTT packets received by this YedMQ node.",
        "counter",
        &labels,
        metric.packets_received.load(Ordering::SeqCst),
    );
    push_metric(
        &mut out,
        "yedmq_packets_sent_total",
        "Total MQTT packets sent by this YedMQ node.",
        "counter",
        &labels,
        metric.packets_sent.load(Ordering::SeqCst),
    );
    push_metric(
        &mut out,
        "yedmq_messages_received_total",
        "Total MQTT publish messages received by this YedMQ node.",
        "counter",
        &labels,
        metric.messages_received.load(Ordering::SeqCst),
    );
    push_metric(
        &mut out,
        "yedmq_messages_sent_total",
        "Total MQTT publish messages sent by this YedMQ node.",
        "counter",
        &labels,
        metric.messages_sent.load(Ordering::SeqCst),
    );
    push_metric(
        &mut out,
        "yedmq_messages_dropped_total",
        "Total MQTT publish messages dropped by this YedMQ node.",
        "counter",
        &labels,
        metric.messages_dropped.load(Ordering::SeqCst),
    );
    push_metric(
        &mut out,
        "yedmq_subscriptions",
        "Current MQTT subscriptions on this YedMQ node.",
        "gauge",
        &labels,
        metric.subscriptions_count.load(Ordering::SeqCst),
    );
    push_metric(
        &mut out,
        "yedmq_uptime_seconds",
        "Current uptime in seconds for this YedMQ node.",
        "gauge",
        &labels,
        metric.get_uptime(),
    );

    out.push_str("# EOF\n");
    out
}

fn push_metric(
    out: &mut String,
    name: &str,
    help: &str,
    metric_type: &str,
    labels: &str,
    value: u64,
) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(metric_type);
    out.push('\n');
    out.push_str(name);
    out.push('{');
    out.push_str(labels);
    out.push_str("} ");
    out.push_str(&value.to_string());
    out.push('\n');
}

fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}
