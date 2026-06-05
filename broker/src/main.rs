use std::{
    io::{self, Read},
    path::Path,
    process,
    sync::Arc,
};

use actix::System;
use clap::Parser;
use log::{info, warn};
use serde_json::{json, Value};
use tokio::signal;
use yedmq::{
    admin_client::{AdminClient, AdminClientError},
    app::YedMQApp,
    cli::{
        ApiArgs, BrokerArgs, BrokerCommand, Cli, ClusterArgs, ClusterCommand, Command, ConfigArgs,
        ConfigCommand, NodeArgs, NodeCommand, OutputFormat, StartArgs,
    },
    config_check::{check_config, CheckReport, CheckSeverity, ConfigCheckError},
    settings::Settings,
    version,
};

const EXIT_SUCCESS: i32 = 0;
const EXIT_GENERAL: i32 = 1;
const EXIT_CONFIG: i32 = 3;
const EXIT_ADMIN: i32 = 4;
const EXIT_UNHEALTHY: i32 = 5;

#[actix::main]
async fn main() {
    let cli = Cli::parse();
    let exit_code = run(cli).await;
    process::exit(exit_code);
}

async fn run(cli: Cli) -> i32 {
    match cli.command.unwrap_or(Command::Start(StartArgs {
        config: None,
        log_level: None,
    })) {
        Command::Start(args) => start(args).await,
        Command::Version => print_version(),
        Command::Config(ConfigArgs {
            command: ConfigCommand::Check(args),
        }) => config_check(args.config.as_deref(), args.strict),
        Command::Node(NodeArgs {
            command: NodeCommand::Status(args),
        }) => node_status(args).await,
        Command::Cluster(ClusterArgs {
            command: ClusterCommand::Status(args),
        }) => cluster_status(args).await,
        Command::Broker(BrokerArgs {
            command: BrokerCommand::Stats(args),
        }) => broker_stats(args).await,
    }
}

async fn start(args: StartArgs) -> i32 {
    init_logging(args.log_level.as_deref());

    let settings = match Settings::load(args.config.as_deref()) {
        Ok(settings) => Arc::new(settings),
        Err(err) => {
            let path = args
                .config
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "./yedmq.toml".to_string());
            eprintln!("Error: failed to load config {path}");
            eprintln!("Reason: {err}");
            return EXIT_CONFIG;
        }
    };

    print_startup_summary(args.config.as_deref(), &settings);

    let app = match YedMQApp::new(settings.clone()).await {
        Ok(app) => Arc::new(app),
        Err(err) => {
            eprintln!("Error: failed to start broker");
            eprintln!("Reason: {err:#}");
            return EXIT_GENERAL;
        }
    };
    YedMQApp::start(app.clone()).await;

    match signal::ctrl_c().await {
        Ok(()) => {
            info!("signal received, shutting down");
            if let Err(e) = app.shutdown().await {
                warn!("shutdown error: {}", e);
            }
            System::current().stop();
            EXIT_SUCCESS
        }
        Err(e) => {
            warn!("signal error: {}", e);
            if let Err(shutdown_error) = app.shutdown().await {
                warn!("shutdown error: {}", shutdown_error);
            }
            System::current().stop();
            EXIT_GENERAL
        }
    }
}

fn init_logging(log_level: Option<&str>) {
    let mut builder = env_logger::Builder::from_env(env_logger::Env::default());
    if let Some(log_level) = log_level {
        builder.filter_level(match log_level {
            "trace" => log::LevelFilter::Trace,
            "debug" => log::LevelFilter::Debug,
            "info" => log::LevelFilter::Info,
            "warn" | "warning" => log::LevelFilter::Warn,
            "error" => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        });
    }
    let _ = builder.try_init();
}

fn print_startup_summary(config_path: Option<&Path>, settings: &Settings) {
    println!("YedMQ Broker Starting");
    println!(
        "Config:        {}",
        config_path
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "default search path".to_string())
    );
    println!("Node ID:       {}", settings.cluster.node_id);
    println!("Cluster Name:  {}", settings.cluster.cluster_name);
    println!("MQTT TCP:      {}", settings.listener.tcp.external);
    println!("MQTT TLS:      {}", settings.listener.tcp_tls.external);
    println!("MQTT WS:       {}", settings.listener.ws.external);
    println!("MQTT WSS:      {}", settings.listener.wss.external);
    println!("Admin API:     {}", settings.listener.api.external);
    println!("Cluster RPC:   {}", settings.cluster.rpc.external);
}

fn print_version() -> i32 {
    let info = version::version_info();
    println!("{}", version::format_text(&info));
    EXIT_SUCCESS
}

fn config_check(path: Option<&Path>, strict: bool) -> i32 {
    match check_config(path) {
        Ok(report) => {
            print_config_report(&report);
            if report.fails(strict) {
                EXIT_CONFIG
            } else {
                EXIT_SUCCESS
            }
        }
        Err(ConfigCheckError::Load { path, source }) => {
            eprintln!("Error: failed to load config {path}");
            eprintln!("Reason: {source}");
            EXIT_CONFIG
        }
    }
}

fn print_config_report(report: &CheckReport) {
    println!("Config Check");
    println!("------------");
    for result in &report.results {
        let label = match result.severity {
            CheckSeverity::Ok => "OK",
            CheckSeverity::Warn => "WARN",
            CheckSeverity::Error => "ERROR",
        };
        println!("[{label:<5}] {}: {}", result.subject, result.message);
    }
    println!();
    let result = if report.has_errors() {
        "FAIL"
    } else if report.has_warnings() {
        "PASS with warnings"
    } else {
        "PASS"
    };
    println!("Result: {result}");
}

async fn node_status(args: ApiArgs) -> i32 {
    let output = args.output;
    let client = match build_admin_client(&args) {
        Ok(client) => client,
        Err(code) => return code,
    };

    match client
        .get_json_with_status::<Value>("/api/v1/node/status", true)
        .await
    {
        Ok((status, body)) => {
            print_node_status(&body, output);
            if status.is_success() && value_bool(&body, "clusterReady").unwrap_or(false) {
                EXIT_SUCCESS
            } else {
                EXIT_UNHEALTHY
            }
        }
        Err(AdminClientError::NotFound { .. }) => node_status_fallback(&client, output).await,
        Err(err) => print_admin_error(err),
    }
}

async fn node_status_fallback(client: &AdminClient, output: OutputFormat) -> i32 {
    let system_info = match client.get_json::<Value>("/api/v1/system_info").await {
        Ok(value) => value,
        Err(err) => return print_admin_error(err),
    };
    let (_, cluster_ready) = match client
        .get_json_with_status::<Value>("/api/v1/cluster/ready", true)
        .await
    {
        Ok(value) => value,
        Err(err) => return print_admin_error(err),
    };

    let body = json!({
        "nodeId": cluster_ready.get("nodeId").cloned().unwrap_or(Value::Null),
        "version": Value::Null,
        "clusterName": Value::Null,
        "health": if value_bool(&cluster_ready, "ready").unwrap_or(false) { "healthy" } else { "degraded" },
        "uptimeSeconds": system_info.get("uptime").cloned().unwrap_or(Value::Null),
        "clusterReady": cluster_ready.get("ready").cloned().unwrap_or(Value::Bool(false)),
        "role": Value::Null,
        "listeners": Value::Null,
    });

    print_node_status(&body, output);
    if value_bool(&body, "clusterReady").unwrap_or(false) {
        EXIT_SUCCESS
    } else {
        EXIT_UNHEALTHY
    }
}

async fn cluster_status(args: ApiArgs) -> i32 {
    let output = args.output;
    let client = match build_admin_client(&args) {
        Ok(client) => client,
        Err(code) => return code,
    };

    match client
        .get_json_with_status::<Value>("/api/v1/cluster/status", true)
        .await
    {
        Ok((status, body)) => {
            print_cluster_status(&body, output);
            if status.is_success() && value_bool(&body, "ready").unwrap_or(false) {
                EXIT_SUCCESS
            } else {
                EXIT_UNHEALTHY
            }
        }
        Err(AdminClientError::NotFound { .. }) => cluster_status_fallback(&client, output).await,
        Err(err) => print_admin_error(err),
    }
}

async fn cluster_status_fallback(client: &AdminClient, output: OutputFormat) -> i32 {
    let (_, ready) = match client
        .get_json_with_status::<Value>("/api/v1/cluster/ready", true)
        .await
    {
        Ok(value) => value,
        Err(err) => return print_admin_error(err),
    };

    let body = json!({
        "clusterName": Value::Null,
        "nodeId": ready.get("nodeId").cloned().unwrap_or(Value::Null),
        "ready": ready.get("ready").cloned().unwrap_or(Value::Bool(false)),
        "health": if value_bool(&ready, "ready").unwrap_or(false) { "healthy" } else { "degraded" },
        "members": ready.get("clusterNodeIds").cloned().unwrap_or_else(|| json!([])),
        "raftGroups": [
            fallback_raft_group("topic", ready.pointer("/checks/topicRaft")),
            fallback_raft_group("sessionActorMap", ready.pointer("/checks/sessionActorMapRaft")),
            fallback_raft_group("sessionState", ready.pointer("/checks/sessionStateRaft")),
        ],
        "reasons": ready.get("reasons").cloned().unwrap_or_else(|| json!([])),
    });

    print_cluster_status(&body, output);
    if value_bool(&body, "ready").unwrap_or(false) {
        EXIT_SUCCESS
    } else {
        EXIT_UNHEALTHY
    }
}

async fn broker_stats(args: ApiArgs) -> i32 {
    let output = args.output;
    let client = match build_admin_client(&args) {
        Ok(client) => client,
        Err(code) => return code,
    };

    match client.get_json::<Value>("/api/v1/broker/stats").await {
        Ok(body) => {
            print_broker_stats(&body, output);
            EXIT_SUCCESS
        }
        Err(AdminClientError::NotFound { .. }) => broker_stats_fallback(&client, output).await,
        Err(err) => print_admin_error(err),
    }
}

async fn broker_stats_fallback(client: &AdminClient, output: OutputFormat) -> i32 {
    let system_info = match client.get_json::<Value>("/api/v1/system_info").await {
        Ok(value) => value,
        Err(err) => return print_admin_error(err),
    };

    let body = json!({
        "clientsConnected": system_info.get("clientsConnected").cloned().unwrap_or(Value::Null),
        "bytesReceived": system_info.get("bytesReceived").cloned().unwrap_or(Value::Null),
        "bytesSent": system_info.get("bytesSent").cloned().unwrap_or(Value::Null),
        "packetsReceived": Value::Null,
        "packetsSent": Value::Null,
        "messagesReceived": Value::Null,
        "messagesSent": Value::Null,
        "messagesDropped": Value::Null,
        "subscriptionsCount": Value::Null,
        "uptimeSeconds": system_info.get("uptime").cloned().unwrap_or(Value::Null),
        "sessions": Value::Null,
        "retainedMessages": Value::Null,
        "inflightPackets": Value::Null,
    });
    print_broker_stats(&body, output);
    EXIT_SUCCESS
}

fn build_admin_client(args: &ApiArgs) -> Result<AdminClient, i32> {
    let stdin_password = if args.password_stdin {
        let mut password = String::new();
        if let Err(err) = io::stdin().read_to_string(&mut password) {
            eprintln!("Error: failed to read password from stdin");
            eprintln!("Reason: {err}");
            return Err(EXIT_ADMIN);
        }
        Some(password.trim_end_matches(['\r', '\n']).to_string())
    } else {
        None
    };

    AdminClient::new(args, stdin_password).map_err(print_admin_error)
}

fn print_admin_error(err: AdminClientError) -> i32 {
    match err {
        AdminClientError::Unauthorized { url } => {
            eprintln!("Error: Admin API authentication failed: {url}");
            eprintln!(
                "Hint: check --user/--password-stdin or [listener.api.auth].users in yedmq.toml."
            );
            EXIT_ADMIN
        }
        AdminClientError::MissingUsername
        | AdminClientError::MissingPassword
        | AdminClientError::InvalidTimeout { .. }
        | AdminClientError::BuildClient(_)
        | AdminClientError::Request { .. }
        | AdminClientError::NotFound { .. } => {
            eprintln!("Error: {err}");
            EXIT_ADMIN
        }
        AdminClientError::Status { .. } | AdminClientError::Decode { .. } => {
            eprintln!("Error: {err}");
            EXIT_UNHEALTHY
        }
    }
}

fn print_node_status(body: &Value, output: OutputFormat) {
    if output == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(body).unwrap());
        return;
    }

    println!("Node Status");
    println!("-----------");
    println!("Node ID:        {}", text_value(body.get("nodeId")));
    println!("Version:        {}", text_value(body.get("version")));
    println!("Health:         {}", text_value(body.get("health")));
    println!(
        "Uptime:         {}",
        format_uptime(body.get("uptimeSeconds"))
    );
    println!("Cluster Ready:  {}", text_value(body.get("clusterReady")));
    println!("Role:           {}", text_value(body.get("role")));
    let api = body.pointer("/listeners/api");
    println!("Admin API:      {}", text_value(api));
}

fn print_cluster_status(body: &Value, output: OutputFormat) {
    if output == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(body).unwrap());
        return;
    }

    println!("Cluster Status");
    println!("--------------");
    println!("Cluster:  {}", text_value(body.get("clusterName")));
    println!("Node ID:  {}", text_value(body.get("nodeId")));
    println!("Health:   {}", text_value(body.get("health")));
    println!("Ready:    {}", text_value(body.get("ready")));
    println!();
    println!("Raft Groups:");
    if let Some(groups) = body.get("raftGroups").and_then(Value::as_array) {
        for group in groups {
            println!(
                "  {:<18} leader={}  ready={}{}",
                text_value(group.get("name")),
                text_value(group.get("leaderId")),
                text_value(group.get("ready")),
                group
                    .get("payloadReady")
                    .and_then(Value::as_bool)
                    .map(|value| format!("  payloadReady={value}"))
                    .unwrap_or_default()
            );
        }
    }
    if let Some(reasons) = body.get("reasons").and_then(Value::as_array) {
        if !reasons.is_empty() {
            println!();
            println!("Reasons:");
            for reason in reasons {
                println!("  - {}", text_value(Some(reason)));
            }
        }
    }
}

fn print_broker_stats(body: &Value, output: OutputFormat) {
    if output == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(body).unwrap());
        return;
    }

    println!("Broker Stats");
    println!("------------");
    println!(
        "Clients Connected:   {}",
        text_value(body.get("clientsConnected"))
    );
    println!(
        "Bytes Received:      {}",
        text_value(body.get("bytesReceived"))
    );
    println!("Bytes Sent:          {}", text_value(body.get("bytesSent")));
    println!(
        "Packets Received:    {}",
        text_value(body.get("packetsReceived"))
    );
    println!(
        "Packets Sent:        {}",
        text_value(body.get("packetsSent"))
    );
    println!(
        "Messages Received:   {}",
        text_value(body.get("messagesReceived"))
    );
    println!(
        "Messages Sent:       {}",
        text_value(body.get("messagesSent"))
    );
    println!(
        "Messages Dropped:    {}",
        text_value(body.get("messagesDropped"))
    );
    println!(
        "Subscriptions:       {}",
        text_value(body.get("subscriptionsCount"))
    );
    println!("Sessions:            {}", text_value(body.get("sessions")));
    println!(
        "Retained Messages:   {}",
        text_value(body.get("retainedMessages"))
    );
    println!(
        "Inflight Packets:    {}",
        text_value(body.get("inflightPackets"))
    );
    println!(
        "Uptime:              {}",
        format_uptime(body.get("uptimeSeconds"))
    );
}

fn fallback_raft_group(name: &str, value: Option<&Value>) -> Value {
    json!({
        "name": name,
        "ready": value.and_then(|value| value.get("ready")).cloned().unwrap_or(Value::Bool(false)),
        "leaderId": value.and_then(|value| value.get("leaderId")).cloned().unwrap_or(Value::Null),
        "membershipNodeIds": value.and_then(|value| value.get("membershipNodeIds")).cloned().unwrap_or_else(|| json!([])),
        "payloadReady": value.and_then(|value| value.get("payloadReady")).cloned().unwrap_or(Value::Null),
        "reason": value.and_then(|value| value.get("reason")).cloned().unwrap_or(Value::Null),
    })
}

fn value_bool(body: &Value, field: &str) -> Option<bool> {
    body.get(field).and_then(Value::as_bool)
}

fn text_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Null) | None => "unknown".to_string(),
        Some(value) => value.to_string(),
    }
}

fn format_uptime(value: Option<&Value>) -> String {
    let Some(seconds) = value.and_then(Value::as_u64) else {
        return "unknown".to_string();
    };
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let remaining_seconds = seconds % 60;
    if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m {remaining_seconds}s")
    } else {
        format!("{remaining_seconds}s")
    }
}
