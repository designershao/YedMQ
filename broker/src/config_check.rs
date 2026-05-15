use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use thiserror::Error;

use crate::settings::{Settings, TcpTls, Wss};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckSeverity {
    Ok,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckResult {
    pub severity: CheckSeverity,
    pub subject: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct CheckReport {
    pub config_path: Option<PathBuf>,
    pub results: Vec<CheckResult>,
}

#[derive(Debug, Error)]
pub enum ConfigCheckError {
    #[error("failed to load config {path}: {source}")]
    Load {
        path: String,
        #[source]
        source: config::ConfigError,
    },
}

impl CheckReport {
    pub fn has_errors(&self) -> bool {
        self.results
            .iter()
            .any(|result| result.severity == CheckSeverity::Error)
    }

    pub fn has_warnings(&self) -> bool {
        self.results
            .iter()
            .any(|result| result.severity == CheckSeverity::Warn)
    }

    pub fn fails(&self, strict: bool) -> bool {
        self.has_errors() || (strict && self.has_warnings())
    }
}

pub fn check_config(path: Option<&Path>) -> Result<CheckReport, ConfigCheckError> {
    let settings = Settings::load(path).map_err(|source| ConfigCheckError::Load {
        path: path
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "./yedmq.toml".to_string()),
        source,
    })?;

    let mut report = CheckReport {
        config_path: path.map(Path::to_path_buf),
        results: Vec::new(),
    };

    report.ok(
        "Config file",
        path.map(|path| path.display().to_string())
            .unwrap_or_else(|| "default search path".to_string()),
    );

    validate_socket(
        &mut report,
        "MQTT TCP listener",
        &settings.listener.tcp.external,
    );
    validate_socket(
        &mut report,
        "MQTT TLS listener",
        &settings.listener.tcp_tls.external,
    );
    validate_socket(
        &mut report,
        "MQTT WS listener",
        &settings.listener.ws.external,
    );
    validate_socket(
        &mut report,
        "MQTT WSS listener",
        &settings.listener.wss.external,
    );
    let api_addr = validate_socket(&mut report, "Admin API", &settings.listener.api.external);
    validate_socket(&mut report, "Cluster RPC", &settings.cluster.rpc.external);
    validate_duplicate_listeners(&mut report, &settings);
    validate_cluster(&mut report, &settings);
    validate_api_auth(&mut report, &settings, api_addr);
    validate_tls_files(&mut report, "MQTT TLS", &settings.listener.tcp_tls);
    validate_wss_files(&mut report, "MQTT WSS", &settings.listener.wss);
    validate_paths(&mut report, &settings);

    Ok(report)
}

impl CheckReport {
    fn push(
        &mut self,
        severity: CheckSeverity,
        subject: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.results.push(CheckResult {
            severity,
            subject: subject.into(),
            message: message.into(),
        });
    }

    fn ok(&mut self, subject: impl Into<String>, message: impl Into<String>) {
        self.push(CheckSeverity::Ok, subject, message);
    }

    fn warn(&mut self, subject: impl Into<String>, message: impl Into<String>) {
        self.push(CheckSeverity::Warn, subject, message);
    }

    fn error(&mut self, subject: impl Into<String>, message: impl Into<String>) {
        self.push(CheckSeverity::Error, subject, message);
    }
}

fn validate_socket(report: &mut CheckReport, subject: &str, value: &str) -> Option<SocketAddr> {
    match value.parse::<SocketAddr>() {
        Ok(addr) => {
            report.ok(subject, value.to_string());
            Some(addr)
        }
        Err(err) => {
            report.error(subject, format!("invalid socket address `{value}`: {err}"));
            None
        }
    }
}

fn validate_duplicate_listeners(report: &mut CheckReport, settings: &Settings) {
    let listeners = [
        ("listener.tcp.external", &settings.listener.tcp.external),
        (
            "listener.tcp_tls.external",
            &settings.listener.tcp_tls.external,
        ),
        ("listener.ws.external", &settings.listener.ws.external),
        ("listener.wss.external", &settings.listener.wss.external),
        ("listener.api.external", &settings.listener.api.external),
        ("cluster.rpc.external", &settings.cluster.rpc.external),
    ];

    let mut seen: HashMap<SocketAddr, &str> = HashMap::new();
    for (name, value) in listeners {
        let Ok(addr) = value.parse::<SocketAddr>() else {
            continue;
        };
        if let Some(previous) = seen.insert(addr, name) {
            report.error(
                "Listener port conflict",
                format!("{name} and {previous} both use {addr}"),
            );
        }
    }
}

fn validate_cluster(report: &mut CheckReport, settings: &Settings) {
    if settings.cluster.node_id == 0 {
        report.error("cluster.node_id", "node id must be greater than 0");
    } else {
        report.ok("cluster.node_id", settings.cluster.node_id.to_string());
    }

    if settings.cluster.nodes.is_empty() {
        report.error("cluster.nodes", "cluster nodes must not be empty");
        return;
    }

    let local_node_exists = settings
        .cluster
        .nodes
        .iter()
        .any(|node| node.id == settings.cluster.node_id);
    if local_node_exists {
        report.ok(
            "cluster.nodes",
            format!("local node {} is present", settings.cluster.node_id),
        );
    } else {
        report.error(
            "cluster.nodes",
            format!(
                "cluster.node_id {} is not present in cluster.nodes",
                settings.cluster.node_id
            ),
        );
    }
}

fn validate_api_auth(report: &mut CheckReport, settings: &Settings, api_addr: Option<SocketAddr>) {
    if settings.listener.api.auth.users.is_empty() {
        report.warn(
            "listener.api.auth.users",
            "Admin API has no users; CLI status commands will receive 401",
        );
    } else {
        report.ok(
            "listener.api.auth.users",
            format!("{} user(s)", settings.listener.api.auth.users.len()),
        );
    }

    if let Some(addr) = api_addr {
        if !addr.ip().is_loopback() {
            report.warn(
                "listener.api.external",
                format!(
                    "Admin API binds to {addr}; Basic Auth over plain HTTP should be protected by TLS, a reverse proxy, or network ACLs"
                ),
            );
        }
    }
}

fn validate_tls_files(report: &mut CheckReport, subject: &str, settings: &TcpTls) {
    validate_file_if_set(report, subject, "cacert_file", &settings.cacert_file);
    validate_file_if_set(report, subject, "cert_file", &settings.cert_file);
    validate_file_if_set(report, subject, "key_file", &settings.key_file);
}

fn validate_wss_files(report: &mut CheckReport, subject: &str, settings: &Wss) {
    validate_file_if_set(report, subject, "cacert_file", &settings.cacert_file);
    validate_file_if_set(report, subject, "cert_file", &settings.cert_file);
    validate_file_if_set(report, subject, "key_file", &settings.key_file);
}

fn validate_file_if_set(report: &mut CheckReport, subject: &str, field: &str, value: &str) {
    if value.trim().is_empty() {
        return;
    }

    let path = Path::new(value);
    if path.is_file() {
        report.ok(format!("{subject} {field}"), value.to_string());
    } else {
        report.warn(
            format!("{subject} {field}"),
            format!("file does not exist: {value}"),
        );
    }
}

fn validate_paths(report: &mut CheckReport, settings: &Settings) {
    let plugin_dir = Path::new(&settings.plugin.dir);
    if plugin_dir.is_dir() {
        report.ok("plugin.dir", settings.plugin.dir.clone());
    } else {
        report.warn(
            "plugin.dir",
            format!(
                "directory does not exist or is not readable: {}",
                settings.plugin.dir
            ),
        );
    }

    let session_clock_path = Path::new(&settings.session.session_clock_path);
    if let Some(parent) = session_clock_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.is_dir() {
            report.warn(
                "session.session_clock_path",
                format!("parent directory does not exist: {}", parent.display()),
            );
        }
    }

    let store_dir = Path::new(&settings.cluster.store_dir);
    if store_dir.is_dir() {
        report.ok("cluster.store_dir", settings.cluster.store_dir.clone());
    } else if let Some(parent) = store_dir.parent() {
        if parent.as_os_str().is_empty() || parent.is_dir() {
            report.warn(
                "cluster.store_dir",
                format!(
                    "directory does not exist and will need to be created: {}",
                    store_dir.display()
                ),
            );
        } else {
            report.warn(
                "cluster.store_dir",
                format!("parent directory does not exist: {}", parent.display()),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    fn write_config(temp_dir: &TempDir, body: &str) -> PathBuf {
        let path = temp_dir.path().join("yedmq.toml");
        fs::write(&path, body).unwrap();
        path
    }

    fn minimal_config(temp_dir: &TempDir) -> String {
        format!(
            r#"
[cluster]
node_id = 1001
nodes = [{{ id = 1001, rpc_address = "127.0.0.1:3457", api_address = "127.0.0.1:3456" }}]
store_dir = "{store_dir}"

[cluster.rpc]
external = "127.0.0.1:3457"

[session]
session_clock_path = "{clock_path}"

[plugin]
dir = "{plugin_dir}"

[listener.api.auth]
users = [{{ username = "admin", password = "password" }}]
"#,
            store_dir = temp_dir.path().join("store").display(),
            clock_path = temp_dir.path().join("clock").display(),
            plugin_dir = temp_dir.path().display()
        )
    }

    #[test]
    fn check_valid_minimal_config_has_no_errors() {
        let temp_dir = TempDir::new().unwrap();
        fs::create_dir_all(temp_dir.path().join("store")).unwrap();
        let path = write_config(&temp_dir, &minimal_config(&temp_dir));

        let report = check_config(Some(&path)).unwrap();

        assert!(!report.has_errors());
    }

    #[test]
    fn check_warns_when_api_users_are_empty() {
        let temp_dir = TempDir::new().unwrap();
        let body = minimal_config(&temp_dir).replace(
            r#"users = [{ username = "admin", password = "password" }]"#,
            "users = []",
        );
        let path = write_config(&temp_dir, &body);

        let report = check_config(Some(&path)).unwrap();

        assert!(report.results.iter().any(|result| {
            result.severity == CheckSeverity::Warn && result.subject == "listener.api.auth.users"
        }));
    }

    #[test]
    fn check_errors_on_invalid_socket_address() {
        let temp_dir = TempDir::new().unwrap();
        let body = format!(
            "{}\n[listener.tcp]\nexternal = \"not a socket\"\n",
            minimal_config(&temp_dir)
        );
        let path = write_config(&temp_dir, &body);

        let report = check_config(Some(&path)).unwrap();

        assert!(report.has_errors());
        assert!(report.results.iter().any(|result| {
            result.severity == CheckSeverity::Error && result.subject == "MQTT TCP listener"
        }));
    }

    #[test]
    fn check_errors_on_duplicate_listener_address() {
        let temp_dir = TempDir::new().unwrap();
        let body = format!(
            "{}\n[listener.tcp]\nexternal = \"127.0.0.1:1883\"\n[listener.ws]\nexternal = \"127.0.0.1:1883\"\n",
            minimal_config(&temp_dir)
        );
        let path = write_config(&temp_dir, &body);

        let report = check_config(Some(&path)).unwrap();

        assert!(report.results.iter().any(|result| {
            result.severity == CheckSeverity::Error && result.subject == "Listener port conflict"
        }));
    }

    #[test]
    fn strict_mode_fails_on_warnings() {
        let temp_dir = TempDir::new().unwrap();
        let path = write_config(&temp_dir, &minimal_config(&temp_dir));

        let report = check_config(Some(&path)).unwrap();

        assert!(report.has_warnings());
        assert!(report.fails(true));
    }
}
