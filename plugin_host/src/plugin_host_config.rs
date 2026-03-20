use std::time::Duration;

#[derive(Clone, Debug)]
pub struct PluginHostConfig {
    pub broker_version: String,

    pub broker_node_id: u32,

    pub cluster_name: String,

    pub plugin_directory: String,

    pub local_socket_path: String,

    pub max_restart_attempts: u32,

    pub health_check_interval_secs: u64,

    pub init_timeout_secs: u64,

    pub request_timeout_secs: u64,

    pub ping_timeout_secs: u64,

    pub shutdown_signal: tokio::sync::broadcast::Sender<()>,

    /// Default result for authorize calls when no plugin is loaded
    pub default_authorize_result: bool,

    /// Default result for authenticate calls when no plugin is loaded
    pub default_authenticate_result: bool,
}

impl PluginHostConfig {
    pub fn init_timeout(&self) -> Duration {
        Duration::from_secs(self.init_timeout_secs)
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }

    pub fn ping_timeout(&self) -> Duration {
        Duration::from_secs(self.ping_timeout_secs)
    }
}
