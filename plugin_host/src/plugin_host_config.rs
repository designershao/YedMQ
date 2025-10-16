#[derive(Clone, Debug)]
pub struct PluginHostConfig {
    pub broker_version: String,
    pub broker_node_id: u32,
    pub cluster_name: String,
    pub plugin_directory: String,
    pub local_socket_path: String,
    pub max_restart_attempts: u32,
    pub health_check_interval_secs: u64,
    pub shutdown_signal: tokio::sync::broadcast::Sender<()>,
}