pub struct PluginHostConfig {
    pub plugin_directory: String,
    pub max_restart_attempts: u32,
    pub health_check_interval_secs: u64,
    pub shutdown_signal: tokio::sync::broadcast::Sender<()>,
}