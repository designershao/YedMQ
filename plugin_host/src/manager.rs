use crate::{loader::PluginManifest, ProtocolMessage};

use super::loader::PluginLoader;

#[derive(Debug)]
pub enum PluginState {
    Discovered, // Plugin has been discovered but not yet loaded
    Starting,  // Plugin is in the process of starting
    Running,   // Plugin is currently running
    Stopping,  // Plugin is in the process of stopping
    Stopped,   // Plugin has been stopped
    Failed,    // Plugin failed to start
}

pub struct RunningPlugin {
    pub name: String,
    pub manifest: PluginManifest,
    pub state: PluginState,
    pub process: Option<tokio::process::Child>,
    pub start_time: Option<std::time::Instant>,
    pub restart_count: u32,
    pub last_health_check: Option<std::time::Instant>,
    pub ipc_send: Option<tokio::sync::mpsc::Sender<ProtocolMessage>>,
}

pub struct PluginManager {
    plugin_loader: PluginLoader,
    
}