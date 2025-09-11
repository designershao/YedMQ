use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use crate::{loader::PluginManifest, plugin_host_config::PluginHostConfig, ProtocolMessage};

use super::loader::PluginLoader;
use anyhow::Result;

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
    pub manifest: PluginManifest, pub state: PluginState,
    pub process: Option<tokio::process::Child>,
    pub start_time: Option<std::time::Instant>,
    pub restart_count: u32,
    pub last_health_check: Option<std::time::Instant>,
    pub ipc_sender: Option<tokio::sync::mpsc::Sender<ProtocolMessage>>,
}

pub struct PluginManager {
    plugin_loader: PluginLoader,
    running_plugins: Arc<RwLock<HashMap<String, RunningPlugin>>>,
    shutdown_signal: tokio::sync::broadcast::Sender<()>,
}

impl PluginManager {
    pub async fn new(config: PluginHostConfig) -> Result<Self> {
        let mut loader = PluginLoader::new(&config.plugin_directory);

        let _ = loader.scan_plugins().await?;

        Ok(Self {
            plugin_loader: loader,
            running_plugins: Arc::new(RwLock::new(HashMap::new())),
            shutdown_signal: config.shutdown_signal,
        })
    }

    pub async fn start_listener(&self) {
        
    }

    pub async fn start_plugin(&self, name: &str) -> Result<()> {
        let manifest = self.plugin_loader.get_plugin_manifest(name)
            .ok_or_else(|| anyhow::anyhow!("Plugin '{}' not found", name))?.clone();

        let mut command = self.plugin_loader.get_plugin_command(name)?.unwrap();

        let process = command.spawn()?;


        let running_plugin = RunningPlugin {
            name: name.to_string(),
            manifest,
            state: PluginState::Starting,
            process: Some(process),
            start_time: Some(std::time::Instant::now()),
            restart_count: 0,
            last_health_check: None,
            ipc_sender: None,
        };

        {
            let mut plugins = self.running_plugins.write().await;
            plugins.insert(name.to_string(), running_plugin);
        }

        Ok(())
    }
}



