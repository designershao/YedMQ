use std::{collections::HashMap, path::PathBuf};

use log::{warn, info};
use thiserror::Error;
use anyhow::{anyhow, Result, Ok};
use tokio::runtime::Builder;

use super::plugin::{PluginMessage, Plugin};

#[derive(Error, Debug)]
pub enum PluginManagerError {

    #[error("plugin dir not existed")]
    PluginDirNotExisted,

}

pub struct PluginManager {
    plugin_table: HashMap<String, tokio::sync::mpsc::Sender<PluginMessage>>,
}

#[derive(Debug)]
enum PluginManagerMessage {
    PluginStartSucceed((String, tokio::sync::mpsc::Sender<PluginMessage>)), // (plugin name, plugin message sender)
    PluginLoadAllFinished
}

impl PluginManager {
    
    // Create plugin manager instance in new thread
    pub async fn new(plugin_path: &String) -> Result<PluginManager> {
        let path = PathBuf::from(plugin_path);
        if !path.exists() {
            return Err(anyhow!(PluginManagerError::PluginDirNotExisted));
        }

        let (init_tx, mut init_rx) = tokio::sync::mpsc::channel(1);

        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        
        let init_tx_clone = init_tx.clone();

        std::thread::spawn(move || {
            let init_tx = init_tx_clone.clone();
            let local_set = tokio::task::LocalSet::new();
            let paths = path.read_dir().unwrap();
            local_set.spawn_local(async move {
                for path in paths {
                    let init_tx = init_tx.clone();
                    let path = path.unwrap().path();
                    let local_set = tokio::task::LocalSet::new();
                    let plugin = Plugin::new(&path, &local_set);
                    if let Err(err) = plugin {
                        warn!("load plugin error skip ! path {} error: {}", path.to_str().unwrap(), err);
                    } else {
                        let plugin_tx = plugin.unwrap();
                        let (plugin_name_tx, plugin_name_rx) = tokio::sync::oneshot::channel();
                        plugin_tx.send(PluginMessage::GetPluginName(plugin_name_tx)).await.unwrap();
                        let plugin_name = plugin_name_rx.await.unwrap();
                        init_tx.send(PluginManagerMessage::PluginStartSucceed((plugin_name, plugin_tx))).await.unwrap();
                    }
                }
                init_tx.send(PluginManagerMessage::PluginLoadAllFinished).await.unwrap();
            });
            rt.block_on(local_set);
        });

        let mut table = HashMap::new();

        loop {
            let msg = init_rx.recv().await.unwrap();
            match msg {
                PluginManagerMessage::PluginStartSucceed((name, tx)) => {
                    table.insert(name, tx);
                },
                PluginManagerMessage::PluginLoadAllFinished => {
                    break;
                },
            }
        }

        Ok(PluginManager {
            plugin_table: table
        })

    }

    pub fn stop(&self, plugin_name: &String) {
    }

    pub fn start(&self, plugin_name: &String) {
    }

}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::PluginManager;


    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_manager_init() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path)
            .join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string()).await.unwrap();

        assert_eq!(1, plugin_manager.plugin_table.len());
        assert_eq!(true, plugin_manager.plugin_table.contains_key("demo_plugin"));

    }

}