use std::{collections::HashMap, path::PathBuf, sync::Arc};

use log::{warn, info};
use thiserror::Error;
use anyhow::{anyhow, Result, Ok};
use tokio::{runtime::Builder, sync::{mpsc::{error::SendError, Sender}, RwLock}};

use super::plugin::{PluginMessage, Plugin};

#[derive(Error, Debug)]
pub enum PluginManagerError {

    #[error("plugin dir not existed")]
    PluginDirNotExisted,

}

#[derive(Debug,PartialEq, Eq)]
pub enum PluginStatus {
    Running,
    Stop,
}

#[derive(Debug)]
pub struct PluginWrapper{
    status: PluginStatus,
    plugin_path: PathBuf,
    plugin_sender: tokio::sync::mpsc::Sender<PluginMessage>,
}

impl PluginWrapper {

    pub async fn stop(&mut self) -> Result<()> {
        self.plugin_sender.send(PluginMessage::Quit).await?;
        self.status = PluginStatus::Stop;
        Ok(())
    }

    pub async fn reload(&mut self) -> Result<()> {
        self.plugin_sender.send(PluginMessage::Quit).await?;
        let plugin = Plugin::new(&self.plugin_path, &tokio::task::LocalSet::new())?;
        self.plugin_sender = plugin; 
        self.status = PluginStatus::Running;
        Ok(())
    }

    pub async fn send(&self, msg: PluginMessage) -> Result<()> {
        self.plugin_sender.send(msg).await?;
        Ok(())
    }

}

pub struct PluginManager {
    tx: tokio::sync::mpsc::Sender<PluginManagerMessage>
}

#[derive(Debug)]
pub enum PluginManagerMessage {
    LoadPlugin(String),
    StartPlugin(String),
    StopPlugin(String),
    GetPlugin(String, tokio::sync::oneshot::Sender<Option<Arc<RwLock<PluginWrapper>>>>),
    GetPluginNames(tokio::sync::oneshot::Sender<Vec<String>>),
    Unload(String),
    Quit
}

impl PluginManager {

    pub async fn new(plugin_path: &String) -> Result<PluginManager> {
        let path = PathBuf::from(plugin_path);
        if !path.exists() {
            return Err(anyhow!(PluginManagerError::PluginDirNotExisted));
        } else {
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            let (tx, mut rx) = tokio::sync::mpsc::channel(1);

            std::thread::spawn(move || {
                let local_set = tokio::task::LocalSet::new();
                local_set.spawn_local(async move {
                    let mut plugin_table = HashMap::new();
                    let paths = path.read_dir().unwrap();
                    for path in paths {
                        let local_set = tokio::task::LocalSet::new();
                        let path = path.unwrap().path();
                        let plugin = Plugin::new(&path, &local_set);
                        if let Err(err) = plugin {
                            warn!("load plugin error skip ! path {} error: {}", path.to_str().unwrap(), err);
                        } else {
                            let plugin_tx = plugin.unwrap();
                            let (plugin_name_tx, plugin_name_rx) = tokio::sync::oneshot::channel();
                            plugin_tx.send(PluginMessage::GetPluginName(plugin_name_tx)).await.unwrap();
                            let plugin_name = plugin_name_rx.await.unwrap();
                            let plugin_wrapper = PluginWrapper {
                                status: PluginStatus::Running,
                                plugin_path: path,
                                plugin_sender: plugin_tx
                            };
                            plugin_table.insert(plugin_name, Arc::new(RwLock::new(plugin_wrapper)));
                        }
                    }
                    loop {
                        let msg = rx.recv().await;
                        match msg {
                            Some(PluginManagerMessage::Quit) => {
                                break;
                            }
                            Some(PluginManagerMessage::LoadPlugin(plugin_path)) => {
                                let local_set = tokio::task::LocalSet::new();
                                let plugin = Plugin::new(&PathBuf::from(plugin_path.clone()), &local_set);
                                if let Err(err) = plugin {
                                    warn!("load plugin error skip ! path {} error: {}", path.to_str().unwrap(), err);
                                } else {
                                    let plugin_tx = plugin.unwrap();
                                    let (plugin_name_tx, plugin_name_rx) = tokio::sync::oneshot::channel();
                                    plugin_tx.send(PluginMessage::GetPluginName(plugin_name_tx)).await.unwrap();
                                    let plugin_name = plugin_name_rx.await.unwrap();
                                    let plugin_wrapper = PluginWrapper {
                                        status: PluginStatus::Running,
                                        plugin_path: PathBuf::from(plugin_path),
                                        plugin_sender: plugin_tx
                                    };
                                    plugin_table.insert(plugin_name, Arc::new(RwLock::new(plugin_wrapper)));
                                }
                            }
                            Some(PluginManagerMessage::StartPlugin(plugin_name)) => {
                                if let Some(plugin)= plugin_table.get_mut(&plugin_name) {
                                    if let Err(e) = plugin.write().await.reload().await {
                                        warn!("plugin {} reload error: {}", plugin_name, e);
                                    }
                                }
                            }
                            Some(PluginManagerMessage::StopPlugin(plugin_name)) => {
                                if let Some(plugin)= plugin_table.get_mut(&plugin_name) {
                                    if let Err(e) = plugin.write().await.stop().await {
                                        warn!("plugin {} stop error: {}", plugin_name, e);
                                    }
                                }
                            }
                            Some(PluginManagerMessage::GetPlugin(plugin_name, tx)) => {
                                if let Some(plugin)= plugin_table.get(&plugin_name) {
                                    tx.send(Some(plugin.clone())).unwrap();
                                }  else {
                                    tx.send(None).unwrap();
                                }
                            }
                            Some(PluginManagerMessage::Unload(plugin_name)) => {
                                if let Some(sender)= plugin_table.get(&plugin_name) {
                                    if let Err(e) = sender.read().await.send(PluginMessage::Quit).await {
                                        warn!("plugin {} quit error: {}", plugin_name, e);
                                    }
                                    plugin_table.remove(&plugin_name);
                                }
                            }
                            Some(PluginManagerMessage::GetPluginNames(tx)) => {
                                let i = plugin_table.keys().map(|k| {
                                    k.clone()
                                }).collect();
                                tx.send(i).unwrap();
                            }
                            None => {
                                break;
                            }
                        }
                    }
                });
                rt.block_on(local_set);
            });
            return Ok(PluginManager { tx });
        }
    }

    pub async fn get_plugin_names(&self) -> Result<Vec<String>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(PluginManagerMessage::GetPluginNames(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn load(&self, plugin_path: &String) -> Result<()> {
        self.tx.send(PluginManagerMessage::LoadPlugin(plugin_path.clone())).await?;
        Ok(())
    }

    pub async fn get_plugin(&self, plugin_name: &String) -> Result<Option<Arc<RwLock<PluginWrapper>>>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(PluginManagerMessage::GetPlugin(plugin_name.clone(), tx)).await?;
        Ok(rx.await?)
    }


}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::plugin::plugin_manager::PluginStatus;

    use super::PluginManager;


    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_manager_init() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path)
            .join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string()).await.unwrap();

        let plugin_names = plugin_manager.get_plugin_names().await.unwrap();

        assert_eq!(1, plugin_names.len());
        assert_eq!(true, plugin_names.contains(&"demo_plugin".to_string()));

    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_manager_load() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path)
            .join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string()).await.unwrap();
        plugin_manager.load(&plugin_path.join("demo_plugin").to_str().unwrap().to_string()).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_status_get() {

        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path)
            .join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string()).await.unwrap();

        let plugin = plugin_manager.get_plugin(&"demo_plugin".to_string()).await.unwrap().unwrap();

        assert_eq!(PluginStatus::Running, plugin.read().await.status);
    }

}