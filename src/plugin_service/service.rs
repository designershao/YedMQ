use std::{collections::BTreeMap, path::PathBuf, sync::{Arc, RwLock}};

use crate::{plugin_service::plugin::plugin_context::{Authentication, PermissionType}, protocol::v3::publish::PublishPacket};

use super::plugin::{plugin_host::PluginHost, plugin_context::{self, Authorization, CallPluginError, ClientInfo, ConnectInfo, TopicInfo, TopicPermission}};
use log::{warn, info};
use thiserror::Error;
use anyhow::{anyhow, Ok};
use tokio::runtime::Builder;

pub enum PluginServiceMessage {
    OnConnectAuth(ConnectInfo, tokio::sync::oneshot::Sender<anyhow::Result<Authentication>>),
    OnPublish(ClientInfo, PublishPacket),
    OnTopicPermissionCheck(ClientInfo, TopicInfo, tokio::sync::oneshot::Sender<anyhow::Result<Authorization>>),
    Quit,
}

#[derive(Error, Debug)]
pub enum PluginServiceError {
    #[error("plugin dir not existed")]
    PluginDirNotExisted,
}

pub struct PluginService {
    tx: tokio::sync::mpsc::Sender<PluginServiceMessage>,
}

impl PluginService {
    pub fn new(plugin_dict: String) -> anyhow::Result<PluginService> {

        let path = PathBuf::from(plugin_dict);

        if !path.exists() {
            return Err(anyhow!(PluginServiceError::PluginDirNotExisted));
        } else {
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);

            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            
            std::thread::spawn(move || {
                let local_set = tokio::task::LocalSet::new();
                local_set.spawn_local(async move {
                    let mut plugin_table = BTreeMap::new();
                    let paths = path.read_dir().unwrap();
                    for path in paths {
                        let path = path.unwrap().path();
                        let plugin = PluginHost::load(path.to_str().unwrap().into());
                        if let core::result::Result::Ok(mut plugin) = plugin {
                            plugin.init().unwrap();
                            info!("load plugin {} success ! path {}",plugin.get_plugin_name(), path.to_str().unwrap());
                            plugin_table.insert(plugin.get_plugin_priority(),Arc::new(RwLock::new(plugin)));
                        } else {
                            warn!("load plugin error skip ! path {} error: {}", path.to_str().unwrap(), plugin.err().unwrap());
                        }
                    }

                    loop {
                        let msg = rx.recv().await;

                        match msg {
                            Some(PluginServiceMessage::OnConnectAuth(connect_info, tx)) => {
                                let r = Self::on_connect_auth(&plugin_table, connect_info);
                                tx.send(r).unwrap();
                            }
                            Some(PluginServiceMessage::OnPublish(client_info, packet)) => {
                                Self::on_publish(&plugin_table, client_info, packet).await;
                            }
                            Some(PluginServiceMessage::OnTopicPermissionCheck(client_info, topic_info, tx)) => {
                                let r = Self::on_topic_permission_check(&plugin_table, client_info, topic_info).await;
                                tx.send(r).unwrap();
                            }
                            Some(PluginServiceMessage::Quit) => {
                                rx.close();
                            }
                            None => {
                                break;
                            }
                        }

                    }
                });
                rt.block_on(local_set);
            });

            Ok(
                PluginService {
                    tx,
                }
            )
        }
    }

    fn on_connect_auth(plugin_table: &BTreeMap<i64, Arc<RwLock<PluginHost>>>, connect_info: plugin_context::ConnectInfo) -> anyhow::Result<Authentication> {
        if plugin_table.len() > 0 {
            let mut i = 1;
            while let Some(plugin) = plugin_table.iter().next_back() {
                let plugin = plugin.1.clone();
                let auth_response = plugin.write().unwrap().on_connect_auth(connect_info.clone());
                if let core::result::Result::Ok(auth_response) = auth_response {
                    match auth_response {
                        Authentication::Allow(tenant_id) => {
                            return Ok(Authentication::Allow(tenant_id));
                        }
                        Authentication::Deny(details, return_code) => {
                            info!("plugin {} on_connect_auth failed, not the last plugin, continue", plugin.read().unwrap().get_plugin_name());
                            if i >= plugin_table.len() { // the lowest priority plugin
                                info!("plugin {} on_connect_auth failed, the last plugin", plugin.read().unwrap().get_plugin_name());
                                return Ok(Authentication::Deny(details, return_code));
                            }
                        }
                    }
                } else {
                    let err = auth_response.err().unwrap();
                    warn!("plugin {} on_connect_auth error: {}", plugin.read().unwrap().get_plugin_name(), err);
                    if i >= plugin_table.len() { // the lowest priority plugin
                        match err.downcast().unwrap() {
                            CallPluginError::HookNotRegister(hook_name) => {
                                info!("plugin {} failed, no hook {} call back register, not the last plugin, continue", plugin.read().unwrap().get_plugin_name(), hook_name);
                                // no on connect auth hook callback, pass anonymous
                                return Ok(Authentication::Allow(
                                    "public".into(),
                                ));
                            }
                        }
                    } else {
                        continue;
                    }
                }
                i += 1;
            }
            Ok(Authentication::Deny("unexpect error".into(), plugin_context::ReturnCode::RefusedInteralError))
        } else {
            info!("no plugin loaded, pass anonymous");
            Ok(
                Authentication::Allow("public".into())
            )
        }
    }
    
    async fn on_topic_permission_check(plugin_table: &BTreeMap<i64, Arc<RwLock<PluginHost>>>,client_info: plugin_context::ClientInfo,topic_info:TopicInfo) -> anyhow::Result<Authorization> {
        if plugin_table.len() > 0 {
            let mut i = 1;
            while let Some(plugin) = plugin_table.iter().next_back() {
                let plugin = plugin.1.clone();
                let auth_response = plugin.write().unwrap().on_topic_permission_check(client_info.clone(), topic_info.clone()).await;
                if let core::result::Result::Ok(auth_response) = auth_response {
                    match auth_response {
                        Authorization::Allow => {
                            return Ok(Authorization::Allow);
                        }
                        Authorization::Deny => {
                            info!("plugin {} on_topic_permission_check failed, not the last plugin, continue", plugin.read().unwrap().get_plugin_name());
                            if i >= plugin_table.len() { // the lowest priority plugin
                                info!("plugin {} on_topic_permission_check failed, the last plugin", plugin.read().unwrap().get_plugin_name());
                                return Ok(Authorization::Deny);
                            }
                        }
                    }
                } else {
                    let err = auth_response.err().unwrap();
                    if i >= plugin_table.len() { // the lowest priority plugin
                        match err.downcast().unwrap() {
                            CallPluginError::HookNotRegister(hook_name) => {
                                info!("plugin {} failed, no hook {} call back register, not the last plugin, continue", plugin.read().unwrap().get_plugin_name(), hook_name);
                                // no on connect auth hook callback, pass anonymous
                                return Ok(Authorization::Allow);
                            }
                            e => {
                                return Ok(Authorization::Deny);
                            }
                        }
                    } else {
                        continue;
                    }
                }
                i += 1;
            }
            Ok(Authorization::Deny)
       } else {
            info!("no plugin loaded, pass anonymous");
            return Ok(Authorization::Allow);
       }
    }


    async fn on_publish(plugin_table: &BTreeMap<i64, Arc<RwLock<PluginHost>>>, client_info: plugin_context::ClientInfo, packet: PublishPacket) -> anyhow::Result<()> {
        if plugin_table.len() > 0 {
            while let Some(plugin) = plugin_table.iter().next_back() {
                let plugin = plugin.1.clone();
                let _ = plugin.write().unwrap().on_publish(client_info.clone(), packet.clone()).await;
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn call_on_publish(&self, client_info: plugin_context::ClientInfo, packet: PublishPacket) -> anyhow::Result<()> {
        let _ = self.tx.send(PluginServiceMessage::OnPublish(client_info, packet )).await;
        Ok(())
    }

    pub async fn call_on_connect_auth(&self, connect_info: plugin_context::ConnectInfo) -> anyhow::Result<plugin_context::Authentication> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.tx.send(PluginServiceMessage::OnConnectAuth(connect_info, tx )).await;
        rx.await.unwrap()
    }

    pub async fn call_on_topic_permiession_check(&self, client_info: plugin_context::ClientInfo, topic_info: plugin_context::TopicInfo) -> anyhow::Result<plugin_context::Authorization> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.tx.send(PluginServiceMessage::OnTopicPermissionCheck(client_info, topic_info, tx )).await;
        rx.await.unwrap()
    }

}
