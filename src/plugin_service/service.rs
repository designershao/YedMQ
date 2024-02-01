use std::{path::PathBuf, sync::{Arc, RwLock}};

use crate::{plugin_service::plugin::plugin_context::{Authentication, PermissionType}, protocol::v3::publish::PublishPacket};

use super::plugin::{plugin_host::PluginHost, plugin_context::{self, Authorization, CallPluginError, TopicInfo, TopicPermission}};
use log::{warn, info};
use rune::alloc::BTreeMap;
use thiserror::Error;
use anyhow::{anyhow, Ok};

#[derive(Error, Debug)]
pub enum PluginServiceError {
    #[error("plugin dir not existed")]
    PluginDirNotExisted,
}

pub struct PluginService {
    inner: BTreeMap<i64, Arc<RwLock<PluginHost>>>,
    plugin_dict_path: PathBuf,
}

impl PluginService {

    pub fn new(plugin_dict: String) -> anyhow::Result<PluginService> {

        let path = PathBuf::from(plugin_dict);
        if !path.exists() {
            return Err(anyhow!(PluginServiceError::PluginDirNotExisted));
        } else {
            let mut inner = BTreeMap::new();
            let paths = path.read_dir().unwrap();
            for path in paths {
                let path = path.unwrap().path();
                let plugin = PluginHost::load(path.to_str().unwrap().into());
            if let core::result::Result::Ok(plugin) = plugin {
                    info!("load plugin {} success ! path {}",plugin.get_plugin_name(), path.to_str().unwrap());
                    inner.try_insert(plugin.get_plugin_priority(),Arc::new(RwLock::new(plugin)))?;
                } else {
                    warn!("load plugin error skip ! path {} error: {}", path.to_str().unwrap(), plugin.err().unwrap());
                }
            }
            Ok(
                PluginService {
                    inner,
                    plugin_dict_path: path,
                }
            )
        }
    }

    fn get_highest_priority_plugin(&mut self) -> Option<Arc<RwLock<PluginHost>>> {
        self.inner.last_entry().map(|mut e| e.get_mut().clone())
    }

    pub fn on_connect_auth(&mut self, connect_info: plugin_context::ConnectInfo) -> anyhow::Result<Authentication> {
        if self.inner.len() > 0 {
            let mut i = 1;
            while let Some(plugin) = self.inner.iter().next_back() {
                let plugin = plugin.1.clone();
                let auth_response = plugin.write().unwrap().on_connect_auth(connect_info.clone());
                if let core::result::Result::Ok(auth_response) = auth_response {
                    match auth_response {
                        Authentication::Allow(tenant_id) => {
                            return Ok(Authentication::Allow(tenant_id));
                        }
                        Authentication::Deny(details, return_code) => {
                            info!("plugin {} on_connect_auth failed, not the last plugin, continue", plugin.read().unwrap().get_plugin_name());
                            if i >= self.inner.len() { // the lowest priority plugin
                                info!("plugin {} on_connect_auth failed, the last plugin", plugin.read().unwrap().get_plugin_name());
                                return Ok(Authentication::Deny(details, return_code));
                            }
                        }
                    }
                } else {
                    let err = auth_response.err().unwrap();
                    if i >= self.inner.len() { // the lowest priority plugin
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
    
    pub async fn on_topic_permission_check(&mut self,client_info: plugin_context::ClientInfo,topic_info:TopicInfo) -> anyhow::Result<Authorization> {
        if self.inner.len() > 0 {
            let mut i = 1;
            while let Some(plugin) = self.inner.iter().next_back() {
                let plugin = plugin.1.clone();
                let auth_response = plugin.write().unwrap().on_topic_permission_check(client_info.clone(), topic_info.clone()).await;
                if let core::result::Result::Ok(auth_response) = auth_response {
                    match auth_response {
                        Authorization::Allow => {
                            return Ok(Authorization::Allow);
                        }
                        Authorization::Deny => {
                            info!("plugin {} on_topic_permission_check failed, not the last plugin, continue", plugin.read().unwrap().get_plugin_name());
                            if i >= self.inner.len() { // the lowest priority plugin
                                info!("plugin {} on_topic_permission_check failed, the last plugin", plugin.read().unwrap().get_plugin_name());
                                return Ok(Authorization::Deny);
                            }
                        }
                    }
                } else {
                    let err = auth_response.err().unwrap();
                    if i >= self.inner.len() { // the lowest priority plugin
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


    pub async fn on_publish(&mut self, client_info: plugin_context::ClientInfo, packet: PublishPacket) -> anyhow::Result<()> {
        if self.inner.len() > 0 {
            while let Some(plugin) = self.inner.iter().next_back() {
                let plugin = plugin.1.clone();
                plugin.write().unwrap().on_publish(client_info.clone(), packet.clone()).await?;
            }
            Ok(())
        } else {
            Ok(())
        }
    }

}
