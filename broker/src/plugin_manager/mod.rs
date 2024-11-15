use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use libloading::{Library, Symbol};
use plugin_metadata::PluginMetadata;
use samoye_plugin::plugin::{AuthenticationResult, Client, Plugin, SubscribeAuthorizationResult, SubscribeReturnCode};
use anyhow::{anyhow, Ok};
use thiserror::Error;
use log::{info, warn};

pub mod plugin_metadata;

#[derive(Error, Debug)]
pub enum PluginManagerError {
    #[error("plugin dir not existed")]
    PluginDirNotExisted,

    #[error("load plugin error:`{0}` ")]
    PluginLoadError(String),
}

pub struct PluginManager {

    plugin_dir: String,

    plugin_table: BTreeMap<i64, Arc<PluginWrapper>>

}

pub struct PluginWrapper {

    plugin_metadata: PluginMetadata,

    plugin: Box<dyn Plugin>

}

pub trait PluginService {

    fn do_on_disconnect(&self, client: &Client);

    fn do_on_publish(&self, client: &Client, packet: &samoye_mqtt::v3::publish::PublishPacket);

    fn do_publish_authorizate(&self, client: &Client, packet: &samoye_mqtt::v3::publish::PublishPacket) -> anyhow::Result<bool>;

    fn do_subscribe_authorizate(&self, client: &Client, packet: &samoye_mqtt::v3::subscribe::SubscribePacket) -> anyhow::Result<SubscribeAuthorizationResult>;

    fn do_connect_authenticate(&self, packet: &samoye_mqtt::v3::connect::ConnectPacket) -> anyhow::Result<AuthenticationResult>;
}

impl PluginService for PluginManager {
    
    fn do_on_disconnect(&self, client: &Client) {
        if self.plugin_table.len() > 0 {
            let mut iter = self.plugin_table.iter();
            while let Some(plugin) = iter.next_back() {
                let plugin = plugin.1.clone();
                plugin.plugin.on_disconnect(client);
            }
        }
    }

    fn do_on_publish(&self, client: &Client, packet: &samoye_mqtt::v3::publish::PublishPacket) {
        if self.plugin_table.len() > 0 {
            let mut iter = self.plugin_table.iter();
            while let Some(plugin) = iter.next_back() {
                let plugin = plugin.1.clone();
                plugin.plugin.on_publish(client, packet);
            }
        }
    }

    fn do_publish_authorizate(&self, client: &Client, packet: &samoye_mqtt::v3::publish::PublishPacket) -> anyhow::Result<bool> {
        if self.plugin_table.len() > 0 {
            let mut iter = self.plugin_table.iter();
            while let Some(plugin) = iter.next_back() {
                let plugin = plugin.1.clone();
                let publish_authorizate_result = plugin.plugin.publish_authorizate(client, packet);
                match publish_authorizate_result {
                    core::result::Result::Ok(publish_authorizate_result) => {
                        if publish_authorizate_result {
                            continue;
                        } else {
                            return Ok(false);
                        }
                    }
                    Err(e) => {
                        warn!("plugin {} publish_authorizate error: {}", plugin.plugin_metadata.name, e);
                        continue;
                    }
                }
            }
        }
        Ok(true)
    }

    fn do_subscribe_authorizate(&self, client: &Client, packet: &samoye_mqtt::v3::subscribe::SubscribePacket) -> anyhow::Result<SubscribeAuthorizationResult> {
        let mut return_code = vec![SubscribeReturnCode::MaxQosLeastOnce; packet.payload.topic_filters.len()];
        if self.plugin_table.len() > 0 {
            let mut iter = self.plugin_table.iter();
            while let Some(plugin) = iter.next_back() {
                let plugin = plugin.1.clone();
                let subscribe_authorizate_result = plugin.plugin.subscribe_authorizate(client, packet);
                if let core::result::Result::Ok(subscribe_authorizate_result) = subscribe_authorizate_result {
                    let return_code_from_plugin = subscribe_authorizate_result.return_code;
                    if return_code_from_plugin.len() != return_code.len() { // plugin not return all topic filter permission
                        continue;
                    } else {
                        for i in 0..return_code.len() {
                            if return_code[i] != SubscribeReturnCode::Failure {
                                return_code[i] = return_code_from_plugin[i].clone();
                            }
                        }
                    }
                } else {
                    continue;
                }
            }
        }
        Ok(SubscribeAuthorizationResult{
            return_code
        })
    }

    fn do_connect_authenticate(&self, packet: &samoye_mqtt::v3::connect::ConnectPacket) -> anyhow::Result<AuthenticationResult> {
        if self.plugin_table.len() > 0 {
            let mut i = 1;
            let mut iter = self.plugin_table.iter();
            while let Some(plugin) = iter.next_back() {
                let plugin = plugin.1.clone();
                let authenticate_result = plugin.plugin.connect_authenticate(packet);
                if let core::result::Result::Ok(authenticate_result) = authenticate_result {
                    match authenticate_result {
                        AuthenticationResult::Success(tenant_id) => {
                            return Ok(AuthenticationResult::Success(tenant_id));
                        },
                        AuthenticationResult::Fail(connect_return_code) => {
                            info!("plugin {} on_connect_auth failed, not the last plugin, continue", plugin.plugin_metadata.name);
                            if i >= self.plugin_table.len() { // the lowest priority plugin
                                info!("plugin {} on_connect_auth failed, the last plugin", plugin.plugin_metadata.name);
                                return Ok(AuthenticationResult::Fail(connect_return_code));
                            }
                        },
                    }
                } else {
                    let err = authenticate_result.err().unwrap();
                    warn!("plugin {} on_connect_auth error: {}", plugin.plugin_metadata.name, err);
                }
                i+=1;
            }
            Ok(AuthenticationResult::Fail(samoye_plugin::plugin::ConnectReturnCode::ConnectionForbidenUnauth))
        } else {
            info!("no plugin loaded, pass anonymous");
            Ok(
                AuthenticationResult::Success("public".into())
            )
        }
    }
} 

impl PluginManager {

    fn load_plugin(&mut self, metadata: PluginMetadata) -> anyhow::Result<()> {
        type PluginRegister = unsafe fn() -> *mut dyn samoye_plugin::plugin::Plugin;
        unsafe {
            let path = metadata.get_entry_absolute_path();
            let entry_path = path.to_str().unwrap();
            let lib = Library::new(entry_path).or(Err(PluginManagerError::PluginLoadError("Failed to load plugin library.".into())))?;
            let constructor: Symbol<PluginRegister> = lib.get(b"_plugin_register").or(Err(PluginManagerError::PluginLoadError("The `_plugin_register` symbol was`t found.".into())))?;
            let boxed_raw = constructor();
            let plugin = Box::from_raw(boxed_raw);

            // after plugin load call on activate hook
            plugin.on_activate();
            //

            self.plugin_table.insert(metadata.priority, Arc::new(PluginWrapper{
                plugin_metadata: metadata,
                plugin
            }));

            Ok(())
        }
    }

    pub fn new(plugin_dir: String) -> anyhow::Result<Self> {
        let path = PathBuf::from(plugin_dir.clone());

        if !path.exists() {
            return Err(anyhow!(PluginManagerError::PluginDirNotExisted));
        } else {
            let plugin_table = BTreeMap::new();
            let mut manager = PluginManager{
                plugin_dir,
                plugin_table
            };
            let paths = path.read_dir().unwrap();
            for path in paths {
                let path = path.unwrap().path();
                let metadata_result = plugin_metadata::PluginMetadata::new(path.to_str().unwrap().into());
                match metadata_result {
                    std::result::Result::Ok(metadata) => {
                            let load_plugin_result = manager.load_plugin(metadata).and_then(|_| {
                                Ok(())
                            });

                            if load_plugin_result.is_err() {
                                warn!("load plugin error skip ! path {} error: {}", path.to_str().unwrap(), load_plugin_result.err().unwrap());
                            }
                    }
                    Err(e) => {
                        warn!("load plugin error skip ! path {} error: {}", path.to_str().unwrap(), e);
                    }
                }
            }
            Ok(manager)
        }
    }

}
