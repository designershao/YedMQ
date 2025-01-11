use std::{collections::BTreeMap, ffi::CStr, path::PathBuf, sync::Arc};

use libloading::{Library, Symbol};
use plugin_metadata::PluginMetadata;
use yedmq_plugin::plugin::{AuthenticationResult, AuthenticationResultValue, AuthorizationResult, Client, Plugin, RegisterPluginResult};
use anyhow::{anyhow, Ok};
use thiserror::Error;
use log::{debug, info, warn};

use crate::settings::{DefaultAuthorizationValue, Settings};

pub mod plugin_metadata;

pub struct SubscribeAuthorizationResult {

    pub return_code: Vec<SubscribeReturnCode>

}

#[derive(Clone, PartialEq, Debug)]
pub enum SubscribeReturnCode {

    MaxQosMostOnce,

    MaxQosLeastOnce,

    MaxQosExactlyOnce,
    
    Failure,

    Invalid

}


#[derive(Error, Debug)]
pub enum PluginManagerError {
    #[error("plugin dir not existed")]
    PluginDirNotExisted,

    #[error("load plugin error:`{0}` ")]
    PluginLoadError(String),
}

pub struct PluginManager {

    plugin_dir: String,

    plugin_table: BTreeMap<i64, Arc<PluginWrapper>>,

    settings: Arc<Settings>

}

pub struct PluginWrapper {

    plugin_metadata: PluginMetadata,

    plugin: Box<dyn Plugin>

}

pub trait PluginService: Send + Sync {

    fn do_on_disconnect(&self, client: &Client);

    fn do_on_publish(&self, client: &Client, packet: &yedmq_mqtt::v3::publish::PublishPacket);

    fn do_publish_authorizate(&self, client: &Client, packet: &yedmq_mqtt::v3::publish::PublishPacket) -> anyhow::Result<bool>;

    fn do_subscribe_authorizate(&self, client: &Client, packet: &yedmq_mqtt::v3::subscribe::SubscribePacket) -> anyhow::Result<SubscribeAuthorizationResult>;

    fn do_connect_authenticate(&self, packet: &yedmq_mqtt::v3::connect::ConnectPacket) -> anyhow::Result<AuthenticationResultValue>;
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

    fn do_on_publish(&self, client: &Client, packet: &yedmq_mqtt::v3::publish::PublishPacket) {
        if self.plugin_table.len() > 0 {
            let mut iter = self.plugin_table.iter();
            while let Some(plugin) = iter.next_back() {
                let plugin = plugin.1.clone();
                plugin.plugin.on_publish(client, packet);
            }
        }
    }

    fn do_publish_authorizate(&self, client: &Client, packet: &yedmq_mqtt::v3::publish::PublishPacket) -> anyhow::Result<bool> {
        if self.plugin_table.len() > 0 {
            let mut iter = self.plugin_table.iter();
            while let Some(plugin) = iter.next_back() {
                let plugin = plugin.1.clone();
                let publish_authorizate_result = plugin.plugin.authorizate_acl_check(client, &packet.variable_header.topic_name, yedmq_plugin::plugin::Action::Publish);
                match publish_authorizate_result {
                    core::result::Result::Ok(publish_authorizate_result) => {
                        match publish_authorizate_result {
                            AuthorizationResult::Result(result) => {
                                return Ok(result)
                            },
                            AuthorizationResult::Next() => {
                                info!("plugin {} authorizate_acl_check return next(), continue", plugin.plugin_metadata.name);
                                continue
                            },
                        }
                    }
                    Err(e) => {
                        match e.downcast_ref() {
                            Some(yedmq_plugin::plugin::PluginError::PluginHookNotImplement()) => {
                                info!("plugin {} hook publish_authorizate not implement skip!", plugin.plugin_metadata.name);
                            }
                            Some(yedmq_plugin::plugin::PluginError::PluginHookExecutionError(e)) => {
                                warn!("plugin {} publish_authorizate error: {}", plugin.plugin_metadata.name, e);
                            }
                            None => {}
                        }
                        continue;
                    }
                }
            }
        }
        let default_result = match self.settings.mqtt.default_authorization {
            DefaultAuthorizationValue::Allow => true,
            DefaultAuthorizationValue::Deny => false
        };
        Ok(default_result)
    }

    fn do_subscribe_authorizate(&self, client: &Client, packet: &yedmq_mqtt::v3::subscribe::SubscribePacket) -> anyhow::Result<SubscribeAuthorizationResult> {
        let default_return_code = match self.settings.mqtt.default_authorization {
            DefaultAuthorizationValue::Allow => SubscribeReturnCode::MaxQosLeastOnce,
            DefaultAuthorizationValue::Deny => SubscribeReturnCode::Failure
        };
        let mut return_code: Vec<SubscribeReturnCode> = vec![default_return_code; packet.payload.topic_filters.len()];
        if self.plugin_table.len() > 0 {
            let mut iter = self.plugin_table.iter();
            while let Some(plugin) = iter.next_back() {
                let plugin = plugin.1.clone();
                let topics: std::iter::Enumerate<std::slice::Iter<'_, yedmq_mqtt::v3::subscribe::TopicFilter>> = packet.payload.topic_filters.iter().enumerate();
                for (i, topic) in topics {
                    let authorizate_result = plugin.plugin.authorizate_acl_check(client, &topic.topic_name, yedmq_plugin::plugin::Action::Subscribe);
                    match authorizate_result {
                        core::result::Result::Ok(authorizate_result) => {
                            match authorizate_result {
                                AuthorizationResult::Result(result) => {
                                    return_code[i] = match result {
                                        true =>  {
                                            match topic.qos {
                                                0 => SubscribeReturnCode::MaxQosLeastOnce,
                                                1 => SubscribeReturnCode::MaxQosMostOnce,
                                                2 => SubscribeReturnCode::MaxQosExactlyOnce,
                                                _ => SubscribeReturnCode::Invalid
                                            }
                                        },
                                        false => SubscribeReturnCode::Failure
                                    };
                                },
                                AuthorizationResult::Next() => {
                                    info!("plugin {} authorizate_acl_check return next(), continue", plugin.plugin_metadata.name);
                                    continue
                                },
                            }
                        }
                        Err(e) => {
                            match e.downcast_ref() {
                                Some(yedmq_plugin::plugin::PluginError::PluginHookNotImplement()) => {
                                    info!("plugin {} hook subscribe_authorizate not implement skip!", plugin.plugin_metadata.name);
                                }
                                Some(yedmq_plugin::plugin::PluginError::PluginHookExecutionError(e)) => {
                                    warn!("plugin {} subscribe_authorizate error: {}", plugin.plugin_metadata.name, e);
                                }
                                None => {}
                            }
                        }
                    }
                }
            }
        }
        Ok(SubscribeAuthorizationResult{
            return_code
        })
    }

    // Connect authenticate logic
    // Execute plugins in order of priority from high to low. 
    // If higher priority plugin returns a success result, then return the result, lower plugin will not be called.
    fn do_connect_authenticate(&self, packet: &yedmq_mqtt::v3::connect::ConnectPacket) -> anyhow::Result<AuthenticationResultValue> {
        if self.plugin_table.len() > 0 {
            let mut i = 1;
            let mut iter = self.plugin_table.iter();
            while let Some(plugin) = iter.next_back() {
                let plugin = plugin.1.clone();
                let authenticate_result = plugin.plugin.connect_authenticate(packet);

                if let core::result::Result::Ok(authenticate_result) = authenticate_result {
                    match authenticate_result {
                        AuthenticationResult::Result(authentication_result_value) => {
                            match authentication_result_value {
                                AuthenticationResultValue::Success(tenant_id) => {
                                    return Ok(AuthenticationResultValue::Success(tenant_id));
                                },
                                AuthenticationResultValue::Fail(connect_return_code) => {
                                    info!("plugin {} on_connect_auth failed, not the last plugin, continue", plugin.plugin_metadata.name);
                                    if i >= self.plugin_table.len() { // the lowest priority plugin
                                        info!("plugin {} on_connect_auth failed, the last plugin", plugin.plugin_metadata.name);
                                        return Ok(AuthenticationResultValue::Fail(connect_return_code));
                                    }
                                },
                            }
                        },
                        AuthenticationResult::Next() => {
                            info!("plugin {} on_connect_auth return next(), continue", plugin.plugin_metadata.name);
                            continue;
                        },
                    }
                } else {

                    match authenticate_result.err().unwrap().downcast_ref() {
                        Some(yedmq_plugin::plugin::PluginError::PluginHookNotImplement()) => {
                            debug!("plugin {} on_connect_auth not implement skip!", plugin.plugin_metadata.name);
                        }
                        Some(yedmq_plugin::plugin::PluginError::PluginHookExecutionError(err)) => {
                            warn!("plugin {} on_connect_auth error: {}, fobiden connection", plugin.plugin_metadata.name, err);
                        }
                        None => {}
                    }
                }
                i+=1;
            }

            // No other plugins return the default result
            let result =  match self.settings.mqtt.default_authentication {
                crate::settings::DefaultAuthenticationValue::Allow => AuthenticationResultValue::Success("public".into()),
                crate::settings::DefaultAuthenticationValue::Deny => AuthenticationResultValue::Fail(yedmq_plugin::plugin::ConnectReturnCode::ConnectionForbidenUnauth),
            };
            //
            Ok(result)
        } else {
            info!("no plugin loaded, return the default authenticate result {:?}", self.settings.mqtt.default_authentication);
            // No other plugins return the default result
            let result =  match self.settings.mqtt.default_authentication {
                crate::settings::DefaultAuthenticationValue::Allow => AuthenticationResultValue::Success("public".into()),
                crate::settings::DefaultAuthenticationValue::Deny => AuthenticationResultValue::Fail(yedmq_plugin::plugin::ConnectReturnCode::ConnectionForbidenUnauth),
            };
            //
            Ok(result)
        }
    }
} 

impl PluginManager {

    fn load_plugin(&mut self, metadata: PluginMetadata) -> anyhow::Result<()> {
        type PluginRegister = unsafe fn(yedmq_plugin::context::Context) -> RegisterPluginResult;
        unsafe {
            let path = metadata.get_entry_absolute_path();
            let entry_path = path.to_str().unwrap();
            let lib = Library::new(entry_path).or(Err(PluginManagerError::PluginLoadError("Failed to load plugin library.".into())))?;
            let constructor: Symbol<PluginRegister> = lib.get(b"_plugin_register").or(Err(PluginManagerError::PluginLoadError("The `_plugin_register` symbol was`t found.".into())))?;
            let context = yedmq_plugin::context::Context::new(metadata.plugin_absolute_path.to_string_lossy().to_string().as_str());
            let plugin_constructor_result = constructor(context);
            if plugin_constructor_result.error_code != 0 {
                warn!("plugin {} constructor error, skip this plugin, error: {}", metadata.name, String::from_utf8_lossy(CStr::from_ptr(plugin_constructor_result.error_msg).to_bytes()).to_string());
                return Ok(());
            }
            let mut plugin = Box::from_raw(plugin_constructor_result.plugin);

            info!("plugin {} loaded, version: {}, author: {}, description: {} ", metadata.name, metadata.version, metadata.author, metadata.description);

            // after plugin load call on activate hook
            if let std::result::Result::Err(e) = plugin.on_activate(){
                warn!("plugin {} on_activate error: {}, skip this plugin", metadata.name, e);
            } else {
                self.plugin_table.insert(metadata.priority, Arc::new(PluginWrapper{
                    plugin_metadata: metadata,
                    plugin
                }));
            }
            //

            Ok(())
        }
    }

    pub fn new(plugin_dir: String, settings: Arc<crate::settings::Settings>) -> anyhow::Result<Self> {
        let path = PathBuf::from(plugin_dir.clone());

        if !path.exists() {
            return Err(anyhow!(PluginManagerError::PluginDirNotExisted));
        } else {
            let plugin_table = BTreeMap::new();
            let mut manager = PluginManager{
                plugin_dir,
                plugin_table,
                settings
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

    pub fn get_plugin_metadata_list_with_pagination(&self, offset: u64, limit: u64) -> (u64,Vec<&PluginMetadata>) {
        let plugin_iter = self.plugin_table.iter();
        let mut result = Vec::new();
        plugin_iter.skip(offset as usize).take(limit as usize).for_each(|(_, plugin)| {
            result.push(&plugin.plugin_metadata);
        });
        let total = self.plugin_table.len() as u64;
        (total, result)
    }

}
