use std::{fs::File, io::Read, path::{PathBuf, Path}, collections::HashMap, sync::Arc};

use anyhow::{anyhow, Error, Ok, Result};
use mlua::{Lua, chunk, Function};
use thiserror::Error;
use tokio::sync::Mutex;
use toml::{Table, Value};

use crate::protocol::v3::connect::ConnectPacket;

use super::hook::Hook;

#[derive(Error, Debug)]
pub enum PluginError {
    #[error("plugin config not found")]
    PluginConfigNotFound,

    #[error("read plugin config error")]
    ReadPluginConfigError(Error),

    #[error("invalid plugin config: {0}")]
    InvalidPluginConfig(String),
}

// Represent plugin
pub struct Plugin { }

pub enum PluginMessage {
    OnConnectAuth(ConnectPacket, tokio::sync::oneshot::Sender<bool>),
    Quit
}

fn init_lua_runtime(lua: &Lua) -> Result<()> {

    let hook_table = lua.create_table().unwrap();

    lua.globals().set("__HOOK_TABLE__", hook_table).unwrap();

    let root_module = lua.create_table().unwrap();
    let hook_module = lua.create_table().unwrap();

    let on_connect_auth = lua.create_table().unwrap();
    on_connect_auth.set("Register", lua.create_function(|lua, hook_func:Function| {
        let table:mlua::Table = lua.globals().get("__HOOK_TABLE__")?;
        table.set("OnConnectAuth", hook_func).unwrap();
        std::result::Result::Ok(())
    }).unwrap()).unwrap();

    let on_subscribe_acl_check = lua.create_table().unwrap();
    on_subscribe_acl_check.set("Register", lua.create_function(|lua, hook_func:Function| {
        let table:mlua::Table = lua.globals().get("__HOOK_TABLE__")?;
        table.set("OnSubscribeACLCheck", hook_func).unwrap();
        std::result::Result::Ok(())
    }).unwrap()).unwrap();

    let on_publish_acl_check = lua.create_table().unwrap();
    on_publish_acl_check.set("Register", lua.create_function(|lua, hook_func:Function| {
        let table:mlua::Table = lua.globals().get("__HOOK_TABLE__")?;
        table.set("OnPublishACLCheck", hook_func).unwrap();
        std::result::Result::Ok(())
    }).unwrap()).unwrap();

    hook_module.set("OnConnectAuth", on_connect_auth).unwrap();
    hook_module.set("OnSubscribeACLCheck", on_subscribe_acl_check).unwrap();
    hook_module.set("OnPublishACLCheck", on_publish_acl_check).unwrap();

    root_module.set("Hook", hook_module).unwrap();

    lua.globals().set("Samoye", root_module).unwrap();

    Ok(())
}

impl Plugin {
    pub fn new(plugin_path: &PathBuf, local_set: tokio::task::LocalSet) -> Result<tokio::sync::mpsc::Sender<PluginMessage>> {
        let plugin_config = PluginConfig::new(plugin_path)?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        local_set.spawn_local(async move {

            let lua = Lua::new();

            init_lua_runtime(&lua).unwrap();

            loop {
                let msg = rx.recv().await;

                if let Some(msg) = msg  {

                } else {
                    break;
                }
            }
        });
        Ok(
            tx
        )
    }
}

pub struct PluginConfig {
    inner: Table,
}

impl PluginConfig {

    // create plugin config from the plugin path
    pub fn new(plugin_path: &PathBuf) -> Result<PluginConfig> {
        let path = plugin_path.join("plugin.toml");
        if !path.exists() {
            return Err(anyhow!(PluginError::PluginConfigNotFound));
        }
        let table = parse_config(&path)?;
        check_plugin_config(&table, plugin_path)?;
        Ok(PluginConfig { inner: table })
    }

    // get toml config section
    pub fn get_section(&self, section: &str) -> Option<&Value> {
        self.inner.get(section)
    }

}

fn check_plugin_config(table:&Table, plugin_path: &PathBuf) -> Result<()> {
    if let Some(plugin_section) = table.get("plugin") {
        if plugin_section.get("name").is_none() {
            return Err(anyhow!(PluginError::InvalidPluginConfig("missing plugin name".to_string())));
        }
        if plugin_section.get("entry").is_none() {
            return Err(anyhow!(PluginError::InvalidPluginConfig("missing plugin entry".to_string())));
        } else {
            let entry = plugin_section.get("entry").unwrap().as_str().unwrap();
            let entry_path = plugin_path.join(entry);
            if !entry_path.exists() {
                return Err(anyhow!(PluginError::InvalidPluginConfig(format!("plugin entry {} not existed", entry_path.to_str().unwrap()))));
            }
        }
        if plugin_section.get("version").is_none() {
            return Err(anyhow!(PluginError::InvalidPluginConfig("missing plugin version".to_string())));
        }
    } else {
        return Err(anyhow!(PluginError::InvalidPluginConfig("missing plugin section".to_string())));
    }

    Ok(())
}

fn parse_config(path: &PathBuf) -> Result<Table> {
    if path.exists() {
        let mut file = File::open(path).unwrap();
        let mut contents = String::new();
        if let Err(e) = file.read_to_string(&mut contents) {
            Err(anyhow!(PluginError::ReadPluginConfigError(e.into())))
        } else {
            let r: Table = toml::from_str(&contents[..]).unwrap();
            Ok(r)
        }
    } else {
        Err(anyhow!(PluginError::PluginConfigNotFound))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{parse_config, check_plugin_config};

    #[test]
    pub fn test_parse_config() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path)
            .join("tests")
            .join("demo_plugin")
            .join("plugin.toml");
        if let Ok(_) = parse_config(&PathBuf::from(plugin_path.to_str().unwrap().to_string())) {
            assert!(true);
        } else {
            assert!(false)
        }
    }

    #[test]
    pub fn test_check_plugin_config() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path)
            .join("tests")
            .join("demo_plugin");
        let plugin_config_path = plugin_path.join("plugin.toml");
        if let Ok(t) = parse_config(&PathBuf::from(plugin_config_path.to_str().unwrap().to_string())) {
            if let Ok(()) = check_plugin_config(&t, &plugin_path) {
                assert!(true)
            } else {
                assert!(false)
            }
        } else {
            assert!(false)
        }
        
    }
}
