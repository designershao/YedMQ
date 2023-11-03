use std::{fs::File, io::Read, path::{PathBuf, Path}, collections::HashMap, sync::Arc};

use anyhow::{anyhow, Error, Ok, Result};
use mlua::{Lua, Function, UserData};
use thiserror::Error;
use toml::{Table, Value};

use crate::protocol::v3::connect::ConnectPacket;

pub struct PluginContext {
    config: PluginConfig,
}

impl PluginContext {
    pub fn new(config: PluginConfig) -> Self {
        PluginContext { config }
    }
}

impl UserData for PluginContext {
    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
    }

    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("config", |lua, this| {
            let table:mlua::Table = convert_toml_table_to_mlua_table(&this.config.inner, lua).unwrap();
            core::result::Result::Ok(table)
        });
    }

}

#[derive(Error, Debug)]
pub enum PluginError {
    #[error("plugin config not found")]
    PluginConfigNotFound,

    #[error("read plugin config error")]
    ReadPluginConfigError(Error),

    #[error("invalid plugin config: {0}")]
    InvalidPluginConfig(String),

    #[error("invalid plugin: {0}")]
    RuntimeError(#[from] mlua::Error),
}

// Represent plugin
pub struct Plugin { }

pub enum PluginMessage {
    OnConnectAuth(ConnectPacket, tokio::sync::oneshot::Sender<bool>),
    Quit
}

impl Plugin {
    pub fn new(plugin_path: &PathBuf, local_set: tokio::task::LocalSet) -> Result<tokio::sync::mpsc::Sender<PluginMessage>> {

        let plugin_config = PluginConfig::new(plugin_path)?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        
        local_set.spawn_local(async move {

            let lua = Lua::new();

            super::module::init_runtime(&lua).unwrap();

            let plugin_entry_src = std::fs::read_to_string(plugin_config.get_entry_path()).unwrap();

            let module = lua.load(plugin_entry_src).eval::<mlua::Table>().unwrap();

            let on_activate = module.get::<&str, Function>("OnActivate").unwrap();

            let plugin_context = PluginContext{
                config: plugin_config
            };

            on_activate.call::<(PluginContext,),()>((plugin_context,)).unwrap();

            loop {
                let msg = rx.recv().await;

                if let Some(msg) = msg  {
                    match msg {
                        PluginMessage::OnConnectAuth(packet, tx) => {
                            let root_module = super::module::get_root_module(&lua).unwrap();
                            let r = root_module.hook.on_connect_auth_hook.handle(&lua, &packet).unwrap();
                            tx.send(r).unwrap();
                        }
                        PluginMessage::Quit => {
                            rx.close();
                        },
                    }
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
    plugin_path: PathBuf,
    pub inner: Table,
}

fn convert_toml_table_to_mlua_table<'a>(src: &toml::Table, lua: &'a Lua) -> Result<mlua::Table<'a>> {
    let table = lua.create_table()?;
    for (k, v) in src {
        match v {
            toml::Value::String(v) => table.set(lua.create_string(k.to_string())?, lua.create_string(v.to_string())?)?,
            toml::Value::Integer(v) => table.set(lua.create_string(k.to_string())?, v.clone())?,
            toml::Value::Float(v) => table.set(lua.create_string(k.to_string())?, v.clone())?,
            toml::Value::Boolean(v) => table.set(lua.create_string(k.to_string())?, v.clone())?,
            toml::Value::Datetime(v) => table.set(lua.create_string(k.to_string())?, 0)?,
            toml::Value::Array(v) => table.set(lua.create_string(k.to_string())?, do_vec_to_mlua_table(v, lua)?)?,
            toml::Value::Table(v) => table.set(lua.create_string(k.to_string())?, convert_toml_table_to_mlua_table(v,lua)?)?,
        }
    }
    Ok(table)
}

fn do_vec_to_mlua_table<'a>(src: &Vec<Value>, lua: &'a Lua) -> Result<mlua::Table<'a>> {
    let table = lua.create_table()?;
    for v in src {
        match v {
            toml::Value::String(v) => table.push(lua.create_string(v.to_string())?)?,
            toml::Value::Integer(v) => table.push(v.clone())?,
            toml::Value::Float(v) => table.push(v.clone())?,
            toml::Value::Boolean(v) => table.push(v.clone())?,
            toml::Value::Datetime(v) => table.push(0)?,
            toml::Value::Array(v) => table.push(do_vec_to_mlua_table(v, lua)?)?,
            toml::Value::Table(v) => table.push(convert_toml_table_to_mlua_table(v,lua)?)?,
        }
    }
    return Ok(table)
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
        Ok(PluginConfig {
            plugin_path: plugin_path.to_path_buf(),
            inner: table 
        })
    }

    // get toml config section
    pub fn get_section(&self, section: &str) -> Option<&toml::Value> {
        self.inner.get(section)
    }

    pub fn get_entry_path(&self) -> PathBuf {
        let entry = self.inner.get("plugin").unwrap().get("entry").unwrap().as_str().unwrap();
        let entry_path = self.plugin_path.join(entry);
        entry_path
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

    use super::{parse_config, check_plugin_config, PluginConfig, PluginContext};

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

    #[test]
    pub fn test_plugin_config_read_from_lua() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path)
            .join("tests")
            .join("demo_plugin");
        let plugin_config = PluginConfig::new(&plugin_path).unwrap();
        let plugin_context = PluginContext{
            config: plugin_config
        };

        let lua = mlua::Lua::new();
        lua.globals().set("plugin_context", plugin_context).unwrap();
        let r = lua.load(r#"
            return plugin_context.config["plugin"]["name"]
        "#).eval::<String>().unwrap();
        assert_eq!(r, "demo_plugin")
    }

}
