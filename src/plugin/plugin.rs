use std::{fs::File, io::{Read, self}, path::{PathBuf, Path}, collections::HashMap, sync::Arc, time::Duration};

use anyhow::{anyhow, Error, Ok, Result};
use log::{warn, info};
use mlua::{Lua, Function, UserData};
use thiserror::Error;
use toml::{Table, Value};

use crate::protocol::v3::{connect::ConnectPacket, publish::PublishPacket};

use super::{session_context::SessionContext, module::hook::ConnectAuthResponse};

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

    #[error("load plugin error: {0}")]
    LoadPluginError(#[from] io::Error),
}

pub struct ConnectInfo {
    pub remote_addr: String,
    pub connect_packet: ConnectPacket,
}

#[derive(Debug)]
pub struct RegisterHookResponse {
    pub hook_names: Vec<String>, // hook name (OnConnectAuth or OnSubscribeACLCheck etc.)
    pub priority: i64, // plugin priority
}

// Represent plugin
pub struct Plugin { }

pub enum PluginMessage {
    OnConnectAuth(ConnectInfo, tokio::sync::oneshot::Sender<PluginResponse>),
    OnPublish(SessionContext, PublishPacket),
    OnSubscribeACLCheck(SessionContext, String, i32, tokio::sync::oneshot::Sender<PluginResponse>),
    GetPluginName(tokio::sync::oneshot::Sender<String>),
    GetRegisterHooks(tokio::sync::oneshot::Sender<RegisterHookResponse>),
    Quit
}

pub enum PluginResponse {
    AuthResult(PluginAuthResult), // Response if the hook is OnConnectAuth or OnSubscribeACLCheck
    ACLCheckResult(PluginACLCheckResult),
}

// Reject the auth details
pub enum RejectResult {
    Forbidden, // Auth failed
    Error(anyhow::Error), // Auth Error
}

pub enum PluginACLCheckResult {
    Pass,
    Reject(RejectResult),
}

// Plugin auth result (OnConnectAuth or OnSubscribeACLCheck)
pub enum PluginAuthResult {
    Pass(String, String), // (TenantId, UserId)
    Reject(RejectResult),
}

impl Plugin {
    pub fn new(plugin_path: &PathBuf, local_set: &tokio::task::LocalSet) -> Result<tokio::sync::mpsc::Sender<PluginMessage>> {

        let plugin_config = PluginConfig::new(plugin_path)?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        
        tokio::task::spawn_local(async move {

            let lua = Lua::new();

            super::module::init_runtime(&lua).unwrap();

            let plugin_entry_src = std::fs::read_to_string(plugin_config.get_entry_path());
            if plugin_entry_src.is_err() {
                let err = plugin_entry_src.unwrap_err();
                warn!("read plugin entry source error: {}", err);
                return Err(PluginError::LoadPluginError(err));
            }

            let module = lua.load(plugin_entry_src.unwrap()).eval::<mlua::Table>();

            if module.is_err() {
                let err = module.unwrap_err();
                warn!("load plugin entry error: {}", err);
                return Err(PluginError::RuntimeError(err));
            }

            let on_activate = module.as_ref().unwrap().get::<&str, Function>("OnActivate").unwrap();


            let plugin_context = PluginContext{
                config: plugin_config.clone()
            };

            on_activate.call::<(PluginContext,),()>((plugin_context,)).unwrap();

            tokio::time::sleep(Duration::from_millis(1)).await;

            loop {
                let msg = rx.recv().await;

                if let Some(msg) = msg  {
                    match msg {
                        PluginMessage::OnConnectAuth(connect_info, tx) => {
                            let root_module = super::module::get_root_module(&lua).unwrap();
                            let r = root_module.hook.on_connect_auth_hook.handle(&lua, &connect_info);
                            match r {
                                std::result::Result::Ok(r) => {
                                    if r.pass {
                                        let response = PluginResponse::AuthResult(PluginAuthResult::Pass(r.tenant_id, r.user_id));
                                        if let Err(_) = tx.send(response) {
                                            warn!("the receiver dropped");
                                        }
                                    } else {
                                        let response = PluginResponse::AuthResult(PluginAuthResult::Reject(RejectResult::Forbidden));
                                        if let Err(_) = tx.send(response) {
                                            warn!("the receiver dropped");
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("plugin {} on_connect_auth_hook error: {}", plugin_config.inner.get("plugin").unwrap().get("name").unwrap().as_str().unwrap(), e);
                                    let response = PluginResponse::AuthResult(PluginAuthResult::Reject(RejectResult::Error(e)));
                                    if let Err(_) = tx.send(response) {
                                        warn!("the receiver dropped");
                                    }
                                }
                            }
                        }
                        PluginMessage::OnSubscribeACLCheck(session_ctx, topic, qos, tx) => {
                            let root_module = super::module::get_root_module(&lua).unwrap();
                            let r = root_module.hook.on_subscribe_acl_check_hook.handle(&lua, &session_ctx, topic, qos);
                            match r {
                                std::result::Result::Ok(r) => {
                                    if r.pass {
                                        let response = PluginResponse::ACLCheckResult(PluginACLCheckResult::Pass);
                                        if let Err(_) = tx.send(response) {
                                            warn!("the receiver dropped");
                                        }
                                    } else {
                                        let response = PluginResponse::ACLCheckResult(PluginACLCheckResult::Reject(RejectResult::Forbidden));
                                        if let Err(_) = tx.send(response) {
                                            warn!("the receiver dropped");
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("plugin {} on_subscribe_acl_check_hook error", plugin_config.inner.get("plugin").unwrap().get("name").unwrap().as_str().unwrap());
                                    let response = PluginResponse::ACLCheckResult(PluginACLCheckResult::Reject(RejectResult::Error(e)));
                                    if let Err(_) = tx.send(response) {
                                        warn!("the receiver dropped");
                                    }
                                },
                            }
                        }
                        PluginMessage::OnPublish(session_ctx, packet) => {
                            let root_module = super::module::get_root_module(&lua).unwrap();
                            match root_module.hook.on_publish_hook.handle(&lua, &packet) {
                                Err(e) => warn!("plugin {} on_publish_hook error: {}", plugin_config.inner.get("plugin").unwrap().get("name").unwrap().as_str().unwrap(), e),
                                _ => ()
                            }
                        }
                        PluginMessage::GetPluginName(tx) => {
                            let plugin_name = plugin_config.inner.get("plugin").unwrap().get("name").unwrap().as_str().unwrap();
                            tx.send(plugin_name.to_string()).unwrap();
                        }
                        PluginMessage::GetRegisterHooks(tx) => {
                            let root_module = super::module::get_root_module(&lua).unwrap();
                            let register_hooks:Vec<String> = root_module.hook.get_register_hooks(&lua).iter().map(|i| i.to_string()).collect();
                            tx.send(RegisterHookResponse { hook_names: register_hooks, priority: plugin_config.priority }).unwrap();
                        }
                        PluginMessage::Quit => {
                            info!("plugin {} receive quit signal", plugin_config.inner.get("plugin").unwrap().get("name").unwrap().as_str().unwrap());
                            let on_deactive = module.as_ref().unwrap().get::<&str, Function>("OnDeactivate").unwrap();
                            on_deactive.call::<(),()>(()).unwrap();
                            rx.close();
                        },
                    }
                } else {
                    info!("plugin {} quit", plugin_config.inner.get("plugin").unwrap().get("name").unwrap().as_str().unwrap());
                    break;
                }
            }
            core::result::Result::Ok(())
        });
        Ok(
            tx
        )
    }
}


#[derive(Clone)]
pub struct PluginConfig {
    plugin_path: PathBuf,
    pub inner: Table,
    pub priority: i64
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
        let priority = table.get("plugin").unwrap().get("priority").unwrap().as_integer().unwrap();
        Ok(PluginConfig {
            plugin_path: plugin_path.to_path_buf(),
            inner: table,
            priority
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
        if plugin_section.get("priority").is_none() {
            return Err(anyhow!(PluginError::InvalidPluginConfig("missing plugin priority".to_string())));
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
    use std::{path::PathBuf, time::Duration};

    use tokio::runtime::Builder;

    use crate::{protocol::{v3::{connect::{VariableHeader, Payload, ConnectPacket}, fixed_header::FixHeader}, PacketType}, plugin::plugin::{PluginMessage, ConnectInfo, PluginResponse, RegisterHookResponse}};

    use super::{parse_config, check_plugin_config, PluginConfig, PluginContext, PluginAuthResult};

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_get_register_hooks() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path)
            .join("tests")
            .join("demo_plugin");

        let (tx, mut rx) = tokio::sync::oneshot::channel();

        let (init_tx, mut init_rx) = tokio::sync::oneshot::channel();

        let join = tokio::spawn(async move {
            let plugin_tx: tokio::sync::mpsc::Sender<PluginMessage> = init_rx.await.unwrap();
            plugin_tx.send(super::PluginMessage::GetRegisterHooks(tx)).await.unwrap();
            let register_hooks = rx.await.unwrap();
            return register_hooks
        });

        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        std::thread::spawn(move || {
            let local_set = tokio::task::LocalSet::new();
            local_set.spawn_local(async move {
                let local_set = tokio::task::LocalSet::new();
                let plugin_tx = super::Plugin::new(&plugin_path, &local_set).unwrap();
                init_tx.send(plugin_tx.clone()).unwrap();
            });

            rt.block_on(local_set);
        });


        let hooks:RegisterHookResponse = join.await.unwrap();
        assert_eq!(2, hooks.hook_names.len());
        assert_eq!("OnConnectAuth", hooks.hook_names[0]);
        assert_eq!("OnSubscribeACLCheck", hooks.hook_names[1]);
        assert_eq!(1000, hooks.priority);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_run_succeed() {
        let variable_header = VariableHeader {
                protocol_name: "MQTT".to_string(),
                protocol_level: 0x04,
                username_flag: true,
                password_flag: true,
                will_retain: true,
                will_qos: 1,
                will_flag: true,
                clean_session: true,
                keep_alive: 0,
            };
        let payload = Payload {
            client_identifier: "MQTT".to_string(),
            will_topic: Some("MQTT".to_string()),
            will_message: Some("MQTT".to_string()),
            username: Some("admin".to_string()),
            password: Some("MQTT".to_string()),
        };

        let fix_header = FixHeader{
                packet_type: PacketType::CONNECT,
                qos: None,
                retain: None,
                dup: None,
                remaining_length: variable_header.get_length() + payload.get_length(),
            };

        let connect_packet = ConnectPacket {
            fix_header,
            variable_header,
            payload
        };


        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path)
            .join("tests")
            .join("demo_plugin");

        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let (init_tx, mut init_rx) = tokio::sync::oneshot::channel();

        let join = tokio::spawn(async move {
            let connect_info = ConnectInfo { remote_addr: "127.0.0.1".to_string(), connect_packet };
            let plugin_tx: tokio::sync::mpsc::Sender<PluginMessage> = init_rx.await.unwrap();
            plugin_tx.send(super::PluginMessage::OnConnectAuth(connect_info, tx)).await.unwrap();
            let r = rx.await.unwrap();
            plugin_tx.send(super::PluginMessage::Quit).await.unwrap();
            r
        });

        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        std::thread::spawn(move || {
            let local_set = tokio::task::LocalSet::new();
            local_set.spawn_local(async move {
                let local_set = tokio::task::LocalSet::new();
                let plugin_tx = super::Plugin::new(&plugin_path, &local_set).unwrap();
                init_tx.send(plugin_tx.clone()).unwrap();
            });

            rt.block_on(local_set);
        });


        let r:PluginResponse = join.await.unwrap();
        if let PluginResponse::AuthResult(PluginAuthResult::Pass(teant_id, user_id)) = r {
            assert_eq!(teant_id, "t-123");
            assert_eq!(user_id, "123");
        } else {
            assert!(false)
        }

    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_on_deactivate_func() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path)
            .join("tests")
            .join("demo_plugin");

        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let r = std::thread::spawn(move || {
            let local_set = tokio::task::LocalSet::new();
            local_set.spawn_local(async move {
                let local_set = tokio::task::LocalSet::new();
                let plugin_tx = super::Plugin::new(&plugin_path, &local_set).unwrap();
                plugin_tx.send(super::PluginMessage::Quit).await.unwrap();
            });
            rt.block_on(local_set);
        });
        r.join().unwrap();
    }

}
