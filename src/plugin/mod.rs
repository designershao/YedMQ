pub mod api;

use std::{collections::HashMap, fs, path::PathBuf, rc::Rc, sync::Arc};
use mlua::{Lua, Table, Function, RegistryKey, UserData};
use crate::protocol::v3::publish::PublishPacket;
use self::api::byte_array::LuaByteArray;
use thiserror::Error;
use anyhow::{Context, Result};


#[derive(Debug, PartialEq, Error)]
pub enum Error {
    #[error("load plugin error")]
    LoadPluginError(String),

    #[error("plugin runtime error")]
    PluginRuntimeError(String),
}

// Represents session context
pub struct SessionContext {
    pub client_id: String,
    pub username: String,
}

impl UserData for SessionContext {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("clientId", |_, this| Ok(this.client_id.clone()));
        fields.add_field_method_get("username", |_,this| Ok(this.username.clone()));
    }
}


// Represents broker plugin.
pub struct Plugin {
    pub name: String,
    pub description: String,
    pub author: String,
    pub version: String,
    runtime: Rc<Lua>,
    hook_table_key: RegistryKey,
    on_activate_func_key: Option<RegistryKey>,
    on_deactivate_func_key: Option<RegistryKey>,
    register_hooks: Vec<String>
}

impl Plugin {
    pub fn on_publish(&self, session_ctx:SessionContext,publish_packet: &PublishPacket) -> Result<()> {
        let hook:Table = self.runtime.registry_value::<Table>(&self.hook_table_key).with_context(|| format!("Plugin {} : Failed to register hook table", self.name))?;
        let on_publish_func:Function = hook.get("onPublish").with_context(|| format!("Plugin {} : Failed to get hook onPublish", self.name))?;
        let byte_array = api::byte_array::LuaByteArray::new(publish_packet.payload.payload.clone());
        on_publish_func.call::<(SessionContext,String,i32, LuaByteArray), ()>((
            session_ctx, 
            publish_packet.variable_header.topic_name.clone(), 
            publish_packet.fix_header.qos.unwrap(),
            byte_array
        )).with_context(|| format!("Call hook onPublish failed"))?;
        Ok(())
    }


    // Called when client publish the publish packet
    pub fn on_publish_acl_check(&self, session_ctx:SessionContext,topic:&String, qos:i32) -> Result<bool> {
        let hook:Table = self.runtime.registry_value::<Table>(&self.hook_table_key)?;
        let on_publish_acl_check_func:Function = hook.get("onPublishAclCheck").with_context(|| format!("Plugin {} : Failed to get hook onPublishAclCheck", self.name))?;
        let result = on_publish_acl_check_func.call::<(SessionContext,String,i32), bool>((session_ctx, topic.to_string(), qos)).with_context(|| format!("Plugin {} : Call hook onPublishAclCheck failed", self.name))?;
        Ok(result)
    }

    // Called new connect packet is connected
    pub fn on_connect_auth(&self, client_id: &String, username:&String, password: &String, ip: &String) -> Result<bool> {
        let hook:Table = self.runtime.registry_value::<Table>(&self.hook_table_key)?;
        let on_connect_auth_func:Function = hook.get("onConnectAuth").with_context(|| format!("Plugin {} : Failed to get hook onConnectAuth", self.name))?;
        let result = on_connect_auth_func.call::<(String,String,String,String), bool>((client_id.to_string(), username.to_string(), password.to_string(), ip.to_string())).with_context(|| format!("Plugin {} : Call hook onConnectAuth failed", self.name))?;
        Ok(result)
    }

    // Called when plugin is activated
    pub fn on_activate(&self) -> Result<()> {
        if self.on_activate_func_key.is_some() {
            let on_activate_func:Function = self.runtime.registry_value(&self.on_activate_func_key.as_ref().unwrap()).unwrap();
            on_activate_func.call::<(),()>(()).with_context(|| format!("Plugin {} : Call hook onActivate failed", self.name))?;
        }
        Ok(())
    }

    /// Called when plugin is deactivated
    pub fn on_deactivate(&self) -> Result<()> {
        if self.on_deactivate_func_key.is_some() {
            let on_deactivate_func:Function = self.runtime.registry_value(&self.on_deactivate_func_key.as_ref().unwrap()).unwrap();
            on_deactivate_func.call::<(),()>(()).with_context(|| format!("Plugin {} : Call hook onDeactivate failed", self.name))?;
        }
        Ok(())
    }

    // Check the hooks table and get the register hooks
    fn get_hook_func_names_from_table(table: &Table) -> Vec<String> {
        let mut result = Vec::new();
        let hooks = vec![String::from("onConnectAuth"), String::from("onPublishAclCheck"), String::from("onPublish")];

        for hook in hooks {
            let hook_name = hook.clone();
            if table.contains_key::<String>(hook.into()).unwrap() {
                result.push(hook_name);
            }
        }
        result
    }

    pub fn new(plugin_path: &PathBuf) -> Result<Plugin> {

        let lua = Rc::new(Lua::new());

        let mut dest_plugin_path = plugin_path.clone();
        dest_plugin_path.push("src");
        dest_plugin_path.push("plugin.lua");

        let content = fs::read(dest_plugin_path.clone()).with_context(|| format!("Can`t read plugin file {}", dest_plugin_path.to_str().unwrap()))?;
        let lua = lua.clone();
        let plugin_module = lua.load(&String::from_utf8(content).unwrap()).eval::<Table>()?;
        let author:String= plugin_module.get("author")?;
        let plugin_name:String = plugin_module.get("name")?;
        let plugin_description:String = plugin_module.get("description")?;
        let plugin_version = plugin_module.get("version")?;
        let hook_table:Table = plugin_module.get("hook")?;
        let register_hooks = Self::get_hook_func_names_from_table(&hook_table);
        let registry_key = lua.create_registry_value(hook_table).with_context(|| format!("Plugin {} : Failed to register hook table", plugin_name))?;

        let mut on_activate_func_key = None;
        let mut on_deactivate_func_key = None;

        if plugin_module.contains_key("onActivate").unwrap() {
            let on_activate_func:Function = plugin_module.get("onActivate").with_context(|| format!("Plugin {} : Failed to get hook onActivate", plugin_name))?;
            on_activate_func_key = Some(lua.create_registry_value(on_activate_func)?)
        }

        if plugin_module.contains_key("onDeactivate").unwrap() {
            let on_deactivate_func:Function = plugin_module.get("onDeactivate").with_context(|| format!("Plugin {} : Failed to get hook onDeactivate", plugin_name))?;
            on_deactivate_func_key = Some(lua.create_registry_value(on_deactivate_func)?)
        }

        Ok(Plugin {
            name: plugin_name,
            description: plugin_description,
            author: author,
            runtime: lua.clone(),
            hook_table_key: registry_key,
            on_activate_func_key,
            on_deactivate_func_key,
            version: plugin_version,
            register_hooks
        })
    }
}

pub struct PluginManager {
    plugin_dict: PathBuf,
    inner: HashMap<String, Arc<Plugin>>
}

impl PluginManager {

    pub fn new(path: &String) -> Self {
        PluginManager { 
            plugin_dict: PathBuf::from(path), 
            inner: HashMap::new()
        }
    }

    pub fn load_all_plugins(&mut self) -> core::result::Result<(), Error> {
        // check plugin_dict existed
        if self.plugin_dict.is_dir() {
            self.loop_directory();
            Ok(())
        } else {
            Err(Error::LoadPluginError(format!("{} is not a directory", self.plugin_dict.to_str().unwrap())))
        }
    }

    // get plugin with plugin name
    pub fn get_plugin(&self, name: &String) -> Option<Arc<Plugin>> {
        self.inner.get(name).cloned() 
    }

    // delete plugin with plugin name
    pub fn delete_plugin(&mut self, name: &String) {
        self.inner.remove(name);
    }

    // according the hookname get plugin list
    pub fn get_plugins_with_hook(&self, name: &String) -> Vec<Arc<Plugin>> {
        let  mut result = Vec::new();
        for plugin in self.inner.values() {
            if plugin.register_hooks.contains(name) {
                result.push(plugin.clone());
            } 
        }
        result
    }

    fn loop_directory(&mut self) {
        // loop directory to find plugin
        if let Ok(entries) = fs::read_dir(self.plugin_dict.clone()) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Ok(entry_type) = entry.file_type() {
                        if entry_type.is_dir() {
                            let dest_plugin_path = entry.path();
                            let plugin = Plugin::new(&dest_plugin_path);
                            if let Ok(plugin) = plugin {
                                plugin.on_activate(); // call on_activate hook when load plugin succeed
                                self.inner.insert(plugin.name.clone(), Arc::new(plugin));
                            }
                        } 
                    }
                }
            } 
        }
    }

}

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf};

    use mlua::{Value, ToLua};

    use crate::protocol::{v3::{publish::{PublishPacket, VariableHeader, Payload}, fixed_header::FixHeader}, PacketType};

    use super::{Plugin, SessionContext};


    #[test]
    fn plugin_load_test() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests").join("demo_plugin");
        let plugin = Plugin::new(&plugin_path).unwrap();
        assert_eq!(plugin.name, "demo_plugin");
        assert_eq!(plugin.description, "demo plugin");
        assert_eq!(plugin.author, "test");
        assert_eq!(plugin.version, "1.0.0");
    }

    #[test]
    fn plugin_on_connect_auth_test() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests").join("demo_plugin");
        let plugin = Plugin::new(&plugin_path).unwrap();
        let r = plugin.on_connect_auth(&String::from("client_id"), &String::from("test"), &String::from("test"), &String::from("127.0.0.1")).unwrap();
        assert_eq!(r, true);
    }

    #[test]
    fn plugin_on_publish_test() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests").join("demo_plugin");
        let plugin = Plugin::new(&plugin_path).unwrap();
        
        let fix_header = FixHeader {
            packet_type: PacketType::PUBLISH,
            qos: Some(1),
            retain: Some(true),
            dup: Some(1),
            remaining_length: 8,
        };
        let variable_header = VariableHeader {
            topic_name: "a/b".to_string(),
            packet_identifier: Some(0x10),
        };

        let payload = Payload{
            payload: vec!(0x68,0x65,0x6c,0x6c,0x6f)
        };

        let publish_packet = PublishPacket {
            fix_header,
            variable_header,
            payload
        };

        let session_ctx = SessionContext {
            client_id: String::from("client_id"),
            username: String::from("test")
        };

        let _ = plugin.on_publish(session_ctx, &publish_packet).unwrap();
        let globals = plugin.runtime.globals();
        let playload_str:String = globals.get::<String,String>("onPublishPacketConentStr".into()).unwrap();
        let publish_qos:i32 = globals.get::<String,i32>("onPublishPacketQos".into()).unwrap();
        let publish_topic:String = globals.get::<String,String>("onPublishPacketTopic".into()).unwrap();
        assert_eq!(playload_str, "hello".to_string());
        assert_eq!(publish_qos, 1);
        assert_eq!(publish_topic, "a/b".to_string());

    }
}