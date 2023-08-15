pub mod api;

use std::{fmt,  collections::HashMap, fs, path::PathBuf, rc::Rc, sync::Arc};
use mlua::{Lua, Table, Function, Result, RegistryKey};

#[derive(Debug, PartialEq)]
pub enum Error {
    LoadPluginError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::LoadPluginError(msg) => write!(f, "{}", msg),
        }
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
    on_deactivate_func_key: Option<RegistryKey>
}

impl Plugin {

    /// Called new connect packet is connected
    pub fn on_connect_auth(&self, client_id: &String, username:&String, password: &String, ip: &String) -> Result<bool> {
        let hook:Table = self.runtime.registry_value::<Table>(&self.hook_table_key)?;
        let on_connect_auth_func:Function = hook.get("onConnectAuth").unwrap();
        on_connect_auth_func.call::<(String,String,String,String), bool>((client_id.to_string(), username.to_string(), password.to_string(), ip.to_string()))
    }

    /// Called when plugin is activated
    pub fn on_activate(&self) {
        if self.on_activate_func_key.is_some() {
            let on_activate_func:Function = self.runtime.registry_value(&self.on_activate_func_key.as_ref().unwrap()).unwrap();
            on_activate_func.call::<(),()>(());
        }
    }

    /// Called when plugin is deactivated
    pub fn on_deactivate(&self) {
        if self.on_deactivate_func_key.is_some() {
            let on_deactivate_func:Function = self.runtime.registry_value(&self.on_deactivate_func_key.as_ref().unwrap()).unwrap();
            on_deactivate_func.call::<(),()>(());
        }
    }


    pub fn new(plugin_path: &PathBuf) -> Result<Plugin> {

        let lua = Rc::new(Lua::new());

        let mut dest_plugin_path = plugin_path.clone();
        dest_plugin_path.push("src");
        dest_plugin_path.push("plugin.lua");

        if let Ok(content) = fs::read(dest_plugin_path.clone()) {
            let lua = lua.clone();
            let plugin_module = lua.load(&String::from_utf8(content).unwrap()).eval::<Table>();
            if let Ok(plugin_module) = plugin_module {
                let author:String= plugin_module.get("author")?;
                let plugin_name:String = plugin_module.get("name")?;
                let plugin_description:String = plugin_module.get("description")?;
                let plugin_version = plugin_module.get("version")?;
                let hook_table:Table = plugin_module.get("hook")?;
                let registry_key = lua.create_registry_value(hook_table)?;

                let on_activate:Result<Function> = plugin_module.get("onActivate");

                let on_activate_func_key = match on_activate {
                    Ok(on_activate_func) => {
                        Some(lua.create_registry_value(on_activate_func)?)
                    },
                    Err(_) => None
                };

                let on_deactivate:Result<Function> = plugin_module.get("onDeactivate");
                let on_deactivate_func_key = match on_deactivate {
                    Ok(on_deactivate_func) => {
                        Some(lua.create_registry_value(on_deactivate_func)?)
                    },
                    Err(_) => None
                };

                Ok(Plugin {
                    name: plugin_name,
                    description: plugin_description,
                    author: author,
                    runtime: lua.clone(),
                    hook_table_key: registry_key,
                    on_activate_func_key,
                    on_deactivate_func_key,
                    version: plugin_version
                })
            } else {
                Err(mlua::Error::BindError)
            }
        } else {
            Err(mlua::Error::BindError)
        }
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

    fn loop_directory(&mut self, plugin_dic_path: &str) {
        // loop directory to find plugin
        if let Ok(entries) = fs::read_dir(plugin_dic_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Ok(entry_type) = entry.file_type() {
                        if entry_type.is_dir() {
                            let dest_plugin_path = entry.path();
                            let plugin = Plugin::new(&dest_plugin_path);
                            if let Ok(plugin) = plugin {
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

    use super::Plugin;


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
}