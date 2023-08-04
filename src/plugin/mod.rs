pub mod api;
pub mod hook_context;
pub mod hook;

use std::{fmt, cell::RefCell, sync::{Arc, Mutex, RwLock}, collections::HashMap, fs, path::PathBuf};
use mlua::{Lua, Chunk, Table, Function, Result};
use nom::Err;

use self::{hook_context::HookContext, hook::{Hook, on_connect_auth_hook::OnConnectAuthHookFuncWrapper}, api::hook::HookApi};

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
    pub runtime: Lua,
    init_lua_path: PathBuf,
}

impl Plugin {

    pub fn init(&'static mut self, hook_ctx: Arc<RwLock<HookContext<'static>>>) -> Result<()> {
        if let Ok(content) = fs::read(self.init_lua_path.clone()) {
            let lua_path = self.init_lua_path.to_str().unwrap().to_string();
            let hook_register_func = self.runtime.create_function(move |lua:&Lua, (hook_name, func_name):(String, String)| {
                let hook_ctx_cloned = hook_ctx.clone();
                let mut hook_api = HookApi::new(hook_ctx_cloned);
                hook_api.register(lua, &hook_name, &func_name, &lua_path)
            })?;

            let hook_func_table = self.runtime.create_table()?;
            hook_func_table.set("register", hook_register_func)?;

            let ns_table = self.runtime.create_table()?;
            ns_table.set("hook", hook_func_table)?;

            self.runtime.globals().set("samoye", ns_table)?;

            let out_table = self.runtime.load(&String::from_utf8(content).unwrap()).eval::<Table>();
            if let Ok(out_table) = out_table {
                let author:String= out_table.get("author")?;
                let plugin_name:String = out_table.get("name")?;
                let plugin_description:String = out_table.get("description")?;
                let setup_func:Function = out_table.get("setup")?;
                setup_func.call(())?;
                self.author = author;
                self.name = plugin_name;
                self.description = plugin_description;
                Ok(())
            } else {
                Err(mlua::Error::BindError)
            }
        } else {
            Err(mlua::Error::BindError)
        }
    }


    pub fn new(plugin_path: &PathBuf) -> Plugin {
        let mut dest_plugin_path = plugin_path.clone();
        dest_plugin_path.push("lua");
        dest_plugin_path.push("init.lua");
        Plugin {
            name: String::from("test"),
            description: String::from("test"),
            author: String::from("test"),
            init_lua_path: dest_plugin_path,
            runtime: Lua::new(),
        }
    }
}

pub struct PluginManager {
}

impl PluginManager {

    pub fn new(path: &String) -> Self {
        Self {  }
    }

    fn loop_directory(&self, plugin_dic_path: &str) {
        // loop directory to find plugin
        if let Ok(entries) = fs::read_dir(plugin_dic_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Ok(entry_type) = entry.file_type() {
                        if entry_type.is_dir() {
                            let mut dest_plugin_path = entry.path();
                        } 
                    }
                }
            } 
        }
    }

}