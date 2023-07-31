pub mod api;
pub mod hook_context;
pub mod hook;

use std::{fmt, cell::RefCell, sync::{Arc, Mutex}, collections::HashMap, fs, path::PathBuf};
use mlua::{Lua, Chunk, Table, Function};
use nom::Err;

#[derive(Debug, PartialEq)]
pub enum Error {
    LoadWasmModuleError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::LoadWasmModuleError(msg) => write!(f, "{}", msg),
        }
    }
}

// Represents broker plugin.
pub struct Plugin {
    pub name: String,
    pub description: String,
    pub author: String,
    pub runtime: Arc<Mutex<Lua>>,
    pub plugin_path: String
}

impl Plugin {

    pub fn new(plugin_path: &PathBuf) -> mlua::Result<Self> {
        let mut dest_plugin_path = plugin_path.clone();
        dest_plugin_path.push("lua");
        dest_plugin_path.push("init.lua");
        if let Ok(content) = fs::read(dest_plugin_path) {
            let lua = Lua::new();
            let out_table = lua.load(&String::from_utf8(content).unwrap()).eval::<Table>();
            if let Ok(out_table) = out_table {
                let author:String= out_table.get("author")?;
                let plugin_name:String = out_table.get("name")?;
                let plugin_description:String = out_table.get("description")?;
                let setup_func:Function = out_table.get("setup")?;
                setup_func.call(())?;
                Ok(
                Self {
                    name: plugin_name,
                    description: plugin_description,
                    author: author,
                    plugin_path: plugin_path.to_str().unwrap().to_string(),
                    runtime: todo!(),
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