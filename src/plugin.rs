use std::{fmt, cell::RefCell, sync::{Arc, Mutex}, collections::HashMap};

use wasmtime::{Module, Engine, Linker, TypedFunc, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder};

use crate::hook::Hook;

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
    pub entry: String,
    pub author: String,
    webassembly_module: Module
}

impl Plugin {

    pub fn new(name: String, description: String, entry: String, author: String, wasm_module_path:String, engine: &Engine) -> Result<Self, Error> {

        let load_module_result = Module::from_file(engine, &wasm_module_path);

        if load_module_result.is_err() {
            Err(Error::LoadWasmModuleError("load wasm module error".to_string()))
        } else {
            Ok(Self {
                name,
                description,
                entry,
                author,
                webassembly_module: load_module_result.unwrap()
            })
        }

    }

}

pub struct PluginManager {
    runtime_engine: Engine,
    linker: Linker<WasiCtx>,
    plugin_table: Mutex<HashMap<Hook, Arc<RefCell<Plugin>>>>,
    plugin_directory_path: String
    wasi: WasiCtx,
}

impl PluginManager {

    pub fn new(path: &String) -> Self {
        let engine = Engine::default();

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::add_to_linker(&mut linker, |cx| cx)?;
        
        let wasi = WasiCtxBuilder::new()
            .inherit_stdio()
            .inherit_args().unwrap()
            .build();


        Self {
            runtime_engine: engine,
            linker,
            plugin_table: Mutex::new(HashMap::new()),
            plugin_directory_path: path.to_string(),
            wasi
        }
    }

    pub fn register_plugin(&mut self,plugin:Plugin, hook:Hook) -> Result<(), Error> {
        let mut plugin_table = self.plugin_table.lock().unwrap();
        plugin_table.insert(hook, Arc::new(RefCell::new(plugin)));
        Ok(())
    }

    pub fn unregister_plugin(&mut self, hook:Hook) -> Result<(), Error> {
        let mut plugin_table = self.plugin_table.lock().unwrap();
        plugin_table.remove(&hook);
        Ok(())
    }

}