use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use libloading::{Library, Symbol};
use plugin_metadata::PluginMetadata;
use samoye_plugin::plugin::Plugin;
use anyhow::{anyhow, Ok};
use thiserror::Error;
use log::warn;

mod plugin_metadata;

#[derive(Error, Debug)]
pub enum PluginManagerError {
    #[error("plugin dir not existed")]
    PluginDirNotExisted,

    #[error("load plugin error:`{0}` ")]
    PluginLoadError(String),
}

pub struct PluginManager {

    plugin_dir: String,

    plugin_table: BTreeMap<i64, Arc<Box<dyn Plugin>>>

}


impl PluginManager {

    pub fn load_plugin(&mut self, metadata: PluginMetadata) -> anyhow::Result<()> {
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

            self.plugin_table.insert(metadata.priority, Arc::new(plugin));

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
                            let load_plugin_result = manager.load_plugin(metadata).and_then(|plugin| {
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
