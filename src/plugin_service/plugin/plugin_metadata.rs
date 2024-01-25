use std::{ fs::File, io::{self, Read}, path::PathBuf};
use anyhow::{anyhow, Error, Ok, Result};
use thiserror::Error;
use toml::Table;

// Represent plugin metadata
pub struct PluginMetadata {
    pub name: String,
    pub author: String,
    pub description: String,
    pub version: String,
    pub entry: String,
    pub priority: i64,
    pub plugin_absolute_path: PathBuf
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

// Check the plugin config is valid
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

// Read the plugin config file
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

impl PluginMetadata {

    // Create plugin metadata from the plugin directory
    pub fn new(plugin_path:PathBuf) -> Result<PluginMetadata> {
        let path = plugin_path.join("plugin.toml");
        if !path.exists() {
            return Err(anyhow!(PluginError::PluginConfigNotFound));
        }
        let table = parse_config(&path)?;
        check_plugin_config(&table, &plugin_path)?;

        let priority = table.get("plugin").unwrap().get("priority").unwrap().as_integer().unwrap();
        let name = table.get("plugin").unwrap().get("name").unwrap().as_str().unwrap();
        let version = table.get("plugin").unwrap().get("version").unwrap().as_str().unwrap();
        let author = table.get("plugin").unwrap().get("author").unwrap().as_str().unwrap();
        let description = table.get("plugin").unwrap().get("description").unwrap().as_str().unwrap();
        let entry = table.get("plugin").unwrap().get("entry").unwrap().as_str().unwrap();

        Ok(PluginMetadata {
            name: name.to_string(),
            author: author.to_string(),
            description: description.to_string(),
            version: version.to_string(),
            entry: entry.to_string(),
            priority,
            plugin_absolute_path: plugin_path
        })
    }

    pub fn get_entry_absolute_path(&self) -> PathBuf {
        let entry_path = self.plugin_absolute_path.join(self.entry.clone());
        entry_path
    }
}