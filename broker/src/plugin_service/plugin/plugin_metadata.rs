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
