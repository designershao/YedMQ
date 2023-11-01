use std::{path::PathBuf, fs::File, io::Read};

use toml::Table;
use thiserror::Error;
use anyhow::{Result, Error, anyhow};

#[derive(Error, Debug)]
pub enum PluginError {
    #[error("plugin config not found")]
    PluginConfigNotFound,

    #[error("read plugin config error")]
    ReadPluginConfigError(Error),

    #[error("invalid plugin config: {0}")]
    InvalidPluginConfig(String)
}

// Represent plugin
pub struct Plugin {
}

fn parse_config(path: &String) -> Result<Table> {
    let mut path = PathBuf::from(path);
    if path.exists() {
        let mut file = File::open(path).unwrap();
        let mut contents = String::new();
        if let Err(e) = file.read_to_string(&mut contents) {
            Err(anyhow!(PluginError::ReadPluginConfigError(e.into())))
        } else {
            return toml::from_str(&contents).unwrap();
        }
    } else {
        Err(anyhow!(PluginError::PluginConfigNotFound))
    }
} 

impl Plugin {
    pub fn new(path: &PathBuf) -> Plugin {
    }
}