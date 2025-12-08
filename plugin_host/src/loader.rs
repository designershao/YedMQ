use std::{collections::HashMap, fs, path::{Path, PathBuf}};
use tokio::process::Command;

use serde::{Serialize, Deserialize};
use anyhow::{Context, Result, anyhow};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    pub plugin: PluginInfo,
    pub runtime: RuntimeConfig
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
    pub license: Option<String>,
    pub homepage: Option<String>,
    pub repository: Option<String>,    
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    #[serde(rename = "type")]
    pub runtime_type: RuntimeType,
    pub executable: Option<String>,
    pub args: Option<Vec<String>>,
    pub env: Option<std::collections::HashMap<String, String>>,
    pub working_dir: Option<String>,
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeType {
    Process,
}

pub struct PluginLoader {
    plugins_dir: PathBuf,
    loaded_plugins: HashMap<String, PluginManifest>,
}

impl PluginLoader {
    pub fn new<P:AsRef<Path>>(plugins_dir: P) -> Self {
        Self {
            plugins_dir: plugins_dir.as_ref().to_path_buf(),
            loaded_plugins: HashMap::new(),
        }
    }


    pub async fn scan_plugins(&mut self) -> Result<Vec<String>> {
        let mut discovered_plugins = Vec::new();
        if !self.plugins_dir.exists() {
            fs::create_dir_all(&self.plugins_dir)
                .context("Failed to create plugins directory")?;
            return Ok(discovered_plugins);
        }

        let mut entries = fs::read_dir(&self.plugins_dir)
            .context("Failed to read plugins directory")?;

        while let Some(entry) = entries.next().transpose()? {
            let path = entry.path();
            if path.is_dir() {
                let manifest_path = path.join("plugin.toml");
                if manifest_path.exists() {
                    match self.load_plugin_manifest(&manifest_path).await {
                        Ok(manifest) => {
                            let plugin_name = manifest.plugin.name.clone();
                            self.loaded_plugins.insert(plugin_name.clone(), manifest);
                            discovered_plugins.push(plugin_name);
                        },
                        Err(e) => {
                            eprintln!("Failed to load plugin manifest from {:?}: {}", manifest_path, e);
                        }
                    }
                }
            }
        }
        Ok(discovered_plugins)
    }

    async fn load_plugin_manifest(&self, manifest_path: &Path) -> Result<PluginManifest> {
        let content = fs::read_to_string(manifest_path)
            .context("Failed to read plugin manifest")?;
        
        let manifest: PluginManifest = toml::from_str(&content)
            .context("Failed to parse plugin manifest")?;

        self.validate_manifest(&manifest)?;
        Ok(manifest)
    }

    fn validate_manifest(&self, manifest: &PluginManifest) -> Result<()> {
        if manifest.plugin.name.trim().is_empty() {
            anyhow::bail!("Plugin name cannot be empty");
        }
        if manifest.plugin.version.trim().is_empty() {
            anyhow::bail!("Plugin version cannot be empty");
        }

        match manifest.runtime.runtime_type {
            RuntimeType::Process => {
                if manifest.runtime.executable.is_none() {
                    anyhow::bail!("Runtime executable must be specified for 'process' type");
                }
            },
        }
        Ok(())
    }

    pub fn get_plugin_path(&self, plugin_name: &str) -> Option<PathBuf> {
        self.loaded_plugins.get(plugin_name).map(|_| self.plugins_dir.join(plugin_name))
    }

    pub fn get_plugin_manifest(&self, plugin_name: &str) -> Option<&PluginManifest> {
        self.loaded_plugins.get(plugin_name)
    }

    pub fn list_plugins(&self) -> Vec<&str> {
        self.loaded_plugins.keys().map(|k| k.as_str()).collect()
    }

    pub fn get_plugin_command(&self, plugin_name: &str, auth_code: &str, socket_path: &str) -> Result<Option<Command>> {
        let manifest = self.get_plugin_manifest(plugin_name)
            .ok_or_else(|| anyhow::anyhow!("Plugin '{}' not found", plugin_name))?;
        match manifest.runtime.runtime_type {
            RuntimeType::Process => {
                if let Some(executable) = &manifest.runtime.executable {
                    let plugin_dir = self.get_plugin_path(plugin_name).ok_or_else(|| anyhow!("Plugin '{}' path not existed", plugin_name))?;
                    let exe_path = plugin_dir.join(executable);
                    let mut cmd = Command::new(exe_path);

                    if let Some(working_dir) = &manifest.runtime.working_dir {
                        cmd.current_dir(plugin_dir.join(working_dir));
                    } else {
                        cmd.current_dir(&plugin_dir);
                    }

                    cmd.env("RUST_LOG", "info");

                    if let Some(args) = &manifest.runtime.args {
                        cmd.args(args);
                    }

                    cmd.args(["--auth-code", auth_code]);

                    cmd.args(["--socket-path", socket_path]);

                    if let Some(env) = &manifest.runtime.env {
                        cmd.envs(env);
                    }
                    return Ok(Some(cmd));
                }
            }
        }
        Ok(None)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_plugin_loader() -> Result<()> {
        let dir = tempdir()?;
        let plugin_dir = dir.path().join("test_plugin");
        fs::create_dir_all(&plugin_dir)?;

        let manifest_content = r#"
            [plugin]
            name = "test_plugin"
            version = "0.1.0"
            description = "A test plugin"
            author = "Test Author"

            [runtime]
            type = "process"
            executable = "test_executable"
            args = ["--arg1", "value1"]
            env = { KEY1 = "VALUE1" }
        "#;

        fs::write(plugin_dir.join("plugin.toml"), manifest_content)?;

        let mut loader = PluginLoader::new(dir.path());
        let plugins = loader.scan_plugins().await?;
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0], "test_plugin");

        let manifest = loader.get_plugin_manifest("test_plugin").unwrap();
        assert_eq!(manifest.plugin.name, "test_plugin");
        assert_eq!(manifest.runtime.runtime_type, RuntimeType::Process);

        let cmd = loader.get_plugin_command("test_plugin", "test_auth_code", "/tmp/yedmq_plugin.sock")?.unwrap();
        assert_eq!(cmd.as_std().get_program().to_str().unwrap().ends_with("test_executable"), true);
        Ok(())
    }
}