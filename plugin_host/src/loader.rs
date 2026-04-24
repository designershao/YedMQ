use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};
use tokio::process::Command;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    pub plugin: PluginInfo,
    pub runtime: RuntimeConfig,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidPluginManifest {
    pub path: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginScanReport {
    pub discovered_plugins: Vec<String>,
    pub invalid_plugins: Vec<InvalidPluginManifest>,
}

pub struct PluginLoader {
    plugins_dir: PathBuf,
    loaded_plugins: HashMap<String, PluginManifest>,
}

impl PluginLoader {
    pub fn new<P: AsRef<Path>>(plugins_dir: P) -> Self {
        Self {
            plugins_dir: plugins_dir.as_ref().to_path_buf(),
            loaded_plugins: HashMap::new(),
        }
    }

    pub fn scan_plugins(&mut self) -> Result<PluginScanReport> {
        let mut discovered_plugins = Vec::new();
        let mut invalid_plugins = Vec::new();
        if !self.plugins_dir.exists() {
            fs::create_dir_all(&self.plugins_dir).context("Failed to create plugins directory")?;
            self.loaded_plugins.clear();
            return Ok(PluginScanReport {
                discovered_plugins,
                invalid_plugins,
            });
        }

        let mut next_loaded_plugins = HashMap::new();
        let mut entries = fs::read_dir(&self.plugins_dir)
            .context("Failed to read plugins directory")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to enumerate plugins directory")?;
        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                let manifest_path = path.join("plugin.toml");
                if manifest_path.exists() {
                    match self.load_plugin_manifest(&manifest_path) {
                        Ok(manifest) => {
                            let plugin_name = manifest.plugin.name.clone();
                            next_loaded_plugins.insert(plugin_name.clone(), manifest);
                            discovered_plugins.push(plugin_name);
                        }
                        Err(e) => {
                            invalid_plugins.push(InvalidPluginManifest {
                                path: manifest_path.to_string_lossy().into_owned(),
                                error: e.to_string(),
                            });
                        }
                    }
                }
            }
        }

        self.loaded_plugins = next_loaded_plugins;

        Ok(PluginScanReport {
            discovered_plugins,
            invalid_plugins,
        })
    }

    fn load_plugin_manifest(&self, manifest_path: &Path) -> Result<PluginManifest> {
        let content =
            fs::read_to_string(manifest_path).context("Failed to read plugin manifest")?;

        let manifest: PluginManifest =
            toml::from_str(&content).context("Failed to parse plugin manifest")?;

        self.validate_manifest(&manifest)?;
        Ok(manifest)
    }

    pub fn plugin_dir_path(&self, plugin_name: &str) -> PathBuf {
        self.plugins_dir.join(plugin_name)
    }

    pub fn plugin_manifest_path(&self, plugin_name: &str) -> PathBuf {
        self.plugin_dir_path(plugin_name).join("plugin.toml")
    }

    pub fn load_plugin_manifest_from_disk(&self, plugin_name: &str) -> Result<PluginManifest> {
        let manifest_path = self.plugin_manifest_path(plugin_name);
        if !manifest_path.is_file() {
            anyhow::bail!(
                "Plugin '{}' manifest not found at '{}'",
                plugin_name,
                manifest_path.display()
            );
        }

        self.load_plugin_manifest(&manifest_path)
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
            }
        }
        Ok(())
    }

    pub fn get_plugin_path(&self, plugin_name: &str) -> Option<PathBuf> {
        self.loaded_plugins
            .get(plugin_name)
            .map(|_| self.plugin_dir_path(plugin_name))
    }

    pub fn get_plugin_manifest(&self, plugin_name: &str) -> Option<&PluginManifest> {
        self.loaded_plugins.get(plugin_name)
    }

    pub fn list_plugins(&self) -> Vec<&str> {
        self.loaded_plugins.keys().map(|k| k.as_str()).collect()
    }

    pub fn list_plugin_manifests(&self) -> Vec<PluginManifest> {
        let mut manifests = self.loaded_plugins.values().cloned().collect::<Vec<_>>();
        manifests.sort_by(|left, right| left.plugin.name.cmp(&right.plugin.name));
        manifests
    }

    pub fn get_plugin_command(
        &self,
        plugin_name: &str,
        auth_code: &str,
        socket_path: &str,
    ) -> Result<Option<Command>> {
        let manifest = self
            .get_plugin_manifest(plugin_name)
            .ok_or_else(|| anyhow::anyhow!("Plugin '{}' not found", plugin_name))?;
        self.get_plugin_command_from_manifest(plugin_name, manifest, auth_code, socket_path)
    }

    pub fn get_plugin_command_from_manifest(
        &self,
        plugin_name: &str,
        manifest: &PluginManifest,
        auth_code: &str,
        socket_path: &str,
    ) -> Result<Option<Command>> {
        match manifest.runtime.runtime_type {
            RuntimeType::Process => {
                if let Some(executable) = &manifest.runtime.executable {
                    let plugin_dir = self.plugin_dir_path(plugin_name);
                    if !plugin_dir.is_dir() {
                        anyhow::bail!("Plugin '{}' path not existed", plugin_name);
                    }
                    #[cfg(windows)]
                    let mut exe_path = plugin_dir.join(executable);
                    #[cfg(not(windows))]
                    let exe_path = plugin_dir.join(executable);

                    #[cfg(windows)]
                    if exe_path.extension().is_none() && !exe_path.exists() {
                        let candidate = exe_path.with_extension("exe");
                        if candidate.exists() {
                            exe_path = candidate;
                        }
                    }

                    if !exe_path.exists() {
                        anyhow::bail!(
                            "Plugin '{}' executable not found at '{}'",
                            plugin_name,
                            exe_path.display()
                        );
                    }

                    let abs_path = std::fs::canonicalize(exe_path.clone())?;

                    let mut cmd = Command::new(&abs_path);

                    if let Some(working_dir) = &manifest.runtime.working_dir {
                        let working_dir_path = plugin_dir.join(working_dir);
                        if !working_dir_path.is_dir() {
                            anyhow::bail!(
                                "Plugin '{}' working_dir not found at '{}'",
                                plugin_name,
                                working_dir_path.display()
                            );
                        }
                        let working_dir_abs_path = std::fs::canonicalize(working_dir_path)?;
                        cmd.current_dir(working_dir_abs_path);
                    } else {
                        let plugin_dir_abs_path = std::fs::canonicalize(plugin_dir)?;
                        cmd.current_dir(&plugin_dir_abs_path);
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
#[allow(clippy::unwrap_used)]
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
        #[cfg(windows)]
        fs::write(plugin_dir.join("test_executable.exe"), b"")?;
        #[cfg(not(windows))]
        fs::write(plugin_dir.join("test_executable"), b"")?;

        let mut loader = PluginLoader::new(dir.path());
        let plugins = loader.scan_plugins()?;
        assert_eq!(plugins.discovered_plugins.len(), 1);
        assert_eq!(plugins.discovered_plugins[0], "test_plugin");

        let manifest = loader.get_plugin_manifest("test_plugin").unwrap();
        assert_eq!(manifest.plugin.name, "test_plugin");
        assert_eq!(manifest.runtime.runtime_type, RuntimeType::Process);

        let cmd = loader
            .get_plugin_command("test_plugin", "test_auth_code", "/tmp/yedmq_plugin.sock")?
            .unwrap();
        #[cfg(windows)]
        assert!(cmd
            .as_std()
            .get_program()
            .to_str()
            .unwrap()
            .ends_with("test_executable.exe"));
        #[cfg(not(windows))]
        assert!(cmd
            .as_std()
            .get_program()
            .to_str()
            .unwrap()
            .ends_with("test_executable"));
        Ok(())
    }
}
