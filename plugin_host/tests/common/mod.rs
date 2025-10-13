use std::path::PathBuf;

use tempfile::TempDir;

pub fn get_mock_plugin_path() -> PathBuf {
    let cargo_target_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target");
    cargo_target_dir.join("debug").join("mock_plugin_harness")
}

pub fn setup_test_plugins(plugins_test_dir: &TempDir) {
    let mock_plugin_dir = plugins_test_dir.path().join("mock_plugin_harness");
    std::fs::create_dir(&mock_plugin_dir).expect("Failed to create mock plugin dir");

    let mock_plugin_exe = get_mock_plugin_path();
    let mock_plugin_harness_dir = plugins_test_dir.path().join("mock_plugin_harness");
    std::fs::create_dir_all(&mock_plugin_harness_dir).expect("Failed to create mock plugin harness dir");
    std::fs::copy(mock_plugin_exe, mock_plugin_harness_dir.join("mock_plugin_harness")).expect("Failed to copy mock plugin exe");

    let mock_plugin_manifest = r###"[plugin]
name = "mock_plugin_harness"
version = "0.1.0"
description = "A test plugin"
author = "Test Author"
license = "MIT"
homepage = "http://test.com"
repository = "https://github.com"

[runtime]
type = "process"
executable = "mock_plugin_harness"
args = []
env = {}
working_dir = "."
timeout_secs = 12
    "###;

    std::fs::write(mock_plugin_dir.join("plugin.toml"), mock_plugin_manifest)
        .expect("Failed to write mock plugin manifest");

}
