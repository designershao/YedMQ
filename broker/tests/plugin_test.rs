use std::{path::PathBuf, sync::Arc};

use rand::seq::index::sample;
use yedmq::plugin_manager::{plugin_metadata::PluginMetadata, PluginManager};

fn get_demo_plugin_path() -> PathBuf {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(crate_root_path)
        .join("tests")
        .join("plugins")
        .join("demo_plugin")
}

fn get_demo_plugins_dir() -> PathBuf {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(crate_root_path)
        .join("tests")
        .join("plugins")
}

#[test]
pub fn test_plugin_metadata_load() {

    let plugin_path = get_demo_plugin_path();

    let plugin_metadata = PluginMetadata::new(plugin_path).unwrap();

    assert!(plugin_metadata.name == "demo_plugin");
    assert!(plugin_metadata.author == "yedmq");
    assert!(plugin_metadata.description == "Just a demo plugin");
    assert!(plugin_metadata.version == "1.0.0");
    assert!(plugin_metadata.entry == "./libexample_plugin.so");
    assert!(plugin_metadata.priority == 1000);

}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
pub async fn test_plugin_service_init() {
    let plugin_dir = get_demo_plugins_dir();
    let settings = yedmq::settings::Settings::default();
    let _ = PluginManager::new(plugin_dir.to_str().unwrap().to_string(), Arc::new(settings)).unwrap();
    assert!(true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
pub async fn test_plugin_service_call_on_connect_auth() {
    let plugin_dir = get_demo_plugins_dir();
    let settings = yedmq::settings::Settings::default();
    let _ = PluginManager::new(plugin_dir.to_str().unwrap().to_string(), Arc::new(settings)).unwrap();
}