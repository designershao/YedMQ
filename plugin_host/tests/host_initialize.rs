use bytes::Buf;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use yedmq_plugin_host::plugin_host_config;

use crate::common::{InitFailMode, MockConfig};

pub mod common;

#[tokio::test]
pub async fn test_plugin_host_init_scan() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    common::setup_test_plugins(&temp_dir, MockConfig::default());

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = plugin_host_config::PluginHostConfig {
        plugin_directory: temp_dir.path().to_string_lossy().to_string(),
        broker_version: "0.1.0".to_string(),
        broker_node_id: 1,
        cluster_name: "test_cluster".to_string(),
        max_restart_attempts: 1,
        health_check_interval_secs: 5,
        shutdown_signal: tx,
    };

    let plugin_manager = yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config).await.unwrap();
    let mock_plugin_manifest = plugin_manager.get_plugin_manifest("mock_plugin_harness").unwrap();

    assert_eq!(mock_plugin_manifest.plugin.name, "mock_plugin_harness");
}


#[tokio::test]
pub async fn test_plugin_host_start_plugin() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    common::setup_test_plugins(&temp_dir, MockConfig::default());

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = plugin_host_config::PluginHostConfig {
        plugin_directory: temp_dir.path().to_string_lossy().to_string(),
        broker_version: "0.1.0".to_string(),
        broker_node_id: 1,
        cluster_name: "test_cluster".to_string(),
        max_restart_attempts: 1,
        health_check_interval_secs: 5,
        shutdown_signal: tx,
    };

    let mut plugin_manager = yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config).await.unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager.start_plugin("mock_plugin_harness").await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let running_plugins = plugin_manager.get_running_plugins();
    let running_plugins = running_plugins.read().await;
    assert!(running_plugins.contains_key("mock_plugin_harness"));

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin init

    assert!(matches!(running_plugins.get("mock_plugin_harness").unwrap().state, yedmq_plugin_host::plugin_manager::PluginState::Running));

}

#[tokio::test]
async fn when_plugin_init_response_timeout_plugin_host_should_disconnect() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut mock_config = MockConfig::default();
    mock_config.initialize.initialization_fail_mode = InitFailMode::Delay(20 * 1000); // delay longer than plugin host timeout (20s)
    common::setup_test_plugins(&temp_dir, mock_config);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = plugin_host_config::PluginHostConfig {
        plugin_directory: temp_dir.path().to_string_lossy().to_string(),
        broker_version: "0.1.0".to_string(),
        broker_node_id: 1,
        cluster_name: "test_cluster".to_string(),
        max_restart_attempts: 1,
        health_check_interval_secs: 5,
        shutdown_signal: tx,
    };

    let mut plugin_manager = yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config).await.unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager.start_plugin("mock_plugin_harness").await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let running_plugins = plugin_manager.get_running_plugins();
    let mut running_plugins = running_plugins.write().await;
    assert!(running_plugins.contains_key("mock_plugin_harness"));

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin init

    let plugin_process = running_plugins.get_mut("mock_plugin_harness").unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // wait for plugin init

    plugin_process.logs.read().await.iter().for_each(|log| {
        println!("Plugin Log: {}", log);
    });

    // TODO read from plugin_process.stdout to check for disconnect message

    assert!(matches!(running_plugins.get("mock_plugin_harness").unwrap().state, yedmq_plugin_host::plugin_manager::PluginState::Starting));

}