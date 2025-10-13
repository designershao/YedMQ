use yedmq_plugin_host::plugin_host_config;

pub mod common;

#[tokio::test]
pub async fn test_plugin_host_init_scan() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    common::setup_test_plugins(&temp_dir);

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