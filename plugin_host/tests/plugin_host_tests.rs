use std::time::{Duration, Instant};

use yedmq_plugin_host::{
    plugin_host_config,
    protocol::plugin_protocol::{AuthenticateRequest, MessagePublishRequest, MqttMessage},
};

use crate::common::{HookConfig, InitFailMode, MockConfig};

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
        local_socket_path: temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy()
            .to_string(),
    };

    let plugin_manager = yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
        .await
        .unwrap();
    let mock_plugin_manifest = plugin_manager
        .get_plugin_manifest("mock_plugin_harness")
        .unwrap();

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
        local_socket_path: temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy()
            .to_string(),
    };

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let running_plugins = plugin_manager.get_running_plugins();
    let running_plugins = running_plugins.read().await;
    assert!(running_plugins.contains_key("mock_plugin_harness"));

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin init

    assert!(matches!(
        running_plugins.get("mock_plugin_harness").unwrap().state,
        yedmq_plugin_host::plugin_manager::PluginState::Running
    ));
}

#[tokio::test]
async fn when_plugin_init_response_timeout_plugin_host_should_disconnect() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut mock_config = MockConfig::default();
    mock_config.initialize.initialization_fail_mode = InitFailMode::SkipResponse; // delay longer than plugin host timeout (20s)
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
        local_socket_path: temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy()
            .to_string(),
    };

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    {
        let running_plugins = plugin_manager.get_running_plugins();
        let running_plugins = running_plugins.read().await;

        assert!(running_plugins.contains_key("mock_plugin_harness"));
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(6)).await; // wait for plugin init

    let running_plugins = plugin_manager.get_running_plugins();

    let running_plugins = running_plugins.read().await;

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    let logs = plugin_process.logs.read().await;
    let full_logs = logs.join("\n");

    assert!(full_logs.contains("Connection closed by host"));
    assert!(matches!(
        plugin_process.state,
        yedmq_plugin_host::plugin_manager::PluginState::Stopped
    ));
}

#[tokio::test]
pub async fn when_call_stop_plugin_plugin_host_should_stop_plugin() {
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
        local_socket_path: temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy()
            .to_string(),
    };

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    plugin_manager
        .stop_plugin("mock_plugin_harness")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await; // wait for plugin to stop

    let running_plugins = plugin_manager.get_running_plugins();

    let running_plugins = running_plugins.read().await;

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    assert!(plugin_process.state == yedmq_plugin_host::plugin_manager::PluginState::Stopped);
    assert!(plugin_process.process_log_handle.is_none());
    assert!(plugin_process.process_wait_handle.is_none());
}

#[tokio::test]
pub async fn when_call_restart_plugin_plugin_host_should_restart_plugin() {
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
        local_socket_path: temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy()
            .to_string(),
    };

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let mut pre_plugin_start_time = Instant::now();

    {
        let running_plugins = plugin_manager.get_running_plugins();

        let running_plugins = running_plugins.read().await;

        let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

        pre_plugin_start_time = plugin_process.start_time.unwrap().clone();
    }

    plugin_manager
        .restart_plugin("mock_plugin_harness")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    let running_plugins = plugin_manager.get_running_plugins();

    let running_plugins = running_plugins.read().await;

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    let latest_plugin_start_time = plugin_process.start_time.unwrap().clone();

    assert!(plugin_process.state == yedmq_plugin_host::plugin_manager::PluginState::Running);
    assert!(latest_plugin_start_time > pre_plugin_start_time);
}

#[tokio::test]
pub async fn when_call_authenticate_hook_plugin_host_should_call_plugin_authenticate_method() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut mock_config = MockConfig::default();
    mock_config.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 1,
    }];
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
        local_socket_path: temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy()
            .to_string(),
    };

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let authenticate_request = AuthenticateRequest {
        password: "test_password".to_string(),
        client_id: "test_client_id".to_string(),
        username: "test_username".to_string(),
        client_ip: "127.0.0.1".to_string(),
        client_cert: Vec::new(),
        protocol_version: "3.1.1".to_string(),
        properties: None,
    };

    let result = plugin_manager
        .call_authenticate_hook(authenticate_request)
        .await
        .unwrap();

    assert!(result.authenticated == true);

    let running_plugins = plugin_manager.get_running_plugins();

    let running_plugins = running_plugins.read().await;

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    let logs = plugin_process.logs.read().await;

    let full_logs = logs.join("\n");

    assert!(full_logs.contains("Handling authenticate request"));
}

#[tokio::test]
pub async fn when_call_message_published_event_plugin_host_should_call_plugin_message_published_method(
) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut mock_config = MockConfig::default();
    mock_config.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 1,
    },HookConfig {
        name: "MessagePublished".to_string(),
        priority: 1,
    }];
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
        local_socket_path: temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy()
            .to_string(),
    };

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let message_publish_request = MessagePublishRequest {
        message: Some(MqttMessage {
            tenant_id: "test_tenant".to_string(),
            client_id: "test_client_id".to_string(),
            topic: "test_topic".to_string(),
            payload: Vec::new(),
            qos: 0,
            retain: false,
            dup: false,
            publish_time: None,
            properties: None,
            message_id: Some("test_message_id".to_string()),
        }),
        context: None,
    };
    plugin_manager.call_message_published_hook(message_publish_request).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let running_plugins = plugin_manager.get_running_plugins();

    let running_plugins = running_plugins.read().await;

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    let logs = plugin_process.logs.read().await;

    let full_logs = logs.join("\n");

    assert!(full_logs.contains("Handling message published request"));
}

#[tokio::test]
pub async fn when_call_on_message_publish_plugin_host_should_call_plugin_on_message_publish_method() {

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut mock_config = MockConfig::default();
    mock_config.on_message_publish.allow = true;
    mock_config.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 1,
    },HookConfig {
        name: "OnMessagePublish".to_string(),
        priority: 1,
    }];
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
        local_socket_path: temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy()
            .to_string(),
    };

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let message_publish_request = MessagePublishRequest {
        message: Some(MqttMessage {
            tenant_id: "test_tenant".to_string(),
            client_id: "test_client_id".to_string(),
            topic: "test_topic".to_string(),
            payload: Vec::new(),
            qos: 0,
            retain: false,
            dup: false,
            publish_time: None,
            properties: None,
            message_id: Some("test_message_id".to_string()),
        }),
        context: None,
    };

    let res = plugin_manager.call_on_message_publish(message_publish_request).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let running_plugins = plugin_manager.get_running_plugins();

    let running_plugins = running_plugins.read().await;

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    let logs = plugin_process.logs.read().await;

    let full_logs = logs.join("\n");

    assert!(res.unwrap().allow);

    assert!(full_logs.contains("Handling on_message_publish request"));

}