use std::{collections::HashMap, time::Duration};

use yedmq_plugin_host::{
    plugin_host_config,
    protocol::plugin_protocol::{
        AuthAction, AuthenticateRequest, AuthorizeRequest, MessagePublishRequest, MqttMessage,
        SubscribeRequest, TopicFilter,
    },
};

use crate::common::{HookConfig, InitFailMode, MockConfig};

pub mod common;

pub fn get_plugin_host_test_config(
    sender: tokio::sync::broadcast::Sender<()>,
    temp_dir: &tempfile::TempDir,
    local_socket_path: &str,
) -> plugin_host_config::PluginHostConfig {
    plugin_host_config::PluginHostConfig {
        plugin_directory: temp_dir.path().to_string_lossy().to_string(),
        broker_version: "0.1.0".to_string(),
        broker_node_id: 1,
        cluster_name: "test_cluster".to_string(),
        max_restart_attempts: 3,
        health_check_interval_secs: 10,
        shutdown_signal: sender,
        local_socket_path: local_socket_path.to_string(),
        default_authenticate_result: true,
        default_authorize_result: true,
    }
}

#[tokio::test]
pub async fn test_plugin_host_init_scan() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    common::setup_test_plugins(&temp_dir, MockConfig::default());

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

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

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

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
    assert!(running_plugins.contains_key("mock_plugin_harness"));

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin init

    assert!(matches!(
        running_plugins.get("mock_plugin_harness").unwrap().state,
        yedmq_plugin_host::plugin_manager::PluginState::Running
    ));
}

#[tokio::test]
async fn when_no_plugin_existed_call_authenticate_plugin_host_should_return_default_result() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

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

    assert!(result.authenticated);
}

#[tokio::test]
async fn when_no_plugin_existed_call_authorize_plugin_host_should_return_default_result() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    let authorize_request = AuthorizeRequest {
        client_id: "test_client_id".to_string(),
        username: "test_username".to_string(),
        topic: "test/topic".to_string(),
        action: AuthAction::Publish.into(),
        context: None,
        tenant_id: "test_tenant".to_string(),
        qos: 0,
    };

    let result = plugin_manager
        .call_authorize_hook(authorize_request)
        .await
        .unwrap();

    assert!(result.authorized);
}

#[tokio::test]
async fn when_plugin_stopped_plugin_host_should_change_the_plugin_state() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut mock_config = MockConfig::default();
    mock_config.initialize.exit_after_init_delay_secs = Some(5);
    common::setup_test_plugins(&temp_dir, mock_config);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

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

        assert!(running_plugins.contains_key("mock_plugin_harness"));
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // wait for plugin init

    {
        let running_plugins = plugin_manager.get_running_plugins();

        println!(
            "Plugin States: {:?}",
            running_plugins.get("mock_plugin_harness").unwrap().state
        );

        assert!(
            running_plugins.get("mock_plugin_harness").unwrap().state
                == yedmq_plugin_host::plugin_manager::PluginState::Running
        );
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(6)).await; // wait for plugin exit

    let running_plugins = plugin_manager.get_running_plugins();

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    assert!(matches!(
        plugin_process.state,
        yedmq_plugin_host::plugin_manager::PluginState::Stopped
    ));
}

#[tokio::test]
async fn when_plugin_init_response_timeout_plugin_host_should_disconnect() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut mock_config = MockConfig::default();
    mock_config.initialize.initialization_fail_mode = InitFailMode::SkipResponse; // delay longer than plugin host timeout (20s)
    common::setup_test_plugins(&temp_dir, mock_config);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

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

        assert!(running_plugins.contains_key("mock_plugin_harness"));
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(6)).await; // wait for plugin init

    let running_plugins = plugin_manager.get_running_plugins();

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

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

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

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    assert!(plugin_process.state == yedmq_plugin_host::plugin_manager::PluginState::Stopped);
    assert!(plugin_process.process_log_handle.is_none());
    assert!(plugin_process.process_wait_handle.is_none());
}

#[tokio::test]
pub async fn when_call_restart_plugin_plugin_host_should_restart_plugin() {
    env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Info)
        .init();
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    common::setup_test_plugins(&temp_dir, MockConfig::default());

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

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

    let pre_plugin_start_time = {
        let running_plugins = plugin_manager.get_running_plugins();

        let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

        plugin_process.start_time.unwrap()
    };

    plugin_manager
        .restart_plugin("mock_plugin_harness")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await; // wait for listener to start

    let running_plugins = plugin_manager.get_running_plugins();

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    let latest_plugin_start_time = plugin_process.start_time.unwrap();

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

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

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

    assert!(result.authenticated);

    let running_plugins = plugin_manager.get_running_plugins();

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
    mock_config.initialize.hooks = vec![
        HookConfig {
            name: "Authenticate".to_string(),
            priority: 1,
        },
        HookConfig {
            name: "MessagePublished".to_string(),
            priority: 1,
        },
    ];
    common::setup_test_plugins(&temp_dir, mock_config);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

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
    plugin_manager
        .call_message_published_hook(message_publish_request)
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let running_plugins = plugin_manager.get_running_plugins();

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    let logs = plugin_process.logs.read().await;

    let full_logs = logs.join("\n");

    assert!(full_logs.contains("Handling message published request"));
}

#[tokio::test]
pub async fn when_call_on_message_publish_plugin_host_should_call_plugin_on_message_publish_method()
{
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut mock_config = MockConfig::default();
    mock_config.on_message_publish.allow = true;
    mock_config.initialize.hooks = vec![
        HookConfig {
            name: "Authenticate".to_string(),
            priority: 1,
        },
        HookConfig {
            name: "OnMessagePublish".to_string(),
            priority: 1,
        },
    ];
    common::setup_test_plugins(&temp_dir, mock_config);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

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

    let res = plugin_manager
        .call_on_message_publish(message_publish_request)
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let running_plugins = plugin_manager.get_running_plugins();

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    let logs = plugin_process.logs.read().await;

    let full_logs = logs.join("\n");

    assert!(res.unwrap().allow);

    assert!(full_logs.contains("Handling on_message_publish request"));
}

#[tokio::test]
pub async fn when_call_on_message_subscribe_plugin_host_should_call_plugin_on_message_subscribe_method(
) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut mock_config = MockConfig::default();
    mock_config.subscribe.default_allow = true;
    mock_config.initialize.hooks = vec![
        HookConfig {
            name: "Authenticate".to_string(),
            priority: 1,
        },
        HookConfig {
            name: "OnMessageSubscribe".to_string(),
            priority: 1,
        },
    ];
    common::setup_test_plugins(&temp_dir, mock_config);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

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

    let subscribe_request = SubscribeRequest {
        client_id: "test_client_id".to_string(),
        subscriptions: vec![TopicFilter {
            topic: "test_topic".to_string(),
            qos: 0,
            options: None,
        }],
        context: None,
    };

    let res = plugin_manager
        .call_on_message_subscribe(subscribe_request)
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let running_plugins = plugin_manager.get_running_plugins();

    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    let logs = plugin_process.logs.read().await;

    let full_logs = logs.join("\n");

    assert!(res.as_ref().unwrap().result[0].allowed);
    assert!(res.as_ref().unwrap().result[0].granted_qos == 0);
    assert!(res.as_ref().unwrap().result[0].topic == "test_topic");

    assert!(full_logs.contains("Handling on_message_subscribe request"));
}

#[tokio::test]
pub async fn when_call_on_message_subscribe_plugin_host_should_call_plugins_strictly_by_priority_in_full_chain(
) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let mut mock_plugin_config_1 = MockConfig::default();
    mock_plugin_config_1.subscribe.default_allow = true;
    mock_plugin_config_1.subscribe.results = vec![("test_topic".to_string(), true, 0)];
    mock_plugin_config_1.subscribe.continue_chain = true;
    mock_plugin_config_1.initialize.hooks = vec![HookConfig {
        name: "OnMessageSubscribe".to_string(),
        priority: 1,
    }];

    let mut mock_plugin_config_2 = MockConfig::default();
    mock_plugin_config_2.subscribe.default_allow = true;
    mock_plugin_config_1.subscribe.results = vec![("test_topic".to_string(), true, 0)];
    mock_plugin_config_2.subscribe.continue_chain = true;
    mock_plugin_config_2.initialize.hooks = vec![HookConfig {
        name: "OnMessageSubscribe".to_string(),
        priority: 2,
    }];

    let mut mock_plugin_config_3 = MockConfig::default();
    mock_plugin_config_3.subscribe.default_allow = true;
    mock_plugin_config_3.subscribe.continue_chain = true;
    mock_plugin_config_3.subscribe.results = vec![("test_topic".to_string(), false, 0)];
    mock_plugin_config_3.initialize.hooks = vec![HookConfig {
        name: "OnMessageSubscribe".to_string(),
        priority: 3,
    }];

    let config_hashmap = HashMap::from([
        ("mock_plugin_harness_1".to_string(), mock_plugin_config_1),
        ("mock_plugin_harness_2".to_string(), mock_plugin_config_2),
        ("mock_plugin_harness_3".to_string(), mock_plugin_config_3),
    ]);

    common::setup_mutiple_test_plugins(&temp_dir, config_hashmap);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness_1")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_2")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_3")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let subscribe_request = SubscribeRequest {
        client_id: "test_client_id".to_string(),
        subscriptions: vec![TopicFilter {
            topic: "test_topic".to_string(),
            qos: 0,
            options: None,
        }],
        context: None,
    };

    let res = plugin_manager
        .call_on_message_subscribe(subscribe_request)
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // enuse all plugin are called

    let plugin_1_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_1")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_1_log.contains("Handling on_message_subscribe request"));

    let plugin_2_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_2")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_2_log.contains("Handling on_message_subscribe request"));

    let plugin_3_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_3")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_3_log.contains("Handling on_message_subscribe request"));
    //

    assert!(!res.as_ref().unwrap().result[0].allowed);
    assert!(res.as_ref().unwrap().result[0].granted_qos == 0);
    assert!(res.as_ref().unwrap().result[0].topic == "test_topic");
}

#[tokio::test]
pub async fn when_call_on_message_subscribe_and_plugin_breaks_chain_host_should_immediately_stop_calling_lower_priority_plugins(
) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let mut mock_plugin_config_1 = MockConfig::default();
    mock_plugin_config_1.subscribe.default_allow = true;
    mock_plugin_config_1.subscribe.results = vec![("test_topic".to_string(), true, 0)];
    mock_plugin_config_1.subscribe.continue_chain = true;
    mock_plugin_config_1.initialize.hooks = vec![HookConfig {
        name: "OnMessageSubscribe".to_string(),
        priority: 1,
    }];

    let mut mock_plugin_config_2 = MockConfig::default();
    mock_plugin_config_2.subscribe.default_allow = true;
    mock_plugin_config_1.subscribe.results = vec![("test_topic".to_string(), true, 0)];
    mock_plugin_config_2.subscribe.continue_chain = false;
    mock_plugin_config_2.initialize.hooks = vec![HookConfig {
        name: "OnMessageSubscribe".to_string(),
        priority: 2,
    }];

    let mut mock_plugin_config_3 = MockConfig::default();
    mock_plugin_config_3.subscribe.default_allow = true;
    mock_plugin_config_3.subscribe.continue_chain = true;
    mock_plugin_config_3.subscribe.results = vec![("test_topic".to_string(), false, 0)];
    mock_plugin_config_3.initialize.hooks = vec![HookConfig {
        name: "OnMessageSubscribe".to_string(),
        priority: 3,
    }];

    let config_hashmap = HashMap::from([
        ("mock_plugin_harness_1".to_string(), mock_plugin_config_1),
        ("mock_plugin_harness_2".to_string(), mock_plugin_config_2),
        ("mock_plugin_harness_3".to_string(), mock_plugin_config_3),
    ]);

    common::setup_mutiple_test_plugins(&temp_dir, config_hashmap);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness_1")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_2")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_3")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let subscribe_request = SubscribeRequest {
        client_id: "test_client_id".to_string(),
        subscriptions: vec![TopicFilter {
            topic: "test_topic".to_string(),
            qos: 0,
            options: None,
        }],
        context: None,
    };

    let res = plugin_manager
        .call_on_message_subscribe(subscribe_request)
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // enuse all plugin are called

    let plugin_1_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_1")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_1_log.contains("Handling on_message_subscribe request"));

    let plugin_2_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_2")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_2_log.contains("Handling on_message_subscribe request"));

    let plugin_3_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_3")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(!plugin_3_log.contains("Handling on_message_subscribe request"));
    //

    assert!(res.as_ref().unwrap().result[0].allowed);
    assert!(res.as_ref().unwrap().result[0].granted_qos == 0);
    assert!(res.as_ref().unwrap().result[0].topic == "test_topic");
}

#[tokio::test]
pub async fn when_call_authenticate_hook_and_plugin_denies_host_should_stop_chain_and_deny_access()
{
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let mut mock_plugin_config_1 = MockConfig::default();
    mock_plugin_config_1.authenticate.authenticated = true;
    mock_plugin_config_1.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 1,
    }];

    let mut mock_plugin_config_2 = MockConfig::default();
    mock_plugin_config_2.authenticate.authenticated = false;
    mock_plugin_config_2.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 2,
    }];

    let mut mock_plugin_config_3 = MockConfig::default();
    mock_plugin_config_3.authenticate.authenticated = true;
    mock_plugin_config_3.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 3,
    }];

    let config_hashmap = HashMap::from([
        ("mock_plugin_harness_1".to_string(), mock_plugin_config_1),
        ("mock_plugin_harness_2".to_string(), mock_plugin_config_2),
        ("mock_plugin_harness_3".to_string(), mock_plugin_config_3),
    ]);

    common::setup_mutiple_test_plugins(&temp_dir, config_hashmap);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness_1")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_2")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_3")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let authenticate_request = AuthenticateRequest {
        client_id: "test_client_id".to_string(),
        username: "test_username".to_string(),
        password: "test_password".to_string(),
        client_ip: "127.0.0.1".to_string(),
        client_cert: Vec::new(),
        protocol_version: "3.1.1".to_string(),
        properties: None,
    };

    let res = plugin_manager
        .call_authenticate_hook(authenticate_request)
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // enuse all plugin are called

    let plugin_1_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_1")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_1_log.contains("Handling authenticate request"));

    let plugin_2_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_2")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_2_log.contains("Handling authenticate request"));

    let plugin_3_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_3")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(!plugin_3_log.contains("Handling authenticate request"));
    //

    assert!(!res.as_ref().unwrap().authenticated);
}

#[tokio::test]
pub async fn when_call_authenticate_hook_and_plugin_response_tenant_id_conflict_host_should_stop_chain_and_deny_access(
) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let mut mock_plugin_config_1 = MockConfig::default();
    mock_plugin_config_1.authenticate.authenticated = true;
    mock_plugin_config_1.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 1,
    }];

    let mut mock_plugin_config_2 = MockConfig::default();
    mock_plugin_config_2.authenticate.authenticated = true;
    mock_plugin_config_2.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 2,
    }];

    let mut mock_plugin_config_3 = MockConfig::default();
    mock_plugin_config_3.authenticate.authenticated = true;
    mock_plugin_config_3.authenticate.tenant_id = Some("test_tenant_id".to_string());
    mock_plugin_config_3.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 3,
    }];

    let config_hashmap = HashMap::from([
        ("mock_plugin_harness_1".to_string(), mock_plugin_config_1),
        ("mock_plugin_harness_2".to_string(), mock_plugin_config_2),
        ("mock_plugin_harness_3".to_string(), mock_plugin_config_3),
    ]);

    common::setup_mutiple_test_plugins(&temp_dir, config_hashmap);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness_1")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_2")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_3")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let authenticate_request = AuthenticateRequest {
        client_id: "test_client_id".to_string(),
        username: "test_username".to_string(),
        password: "test_password".to_string(),
        client_ip: "127.0.0.1".to_string(),
        client_cert: Vec::new(),
        protocol_version: "3.1.1".to_string(),
        properties: None,
    };

    let res = plugin_manager
        .call_authenticate_hook(authenticate_request)
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // enuse all plugin are called

    let plugin_1_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_1")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_1_log.contains("Handling authenticate request"));

    let plugin_2_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_2")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_2_log.contains("Handling authenticate request"));

    let plugin_3_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_3")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_3_log.contains("Handling authenticate request"));
    //

    assert!(!res.as_ref().unwrap().authenticated);
    assert_eq!(
        res.as_ref().unwrap().error_reason,
        Some("Tenant ID mismatch".to_string())
    );
}

#[tokio::test]
pub async fn when_call_authenticate_hook_and_all_plugin_execute_timeout_host_should_deny_access() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let mut mock_plugin_config_1 = MockConfig::default();
    mock_plugin_config_1.authenticate.authenticated = true;
    mock_plugin_config_1.authenticate.delay_secs = Some(6);
    mock_plugin_config_1.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 1,
    }];

    let mut mock_plugin_config_2 = MockConfig::default();
    mock_plugin_config_2.authenticate.authenticated = true;
    mock_plugin_config_1.authenticate.delay_secs = Some(6);
    mock_plugin_config_2.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 2,
    }];

    let mut mock_plugin_config_3 = MockConfig::default();
    mock_plugin_config_3.authenticate.authenticated = true;
    mock_plugin_config_1.authenticate.delay_secs = Some(6);
    mock_plugin_config_3.initialize.hooks = vec![HookConfig {
        name: "Authenticate".to_string(),
        priority: 3,
    }];

    let config_hashmap = HashMap::from([
        ("mock_plugin_harness_1".to_string(), mock_plugin_config_1),
        ("mock_plugin_harness_2".to_string(), mock_plugin_config_2),
        ("mock_plugin_harness_3".to_string(), mock_plugin_config_3),
    ]);

    common::setup_mutiple_test_plugins(&temp_dir, config_hashmap);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness_1")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_2")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_3")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let authenticate_request = AuthenticateRequest {
        client_id: "test_client_id".to_string(),
        username: "test_username".to_string(),
        password: "test_password".to_string(),
        client_ip: "127.0.0.1".to_string(),
        client_cert: Vec::new(),
        protocol_version: "3.1.1".to_string(),
        properties: None,
    };

    let res = plugin_manager
        .call_authenticate_hook(authenticate_request)
        .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // enuse all plugin are called

    let plugin_1_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_1")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_1_log.contains("Delaying authenticate response by 6 seconds"));

    let plugin_2_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_2")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(!plugin_2_log.contains("Handling authenticate request"));

    let plugin_3_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_3")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(!plugin_3_log.contains("Handling authenticate request"));
    //

    assert!(!res.as_ref().unwrap().authenticated);
    assert_eq!(
        res.as_ref().unwrap().error_reason,
        Some(format!(
            "Plugin {} response timeout",
            "mock_plugin_harness_1"
        ))
    );
}

#[tokio::test]
pub async fn when_call_authorize_hook_and_plugin_denies_host_should_stop_chain_and_deny_access() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let mut mock_plugin_config_1 = MockConfig::default();
    mock_plugin_config_1.authorize.authorized = false;
    mock_plugin_config_1.initialize.hooks = vec![HookConfig {
        name: "Authorize".to_string(),
        priority: 1,
    }];

    let mut mock_plugin_config_2 = MockConfig::default();
    mock_plugin_config_2.authorize.authorized = true;
    mock_plugin_config_2.initialize.hooks = vec![HookConfig {
        name: "Authorize".to_string(),
        priority: 2,
    }];

    let mut mock_plugin_config_3 = MockConfig::default();
    mock_plugin_config_3.authorize.authorized = true;
    mock_plugin_config_3.initialize.hooks = vec![HookConfig {
        name: "Authorize".to_string(),
        priority: 3,
    }];

    let config_hashmap = HashMap::from([
        ("mock_plugin_harness_1".to_string(), mock_plugin_config_1),
        ("mock_plugin_harness_2".to_string(), mock_plugin_config_2),
        ("mock_plugin_harness_3".to_string(), mock_plugin_config_3),
    ]);

    common::setup_mutiple_test_plugins(&temp_dir, config_hashmap);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness_1")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_2")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_3")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let authorize_request = AuthorizeRequest {
        client_id: "test_client_id".to_string(),
        username: "test_username".to_string(),
        topic: "test/topic".to_string(),
        context: None,
        tenant_id: "test_tenant".to_string(),
        action: AuthAction::Subscribe.into(),
        qos: 0,
    };

    let res = plugin_manager.call_authorize_hook(authorize_request).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // enuse all plugin are called

    let plugin_1_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_1")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_1_log.contains("Handling authorize request"));

    let plugin_2_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_2")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(!plugin_2_log.contains("Handling authorize request"));

    let plugin_3_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_3")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(!plugin_3_log.contains("Handling authorize request"));
    //

    assert!(!res.as_ref().unwrap().authorized);
}

#[tokio::test]
pub async fn when_call_authorize_hook_and_all_plugin_execute_timeout_host_should_deny_access() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let mut mock_plugin_config_1 = MockConfig::default();
    mock_plugin_config_1.authorize.authorized = false;
    mock_plugin_config_1.authorize.delay_secs = Some(6);
    mock_plugin_config_1.initialize.hooks = vec![HookConfig {
        name: "Authorize".to_string(),
        priority: 1,
    }];

    let mut mock_plugin_config_2 = MockConfig::default();
    mock_plugin_config_2.authorize.authorized = true;
    mock_plugin_config_2.authorize.delay_secs = Some(6);
    mock_plugin_config_2.initialize.hooks = vec![HookConfig {
        name: "Authorize".to_string(),
        priority: 2,
    }];

    let mut mock_plugin_config_3 = MockConfig::default();
    mock_plugin_config_3.authorize.authorized = true;
    mock_plugin_config_3.authorize.delay_secs = Some(6);
    mock_plugin_config_3.initialize.hooks = vec![HookConfig {
        name: "Authorize".to_string(),
        priority: 3,
    }];

    let config_hashmap = HashMap::from([
        ("mock_plugin_harness_1".to_string(), mock_plugin_config_1),
        ("mock_plugin_harness_2".to_string(), mock_plugin_config_2),
        ("mock_plugin_harness_3".to_string(), mock_plugin_config_3),
    ]);

    common::setup_mutiple_test_plugins(&temp_dir, config_hashmap);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    plugin_manager
        .start_plugin("mock_plugin_harness_1")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_2")
        .await
        .unwrap();
    plugin_manager
        .start_plugin("mock_plugin_harness_3")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for plugin to start

    let authorize_request = AuthorizeRequest {
        client_id: "test_client_id".to_string(),
        username: "test_username".to_string(),
        topic: "test/topic".to_string(),
        context: None,
        tenant_id: "test_tenant".to_string(),
        action: AuthAction::Subscribe.into(),
        qos: 0,
    };

    let res = plugin_manager.call_authorize_hook(authorize_request).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // enuse all plugin are called

    let plugin_1_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_1")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(plugin_1_log.contains("Handling authorize request"));

    let plugin_2_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_2")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(!plugin_2_log.contains("Handling authorize request"));

    let plugin_3_log = plugin_manager
        .get_running_plugins()
        .get("mock_plugin_harness_3")
        .unwrap()
        .logs
        .read()
        .await
        .join("\n");

    assert!(!plugin_3_log.contains("Handling authorize request"));
    //

    assert!(!res.as_ref().unwrap().authorized);
    assert_eq!(
        res.as_ref().unwrap().reason.as_ref().unwrap(),
        "Plugin mock_plugin_harness_1 response timeout"
    );
}

#[tokio::test]
async fn when_plugin_start_failed_plugin_state_should_be_failed() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Create a plugin directory but with a non-existent executable in the manifest
    let plugin_name = "failed_plugin";
    let plugin_dir = temp_dir.path().join(plugin_name);
    std::fs::create_dir(&plugin_dir).expect("Failed to create plugin dir");

    let plugin_manifest = r#"[plugin]
name = "failed_plugin"
version = "0.1.0"
description = "A failing plugin"
author = "Test Author"

[runtime]
type = "process"
executable = "non_existent_executable"
"#;

    std::fs::write(plugin_dir.join("plugin.toml"), plugin_manifest)
        .expect("Failed to write plugin manifest");

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );

    let mut plugin_manager =
        yedmq_plugin_host::plugin_manager::PluginManager::new(plugin_host_config)
            .await
            .unwrap();

    plugin_manager.start_listener().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // wait for listener to start

    // start_plugin will return Err because spawn fails
    let _ = plugin_manager.start_plugin(plugin_name).await;

    let running_plugins = plugin_manager.get_running_plugins();
    assert!(running_plugins.contains_key(plugin_name));
    assert_eq!(
        running_plugins.get(plugin_name).unwrap().state,
        yedmq_plugin_host::plugin_manager::PluginState::Failed
    );
}

#[tokio::test]
pub async fn test_plugin_responds_to_ping_correctly() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    common::setup_test_plugins(&temp_dir, MockConfig::default());

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let mut plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );
    // Set a short health check interval for testing
    plugin_host_config.health_check_interval_secs = 1;

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

    // Start heartbeat check
    plugin_manager.start_heartbeat_check_task().await;

    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await; // wait for a few ping cycles

    let running_plugins = plugin_manager.get_running_plugins();
    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    // Plugin should still be running
    assert_eq!(
        plugin_process.state,
        yedmq_plugin_host::plugin_manager::PluginState::Running
    );

    // Verify logs to see if ping was handled (optional, but good for confirmation)
    let logs = plugin_process.logs.read().await;
    let full_logs = logs.join("\n");
    println!("Plugin Logs:\n{}", full_logs);
    assert!(full_logs.contains("Handling ping request"));
}

#[tokio::test]
pub async fn test_plugin_state_changed_when_ping_timeout() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut mock_config = MockConfig::default();
    // Configure plugin to NOT respond to ping
    mock_config.ping.respond = false;

    common::setup_test_plugins(&temp_dir, mock_config);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let mut plugin_host_config = get_plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy().as_ref()
    );
    // Set a short health check interval for testing
    plugin_host_config.health_check_interval_secs = 1;

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

    // Start heartbeat check
    plugin_manager.start_heartbeat_check_task().await;

    // Wait for > 3 * interval.
    // Interval 1s. Timeout 3 counts. so at least 3 seconds.
    // Also ping request timeout is 5s (hardcoded in PluginManager).
    // So for each ping, it waits 5s if it times out?
    // Let's check PluginManager::start_heartbeat_check_task again.
    // send_and_wait timeout is 5s.
    // if respond = false, mock plugin might return error immediately or just hang?
    // In mock_plugin_harness:
    // if config.ping.respond == false { return Err(...) }
    // If it returns Err, `handle_request` logs error and continues loop. It does NOT send response.
    // So `plugin_host` will timeout after 5s.
    // So 1st ping: starts at 1s, timeouts at 6s. Count = 1.
    // 2nd ping: starts at 7s (approx), timeouts at 12s. Count = 2.
    // 3rd ping: starts at 13s, timeouts at 18s. Count = 3 -> Failed.
    // This is too long for a unit test (18s).

    // Optimization: Configure mock plugin to delay response slightly (e.g., 6s) so it timeouts?
    // Or just let it not respond.
    // If I want to make it faster, I need to reduce the timeout in `PluginManager` but that is hardcoded to 5s.
    //
    // However, `PluginManager::start_heartbeat_check_task` loop:
    // interval.tick().await; // first tick finishes immediately? No, "The first tick completes immediately".
    // So:
    // T=0: Tick. Send Ping 1.
    // If mock plugin returns Err internally, it doesn't send response frame.
    // Host waits 5s. T=5: Timeout. Count=1.
    // Loop continues. interval was 1s. We are already past that.
    // Next tick will happen immediately? "If the task is behind, the next tick will happen immediately".
    // T=5: Tick. Send Ping 2.
    // T=10: Timeout. Count=2.
    // T=10: Tick. Send Ping 3.
    // T=15: Timeout. Count=3. State -> Failed.
    // So expected wait time is around 15 seconds. This is acceptable for a test, but maybe I can make it faster if I mock the delay?
    //
    // Actually, if `mock_plugin` returns error to `framed.send(response)`, `plugin_host` just timeouts.
    //
    // If I want to speed this up, I would need to change the 5s timeout in `PluginManager` to be configurable or smaller.
    // But I shouldn't change too much production code just for tests if not necessary.
    // I will wait 20s to be safe.

    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;

    let running_plugins = plugin_manager.get_running_plugins();
    let plugin_process = running_plugins.get("mock_plugin_harness").unwrap();

    // Plugin should be Failed
    assert_eq!(
        plugin_process.state,
        yedmq_plugin_host::plugin_manager::PluginState::Failed
    );

    // Check logs to confirm pings were received (even if not responded to)
    let logs = plugin_process.logs.read().await;
    let full_logs = logs.join("\n");
    assert!(full_logs.contains("Not responding to ping as per configuration"));
}
