use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use tempfile::TempDir;
use yedmq_plugin_host::{
    plugin_host_config::PluginHostConfig,
    plugin_manager::{PluginManager, PluginState},
    protocol::plugin_protocol::{AuthAction, AuthenticateRequest, AuthorizeRequest},
};

fn plugin_host_test_config(
    sender: tokio::sync::broadcast::Sender<()>,
    temp_dir: &TempDir,
    local_socket_path: &str,
) -> PluginHostConfig {
    PluginHostConfig {
        plugin_directory: temp_dir.path().to_string_lossy().to_string(),
        broker_version: "0.1.0".to_string(),
        broker_node_id: 1,
        cluster_name: "test_cluster".to_string(),
        max_restart_attempts: 3,
        health_check_interval_secs: 10,
        init_timeout_secs: 2,
        request_timeout_secs: 2,
        ping_timeout_secs: 2,
        shutdown_signal: sender,
        local_socket_path: local_socket_path.to_string(),
        default_authenticate_result: true,
        default_authorize_result: true,
    }
}

fn write_plugin_fixture(root: &Path, acl_json: &str) {
    let plugin_dir = root.join("acl_file");
    fs::create_dir_all(&plugin_dir).expect("failed to create plugin dir");

    let binary_path = PathBuf::from(env!("CARGO_BIN_EXE_acl_file"));
    let executable_name = format!("acl_file{}", std::env::consts::EXE_SUFFIX);
    let target_binary = plugin_dir.join(&executable_name);
    fs::copy(binary_path, &target_binary).expect("failed to copy plugin binary");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(&target_binary)
            .expect("failed to read binary metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&target_binary, permissions).expect("failed to set binary permissions");
    }

    fs::write(plugin_dir.join("acl.json"), acl_json).expect("failed to write acl file");
    let manifest = format!(
        r#"[plugin]
name = "acl_file"
version = "0.1.0"
description = "ACL file test plugin"
author = "Test Author"

[runtime]
type = "process"
executable = "{executable_name}"
args = ["--acl-file", "./acl.json"]
env = {{}}
working_dir = "."
timeout_secs = 12
"#,
    );
    fs::write(plugin_dir.join("plugin.toml"), manifest).expect("failed to write plugin manifest");
}

async fn wait_until_running(plugin_manager: &PluginManager, plugin_name: &str) {
    for _ in 0..30 {
        if let Some(plugin) = plugin_manager.get_running_plugins().get(plugin_name) {
            if plugin.state == PluginState::Running {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    panic!("plugin {plugin_name} did not reach running state");
}

#[tokio::test]
async fn acl_file_plugin_authenticates_and_authorizes_from_acl() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    write_plugin_fixture(
        temp_dir.path(),
        r#"{
  "authenticate": {
    "rules": [
      {
        "action": "allow",
        "tenant": "public",
        "username": ["super"],
        "password": ["super123"],
        "client_ip": ["%"]
      },
      {
        "action": "deny",
        "username": ["blocked"],
        "password": ["%"],
        "client_ip": ["%"]
      }
    ]
  },
  "authorize": {
    "rules": [
      {
        "action": "allow",
        "tenant": ["public"],
        "username": ["super"],
        "subscribe": ["%"],
        "publish": ["%"]
      },
      {
        "action": "deny",
        "tenant": ["public"],
        "username": ["blocked"],
        "subscribe": ["%"],
        "publish": ["%"]
      }
    ]
  }
}"#,
    );

    let (tx, _) = tokio::sync::broadcast::channel(1);
    let mut plugin_manager = PluginManager::new(plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy()
            .as_ref(),
    ))
    .await
    .expect("failed to create plugin manager");

    plugin_manager
        .start_listener()
        .await
        .expect("failed to start listener");
    tokio::time::sleep(Duration::from_millis(300)).await;

    plugin_manager
        .start_plugin("acl_file")
        .await
        .expect("failed to start plugin");
    wait_until_running(&plugin_manager, "acl_file").await;

    let allow_auth = plugin_manager
        .call_authenticate_hook(AuthenticateRequest {
            client_id: "client-1".to_string(),
            username: "super".to_string(),
            password: "super123".to_string(),
            client_ip: "127.0.0.1".to_string(),
            client_cert: Vec::new(),
            protocol_version: "3.1.1".to_string(),
            properties: None,
        })
        .await
        .expect("authenticate should succeed");
    assert!(allow_auth.authenticated);
    assert_eq!(allow_auth.tenant_id, Some("public".to_string()));

    let deny_auth = plugin_manager
        .call_authenticate_hook(AuthenticateRequest {
            client_id: "client-2".to_string(),
            username: "blocked".to_string(),
            password: "whatever".to_string(),
            client_ip: "127.0.0.1".to_string(),
            client_cert: Vec::new(),
            protocol_version: "3.1.1".to_string(),
            properties: None,
        })
        .await
        .expect("authenticate should return response");
    assert!(!deny_auth.authenticated);

    let allow_authorize = plugin_manager
        .call_authorize_hook(AuthorizeRequest {
            tenant_id: "public".to_string(),
            client_id: "client-1".to_string(),
            username: "super".to_string(),
            action: AuthAction::Publish.into(),
            topic: "/topic/1".to_string(),
            qos: 0,
            context: None,
        })
        .await
        .expect("authorize should succeed");
    assert!(allow_authorize.authorized);

    let deny_authorize = plugin_manager
        .call_authorize_hook(AuthorizeRequest {
            tenant_id: "public".to_string(),
            client_id: "client-2".to_string(),
            username: "blocked".to_string(),
            action: AuthAction::Subscribe.into(),
            topic: "/topic/1".to_string(),
            qos: 0,
            context: None,
        })
        .await
        .expect("authorize should return response");
    assert!(!deny_authorize.authorized);

    plugin_manager
        .stop_plugin("acl_file")
        .await
        .expect("failed to stop plugin");
}

#[tokio::test]
async fn acl_file_plugin_skips_missing_authorize_hook_and_falls_back_to_default() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    write_plugin_fixture(
        temp_dir.path(),
        r#"{
  "authenticate": {
    "rules": [
      {
        "action": "allow",
        "username": ["super"],
        "password": ["super123"],
        "client_ip": ["%"]
      }
    ]
  }
}"#,
    );

    let (tx, _) = tokio::sync::broadcast::channel(1);
    let mut plugin_manager = PluginManager::new(plugin_host_test_config(
        tx,
        &temp_dir,
        temp_dir
            .path()
            .join("yedmq_plugin.sock")
            .to_string_lossy()
            .as_ref(),
    ))
    .await
    .expect("failed to create plugin manager");

    plugin_manager
        .start_listener()
        .await
        .expect("failed to start listener");
    tokio::time::sleep(Duration::from_millis(300)).await;

    plugin_manager
        .start_plugin("acl_file")
        .await
        .expect("failed to start plugin");
    wait_until_running(&plugin_manager, "acl_file").await;

    let authorize_result = plugin_manager
        .call_authorize_hook(AuthorizeRequest {
            tenant_id: "public".to_string(),
            client_id: "client-1".to_string(),
            username: "someone".to_string(),
            action: AuthAction::Publish.into(),
            topic: "/topic/1".to_string(),
            qos: 0,
            context: None,
        })
        .await
        .expect("authorize should return broker default");

    assert!(authorize_result.authorized);

    plugin_manager
        .stop_plugin("acl_file")
        .await
        .expect("failed to stop plugin");
}
