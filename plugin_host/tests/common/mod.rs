use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            initialize: default_init_config(),
            authenticate: default_auth_config(),
            subscribe: default_subscribe_config(),
            on_message_publish: default_on_message_publish_config(),
            authorize: default_authorize_config(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MockConfig {
    #[serde(default = "default_init_config")]
    pub initialize: InitializeConfig,

    #[serde(default = "default_auth_config")]
    pub authenticate: AuthenticateConfig,

    #[serde(default = "default_subscribe_config")]
    pub subscribe: SubscribeConfig,

    #[serde(default = "default_on_message_publish_config")]
    pub on_message_publish: OnMessagePublishConfig,

    #[serde(default = "default_authorize_config")]
    pub authorize: AuthorizeConfig,
}

fn default_authorize_config() -> AuthorizeConfig {
    AuthorizeConfig {
        authorized: true,
        reason: None,
        modified_context: None,
    }
}

fn default_on_message_publish_config() -> OnMessagePublishConfig {
    OnMessagePublishConfig {
        allow: true,
        modified_message: None,
        error_reason: None,
        continue_chain: false,
    }
}

fn default_init_config() -> InitializeConfig {
    InitializeConfig {
        initialization_fail_mode: InitFailMode::None,
        status: "ready".to_string(),
        hooks: vec![],
        initialization_auth_code_mode: AuthCodeMode::Correct,
    }
}

fn default_auth_config() -> AuthenticateConfig {
    AuthenticateConfig {
        authenticated: true,
        error_reason: None,
        tenant_id: Some("default_tenant".to_string()),
    }
}

fn default_subscribe_config() -> SubscribeConfig {
    SubscribeConfig {
        default_allow: true,
        results: vec![],
        continue_chain: false,
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SubscribeConfig {
    pub default_allow: bool,
    pub results: Vec<(String, bool, u32)>, // (Topic, Allowed,  QOS)
    pub continue_chain: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct AuthorizeConfig {

    /// whether to authorize successfully or not
    pub authorized: bool,

    /// if not authorized, the reason for failure
    pub reason: Option<String>,

    /// the modified context
    pub modified_context: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct AuthenticateConfig {
    /// whether to authenticate successfully or not
    pub authenticated: bool,

    /// if not authenticated, the reason for failure
    pub error_reason: Option<String>,

    /// the tenant id
    pub tenant_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InitializeConfig {
    pub initialization_fail_mode: InitFailMode,
    pub status: String, // error or ready
    pub hooks: Vec<HookConfig>,
    pub initialization_auth_code_mode: AuthCodeMode,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InitFailMode {
    None,
    Delay(u64),
    Crash,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum AuthCodeMode {
    /// return the host correct auth_code
    Correct,

    /// return an  auth_code that is definitely wrong
    Incorrect,

    /// return a custom auth_code
    #[serde(alias = "custom")]
    Custom(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OnMessagePublishConfig {
    pub allow: bool,
    pub modified_message: Option<MqttMessage>,
    pub error_reason: Option<String>,
    pub continue_chain: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct MqttMessage {
    pub modified_message: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct HookConfig {
    pub name: String,
    pub priority: i32,
}

pub fn get_mock_plugin_path() -> PathBuf {
    let cargo_target_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target");
    cargo_target_dir.join("debug").join("mock_plugin_harness")
}

pub fn setup_test_plugins(plugins_test_dir: &TempDir, mock_config: MockConfig) {
    let mock_plugin_dir = plugins_test_dir.path().join("mock_plugin_harness");
    std::fs::create_dir(&mock_plugin_dir).expect("Failed to create mock plugin dir");

    let mock_plugin_exe = get_mock_plugin_path();
    let mock_plugin_harness_dir = plugins_test_dir.path().join("mock_plugin_harness");
    std::fs::create_dir_all(&mock_plugin_harness_dir).expect("Failed to create mock plugin harness dir");
    std::fs::copy(mock_plugin_exe, mock_plugin_harness_dir.join("mock_plugin_harness")).expect("Failed to copy mock plugin exe");

    let mock_config_json = serde_json::to_string_pretty(&mock_config);

    let mock_plugin_manifest = format!(r###"[plugin]
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
args = ["--config", {:?}]
env = {{}}
working_dir = "."
timeout_secs = 12
    "###, mock_config_json.unwrap());
    println!("Mock plugin manifest:\n{}", mock_plugin_manifest);

    std::fs::write(mock_plugin_dir.join("plugin.toml"), mock_plugin_manifest)
        .expect("Failed to write mock plugin manifest");

}
