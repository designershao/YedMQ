use clap::Parser;
use interprocess::local_socket::{
    tokio::{prelude::*, Stream}, GenericFilePath, GenericNamespaced, ToNsName
};
use log::{error, info};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use tokio_util::codec::Framed;
use std::{any, collections::HashMap, time::Duration};
use tokio::time::timeout;
use yedmq_plugin_host::protocol::{
    plugin_protocol::{
        AuthenticateResponse, Hook, InitializeResponse, MessageType, Method, ProtocolMessage,
    },
    protocol_frame::ProtocolFrameCodec,
};
use futures::{SinkExt, StreamExt};

#[derive(Serialize, Deserialize, Debug)]
struct MockConfig {
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
        continue_chain: false,
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

    /// whether to continue the chain
    pub continue_chain: bool,
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
    SkipResponse,
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

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The auth code from the host
    #[arg(long)]
    auth_code: String,

    #[arg(long)]
    socket_path: String,

    /// The configuration for the mock plugin in JSON format
    #[arg(long, default_value = "{}")]
    config: String,

    /// The socket name to connect to the host
    #[arg(long, default_value = "yedmq_plugin.sock")]
    socket_name: String,
}

async fn handle_initialize_request(
    request: &ProtocolMessage,
    config: &MockConfig,
    real_auth_code: &str,
) -> Result<ProtocolMessage, anyhow::Error> {
    info!("Handling initialize request");
    info!("Initialization fail mode: {:?}", config.initialize.initialization_fail_mode);
    match config.initialize.initialization_fail_mode {
        InitFailMode::None => {}
        InitFailMode::Delay(ms) => {
            info!("Delaying initialization response by {} ms", ms);
            tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
        }
        InitFailMode::Crash => {
            error!("Crashing as per configuration");
            std::process::exit(1);
        }
        InitFailMode::SkipResponse => {
            info!("Skipping initialization response as per configuration");
            return Err(anyhow::anyhow!("Skipping initialization response as per configuration")); 
        }
    }
    let hooks: Vec<Hook> = config
        .initialize
        .hooks
        .iter()
        .map(|h| Hook {
            name: h.name.clone(),
            priority: h.priority as u32,
            filter: None,
        })
        .collect();

    let response_auth_code = match &config.initialize.initialization_auth_code_mode {
        AuthCodeMode::Correct => real_auth_code.to_string(),
        AuthCodeMode::Incorrect => format!("{}-wrong", real_auth_code),
        AuthCodeMode::Custom(code) => code.to_string(),
    };

    let response = InitializeResponse {
        hooks,
        status: config.initialize.status.clone(),
        capabilities: vec![],
        plugin_info: None,
        auth_code: response_auth_code,
    };

    Ok(ProtocolMessage {
        version: request.version.clone(),
        r#type: MessageType::Response as i32,
        id: request.id.clone(),
        timestamp: Some(yedmq_plugin_host::create_timestamp()),
        source: "mock_plugin".to_string(),
        target: "plugin_host".to_string(),
        method: None,
        params: None,
        result: Some(prost_types::Any {
            type_url: yedmq_plugin_host::protocol::INIT_RESPONSE_TYPE_URL.to_string(),
            value: response.encode_to_vec(),
        }),
        error: None,
        metadata: HashMap::new(),
    })
}

async fn handle_authenticate_request(
    request: &ProtocolMessage,
    config: &MockConfig,
) -> Result<ProtocolMessage, anyhow::Error> {
    info!("Handling authenticate request");
    let response = AuthenticateResponse {
        authenticated: config.authenticate.authenticated,
        error_reason: config.authenticate.error_reason.clone(),
        tenant_id: config.authenticate.tenant_id.clone(),
        permissions: vec![],
        session_data: Default::default(),
    };
    Ok(ProtocolMessage {
        version: request.version.clone(),
        r#type: MessageType::Response as i32,
        id: request.id.clone(),
        timestamp: Some(yedmq_plugin_host::create_timestamp()),
        source: "mock_plugin".to_string(),
        target: "plugin_host".to_string(),
        method: None,
        params: None,
        result: Some(
            prost_types::Any {
                type_url: yedmq_plugin_host::protocol::AUTHENTICATE_RESPONSE_TYPE_URL.to_string(),
                value: response.encode_to_vec(),
            }
        ),
        error: None,
        metadata: HashMap::new(),
    })
}

async fn handle_authorize_request(
    request: &ProtocolMessage,
    config: &MockConfig,
) -> Result<ProtocolMessage, anyhow::Error> {
    // For simplicity, we just allow all authorization requests in this mock
    let response = yedmq_plugin_host::protocol::plugin_protocol::AuthorizeResponse {
        authorized: config.authorize.authorized,
        reason: config.authorize.reason.clone(),
        modified_context: None,
        continue_chain: false,
    };

    Ok(ProtocolMessage {
        version: request.version.clone(),
        r#type: MessageType::Response as i32,
        id: request.id.clone(),
        timestamp: Some(yedmq_plugin_host::create_timestamp()),
        source: "mock_plugin".to_string(),
        target: "plugin_host".to_string(),
        method: None,
        params: None,
        result: Some(prost_types::Any {
            type_url: yedmq_plugin_host::protocol::AUTHORIZE_RESPONSE_TYPE_URL.to_string(),
            value: response.encode_to_vec(),
        }),
        error: None,
        metadata: HashMap::new(),
    })
}

async fn handle_on_message_subscribe_request(
    request: &ProtocolMessage,
    config: &MockConfig,
) -> Result<ProtocolMessage, anyhow::Error> {
    info!("Handling on_message_subscribe request");
    let mut results = vec![];
    for (topic, allowed, qos) in &config.subscribe.results {
        results.push(yedmq_plugin_host::protocol::plugin_protocol::SubscribeResult {
            topic: topic.clone(),
            allowed: *allowed,
            granted_qos: *qos,
            reason: None,
        });
    }

    Ok(ProtocolMessage {
        version: request.version.clone(),
        r#type: MessageType::Response as i32,
        id: request.id.clone(),
        timestamp: Some(yedmq_plugin_host::create_timestamp()),
        source: "mock_plugin".to_string(),
        target: "plugin_host".to_string(),
        method: None,
        params: None,
        result: Some(prost_types::Any {
            type_url: yedmq_plugin_host::protocol::SUBSCRIBE_RESPONSE_TYPE_URL.to_string(),
            value: yedmq_plugin_host::protocol::plugin_protocol::SubscribeResponse {
                results,
                continue_chain: Some(config.subscribe.continue_chain),
            }
            .encode_to_vec(),
        }),
        error: None,
        metadata: HashMap::new(),
    })
}

async fn handle_on_message_publish_request(
    request: &ProtocolMessage,
    config: &MockConfig,
) -> Result<ProtocolMessage, anyhow::Error> {
    info!("Handling on_message_publish request");
    let incoming_mqtt_message = if let Some(params) = &request.params {
        let any_msg = params;
        if any_msg.type_url != yedmq_plugin_host::protocol::MQTT_MESSAGE_PUBLISH_REQUEST_TYPE_URL {
            return Err(anyhow::anyhow!(
                "Invalid type_url for publish request: {}",
                any_msg.type_url
            ));
        }
        yedmq_plugin_host::protocol::plugin_protocol::MessagePublishRequest::decode(&*any_msg.value)?
    } else {
        return Err(anyhow::anyhow!("Missing params in publish request"));
    };

    let mut raw_mqtt_msg = incoming_mqtt_message.message.unwrap();

    let on_message_publish_config = config.on_message_publish.clone();

    if config.on_message_publish.modified_message.is_some() {
        raw_mqtt_msg.payload = on_message_publish_config
            .modified_message
            .clone()
            .as_mut()
            .unwrap()
            .modified_message
            .as_ref()
            .unwrap().clone().into_bytes();
    }

    let response = yedmq_plugin_host::protocol::plugin_protocol::MessagePublishResponse {
        allow: config.on_message_publish.allow,
        continue_chain: Some(false),
        error_reason: config.on_message_publish.error_reason.clone(),
        modified_message: raw_mqtt_msg.into(),
    };

    Ok(ProtocolMessage {
        version: request.version.clone(),
        r#type: MessageType::Response as i32,
        id: request.id.clone(),
        timestamp: Some(yedmq_plugin_host::create_timestamp()),
        source: "mock_plugin".to_string(),
        target: "plugin_host".to_string(),
        method: None,
        params: None,
        result: Some(prost_types::Any {
            type_url: yedmq_plugin_host::protocol::MQTT_MESSAGE_PUBLISH_RESPONSE_TYPE_URL.to_string(),
            value: response.encode_to_vec(),
        }),
        error: None,
        metadata: HashMap::new(),
    })
}

async fn handle_message_published_request(
    request: &ProtocolMessage,
    config: &MockConfig,
) -> Result<ProtocolMessage, anyhow::Error> {
    info!("Handling message published request");
    return Err(anyhow::anyhow!("Not response"));
}

async fn handle_request(
    request: &ProtocolMessage,
    config: &MockConfig,
    auth_code: &str,
    framed: &mut tokio_util::codec::Framed<Stream, ProtocolFrameCodec>,
) -> Result<(), anyhow::Error> {
    let method = request.method();

    let response_msg = match method {
        Method::Initialize => handle_initialize_request(request, config, auth_code).await,
        Method::Authenticate => handle_authenticate_request(request, config).await,
        Method::Authorize => handle_authorize_request(request, config).await,
        Method::OnMessageSubscribe => handle_on_message_subscribe_request(request, config).await,
        Method::OnMessagePublish => handle_on_message_publish_request(request, config).await,
        Method::MessagePublished => handle_message_published_request(request, config).await,
        _ => Err(anyhow::anyhow!("Unknown method")),
    };


    if response_msg.is_ok() {
        info!("Sending response: {:?}", response_msg);
        framed.send(response_msg?).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::init();

    println!("Mock Plugin starting...");

    let args = Args::parse();

    let config: MockConfig = serde_json::from_str(&args.config).expect("Invalid config JSON");

    info!("Mock Plugin started with auth_code: {}", args.auth_code);
    info!("Socket path: {}", args.socket_path);
    info!("Using config: {:?}", config);
    
    let socket_name = args.socket_path.to_fs_name::<GenericFilePath>().unwrap();

    let stream =  match timeout(Duration::from_secs(5), Stream::connect(socket_name)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            error!("Failed to connect to socket: {}", e);
            return;
        }
        Err(_) => {
            error!("Timeout while trying to connect to socket");
            return;
        }
    };

    info!("Successfully connected to the host socket");

    let mut framed = Framed::new(
        stream,
        yedmq_plugin_host::protocol::protocol_frame::ProtocolFrameCodec::new(),
    );

    info!("Frame codec init success, start handling messages");

    loop {
        match framed.next().await {
            Some(Ok(msg)) => {
                info!("Received a frame: {:?}", msg);
                let protocol_msg = ProtocolMessage::decode(msg.payload.as_ref()).unwrap();

                if let Err(e) = handle_request(&protocol_msg, &config, &args.auth_code, &mut framed).await {
                    error!("Error handling request: {}", e);
                }
            }
            None => {
                info!("Connection closed by host, exiting");
                break;
            }
            Some(Err(e)) => {
                println!("Error reading from socket: {}", e);
                break;
            }
        }
    }

}
