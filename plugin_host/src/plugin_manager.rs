use futures::{SinkExt, StreamExt};
use interprocess::local_socket::{
    tokio::prelude::*, tokio::Stream, GenericNamespaced, ListenerOptions, ToNsName,
};
use log::{error, info, warn};
use std::{collections::HashMap, path::Path, process::Stdio, sync::Arc, time::Duration};

use prost::Message as _;
use tokio::{
    io::{AsyncBufReadExt, BufReader}, select, sync::{oneshot, RwLock}, time::timeout
};
use tokio_util::codec::Framed;

use crate::{
    loader::PluginManifest, plugin_host_config::PluginHostConfig, protocol::{
        plugin_protocol::{
            AuthenticateRequest, AuthenticateResponse, AuthorizeRequest, AuthorizeResponse, BatchResponse, InitializeRequest, InitializeResponse, MessagePublishRequest, MessagePublishResponse, MessageType, Method, ProtocolMessage, SubscribeRequest, SubscribeResponse
        },
        ProtocolMessageBuilder,
    }
};

use super::loader::PluginLoader;
use anyhow::{Ok, Result};
use rand::Rng;

type RequestContext = oneshot::Sender<Result<ProtocolMessage>>;

pub struct AuthorizeResult {
    pub authorized: bool,
    pub reason: Option<String>,
    pub modified_context: HashMap<String, String>,
}

pub struct AuthenticateResult {
    pub authenticated: bool,
    pub error_reason: Option<String>,
    pub tenant_id: Option<String>,
}

pub struct SubscribeResult {
    pub result: Vec<SubscribeResultItem>,
}

pub struct SubscribeResultItem {
    pub topic: String,
    pub allowed: bool,
    pub granted_qos: u8,
    pub reason: Option<String>,
}

pub struct MessagePublishResult {
    pub allow: bool,
    pub modified_message: Option<crate::protocol::plugin_protocol::MqttMessage>,
    pub error_reason: Option<String>,
}

// Generate a random authentication code
fn generate_auth_code(length: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    let mut rng = rand::rng();

    let auth_code: String = (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();

    auth_code
}

#[derive(Debug)]
pub enum PluginState {
    Discovered, // Plugin has been discovered but not yet loaded
    Starting,   // Plugin is in the process of starting
    Running,    // Plugin is currently running
    Stopping,   // Plugin is in the process of stopping
    Stopped,    // Plugin has been stopped
    Failed,     // Plugin failed to start
}

pub struct RunningPlugin {
    pub name: String,
    pub manifest: PluginManifest,
    pub state: PluginState,
    pub plugin_abort_tx: Option<tokio::sync::mpsc::Sender<()>>,
    pub process_wait_handle: Option<tokio::task::JoinHandle<()>>,
    pub process_log_handle: Option<tokio::task::JoinHandle<()>>,
    pub start_time: Option<std::time::Instant>,
    pub restart_count: u32,
    pub last_health_check: Option<std::time::Instant>,
    pub ipc_sender: Option<tokio::sync::mpsc::Sender<TxCmd>>,
    pub auth_code: String,
    pub logs: Arc<RwLock<Vec<String>>>,
}

pub struct PluginManager {
    config: PluginHostConfig,
    plugin_loader: PluginLoader,
    running_plugins: Arc<RwLock<HashMap<String, RunningPlugin>>>,
    inflight: Arc<RwLock<HashMap<String, RequestContext>>>,
    hook_manager: Arc<RwLock<crate::hook::manager::HookManager>>,
    rx_cmd_sender: Option<tokio::sync::mpsc::Sender<RxCmd>>,
}

pub enum TxCmd {
    SendMessage(ProtocolMessage),
    Shutdown,
}

pub enum RxCmd {
    InitMessage {
        msg: ProtocolMessage,
        tx_cmd_sender: tokio::sync::mpsc::Sender<TxCmd>,
    },
    NormalRecievedMessage(ProtocolMessage),
    PluginStatusChanged(String, PluginState),
    Shutdown,
}


#[derive(Debug, thiserror::Error)]
pub enum PluginManagerError {

    #[error("Call hook timeout")]
    CallHookTimeout(#[from] tokio::time::error::Elapsed),

    #[error("Plugin response receive error")]
    PluginResponseReveiveError(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("Plugin execution error: {0}")]
    PluginExecutionError(#[from] anyhow::Error),

    #[error("Invalid plugin response")]
    InvalidResponse,

    #[error("No plugin registered hook: {0}")]
    NoPluginRegistered(String),

    #[error("Plugin authenticate error: {0}")]
    PluginAuthenticateTenantIdConflict(String),

}

impl PluginManager {
    pub async fn new(config: PluginHostConfig) -> Result<Self> {
        let mut loader = PluginLoader::new(&config.plugin_directory);

        let _ = loader.scan_plugins().await?;

        Ok(Self {
            config,
            plugin_loader: loader,
            running_plugins: Arc::new(RwLock::new(HashMap::new())),
            inflight: Arc::new(RwLock::new(HashMap::new())),
            hook_manager: Arc::new(RwLock::new(crate::hook::manager::HookManager::new())),
            rx_cmd_sender: None,
        })
    }

    pub fn get_plugin_manifest(&self, plugin_name: &str) -> Option<&PluginManifest> {
        self.plugin_loader.get_plugin_manifest(plugin_name)
    }

    async fn handle_plugin_connection(
        config: &PluginHostConfig,
        s: Stream,
        rx_cmd_sender: tokio::sync::mpsc::Sender<RxCmd>,
    ) -> Result<()> {
        let (tx_cmd_sender, mut tx_cmd_receiver) = tokio::sync::mpsc::channel::<TxCmd>(32);

        let mut framed = Framed::new(
            s,
            crate::protocol::protocol_frame::ProtocolFrameCodec::new(),
        );

        let initialize_request_param = InitializeRequest::new_from_plugin_host_config(config);

        let connection_join_handle = tokio::spawn(async move {
            let wrap_initialize_param_to_any = prost_types::Any {
                type_url: super::protocol::INIT_REQUEST_TYPE_URL.to_owned(),
                value: initialize_request_param.encode_to_vec(),
            };

            let init_plugin_request_message = ProtocolMessageBuilder::new()
                .with_method(Method::Initialize)
                .with_params(wrap_initialize_param_to_any)
                .build();


            framed.send(init_plugin_request_message).await.unwrap();

            println!("Send init message success");

            // ensure the plugin response init response message in 5 seconds
            match tokio::time::timeout(Duration::from_secs(5),  framed.next()).await {
                std::result::Result::Ok(Some(std::result::Result::Ok(msg))) => {
                    let protocol_message = ProtocolMessage::decode(msg.payload);
                    println!("Received init response protocol message: {:?}", protocol_message);
                    if let std::result::Result::Ok(protocol_message) = protocol_message {
                        if protocol_message.result.is_none() {
                            warn!("Plugin init response has no result, closing connection");
                            return;
                        }
                        let result = protocol_message.result.as_ref().unwrap();
                        if result.type_url != super::protocol::INIT_RESPONSE_TYPE_URL {
                            warn!("Plugin init response has invalid result type, closing connection");
                            return;
                        }
                        let _ = rx_cmd_sender.send(RxCmd::InitMessage{
                            msg:protocol_message,
                            tx_cmd_sender: tx_cmd_sender.clone(),
                        }).await;
                    } else {
                        println!("Failed to decode protocol message from plugin");
                        return;
                    }
                },

                std::result::Result::Ok(None) => {
                    println!("Plugin connection closed");
                    return;
                }
                std::result::Result::Ok(_) => {
                    println!("Plugin init response timeout, closing connection");
                    return;
                }
                std::result::Result::Err(_) => {
                    println!("Plugin init response timeout, closing connection");
                    return;
                },
            }

            loop {
                tokio::select! {
                    msg = framed.next() => {
                        if let Some(std::result::Result::Ok(msg)) = msg {
                            let protocol_message = ProtocolMessage::decode(msg.payload);
                            if let std::result::Result::Ok(protocol_message) = protocol_message {
                                println!("Received a protocol message: {:?}", protocol_message);
                                let _ = rx_cmd_sender.send(RxCmd::NormalRecievedMessage(protocol_message)).await;
                            } else {
                                warn!("Failed to decode protocol message from plugin");
                                break;
                            }
                        } else if let Some(std::result::Result::Err(e)) = msg {
                            warn!("Failed to read message from plugin: {}", e);
                            break;
                        } else {
                            warn!("Plugin connection closed");
                            break;
                        }
                    },
                    tx_cmd = tx_cmd_receiver.recv() => {
                        match tx_cmd {
                            Some(TxCmd::SendMessage(msg)) => {
                                if let Err(e) = framed.send(msg).await {
                                    warn!("Failed to send message to plugin: {}", e);
                                    break;
                                }
                            },
                            Some(TxCmd::Shutdown) | None => {
                                break;
                            }
                        }
                    }
                }
            }
        });
        Ok(())
    }

    async fn start_handle_plugin_rx_cmd(
        &self,
        mut rx_cmd_receiver: tokio::sync::mpsc::Receiver<RxCmd>,
    ) -> Result<()> {
        let plugins = self.running_plugins.clone();
        let inflight = self.inflight.clone();
        let hook_manager = self.hook_manager.clone();

        let rx_cmd_join_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = rx_cmd_receiver.recv() => {
                        match msg {
                            Some(RxCmd::PluginStatusChanged(name, state)) => {
                                let mut plugins_guard = plugins.write().await;
                                if let Some(plugin) = plugins_guard.get_mut(&name) {
                                    println!("Plugin {} status changed to {:?}", name, state);
                                    plugin.state = state;
                                }
                            },
                            Some(RxCmd::InitMessage{msg, tx_cmd_sender}) => {
                                // Handle initialization message
                                info!("Received initialization message: {:?}", msg);
                                if let Some(r) = msg.result {
                                    let init_response = InitializeResponse::decode(r.value.as_slice()).unwrap();
                                    let auth_code = init_response.auth_code;
                                    let register_hooks = init_response.hooks;
                                    let mut plugins_guard = plugins.write().await;
                                    for (_, plugin) in plugins_guard.iter_mut() {
                                        // find the plugin with matching auth_code
                                        if plugin.auth_code == auth_code {
                                            plugin.ipc_sender = Some(tx_cmd_sender.clone());
                                            plugin.state = PluginState::Running;
                                            for hook in &register_hooks {
                                                let name = &hook.name;
                                                let hook_type = crate::hook::get_hook_from_name(name);
                                                if let Some(hook_type) = hook_type {
                                                    hook_manager.write().await.register_hook(hook_type, plugin.name.clone(), hook.priority);
                                                } else {
                                                    warn!("Unknown hook name '{}' from plugin '{}'", name, plugin.name);
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            Some(RxCmd::NormalRecievedMessage(msg)) => {
                                // Handle normal received message
                                info!("Received normal message: {:?}", msg);
                                match msg.r#type() {
                                    MessageType::Unspecified => {
                                        warn!("Received message with unspecified type");
                                    },
                                    MessageType::Request => {
                                        warn!("Received message with request type, not supported yet.");
                                    },
                                    MessageType::Response => {
                                        let msg_id = &msg.id;
                                        {
                                            let mut inflight_guard = inflight.write().await;
                                            if let Some(resp_sender) = inflight_guard.remove(msg_id) {
                                                if let Err(_) = resp_sender.send(Ok(msg)) {
                                                    warn!("Failed to send response : receiver has droped");
                                                }
                                            }
                                        }
                                    },
                                    MessageType::Notification => {
                                        warn!("Received message with notification type, not supported yet.");
                                    },
                                    MessageType::Event =>  {
                                        warn!("Received message with event type, not supported yet.");
                                    },
                                    MessageType::Error => {
                                        warn!("Received message with error type, not supported yet.");
                                    },
                                    MessageType::BatchRequest => {
                                        warn!("Received message with batch request type, not supported yet.");
                                    },
                                    MessageType::BatchResponse => {
                                        warn!("Received message with batch response type, not supported yet.");
                                        if let Some(result) = msg.result {
                                            if result.type_url == crate::protocol::BATCH_RESPONSE_TYPE_URL {
                                                let batch_response = BatchResponse::decode(result.value.as_slice()).unwrap();
                                                for response in batch_response.responses {
                                                }
                                            }
                                        }
                                    },
                                }
                            },
                            Some(RxCmd::Shutdown) | None => {
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn start_listener(&mut self) -> Result<()> {

        let socket_path = self.config.local_socket_path.clone();

        let path = Path::new(&socket_path);

        if path.exists() {
            std::fs::remove_file(path)?; // remove the existing socket file
        }

        let name = socket_path.to_fs_name::<interprocess::local_socket::GenericFilePath>().unwrap();

        let (rx_cmd_sender, rx_cmd_receiver) = tokio::sync::mpsc::channel::<RxCmd>(32);

        self.rx_cmd_sender = Some(rx_cmd_sender.clone());

        let _ = self.start_handle_plugin_rx_cmd(rx_cmd_receiver).await;

        let opts = ListenerOptions::new().name(name);

        let listener = match opts.create_tokio() {
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                return Err(anyhow::anyhow!(
                    "Socket address already in use: {}",
                    self.config.local_socket_path
                ));
            }
            listener => listener?,
        };

        let config = self.config.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    std::result::Result::Ok(c) => {
                        println!("Plugin connected, start handling connection");
                        let _ = Self::handle_plugin_connection(&config, c, rx_cmd_sender.clone())
                            .await;
                    }
                    Err(_) => continue,
                };
            }
        });

        Ok(())
    }

    pub fn get_running_plugins(&self) -> Arc<RwLock<HashMap<String, RunningPlugin>>> {
        self.running_plugins.clone()
    }

    pub async fn call_subscribe_removed_hook(
        &self,
        subscribe_request: SubscribeRequest
    ) {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::SubscribeRemoved);
        let running_plugins = self.running_plugins.read().await;

        if let Some(plugins) = plugins {
            for plugin in plugins {
                let running_plugin = running_plugins.get(&plugin.plugin_name);
                if let Some(running_plugin) = running_plugin {
                    if matches!(running_plugin.state, PluginState::Running) {
                        let subscribe_request_any_wrapper = prost_types::Any {
                            type_url: crate::protocol::SUBSCRIBE_REQUEST_TYPE_URL.to_string(),
                            value: subscribe_request.encode_to_vec(),
                        };

                        let protocol_message = ProtocolMessageBuilder::new()
                            .with_method(Method::SubscriptionAdded)
                            .with_type(MessageType::Event)
                            .with_params(subscribe_request_any_wrapper)
                            .build();

                        let _ = running_plugin.ipc_sender.as_ref().unwrap().send(TxCmd::SendMessage(protocol_message)).await;
                    }
                }
            }
        }
    }

    pub async fn call_subscribe_added_hook(
        &self,
        subscribe_request: SubscribeRequest
    ) {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::SubscribeAdded);
        let running_plugins = self.running_plugins.read().await;

        if let Some(plugins) = plugins {
            for plugin in plugins {
                let running_plugin = running_plugins.get(&plugin.plugin_name);
                if let Some(running_plugin) = running_plugin {
                    if matches!(running_plugin.state, PluginState::Running) {
                        let subscribe_request_any_wrapper = prost_types::Any {
                            type_url: crate::protocol::SUBSCRIBE_REQUEST_TYPE_URL.to_string(),
                            value: subscribe_request.encode_to_vec(),
                        };

                        let protocol_message = ProtocolMessageBuilder::new()
                            .with_method(Method::SubscriptionAdded)
                            .with_type(MessageType::Event)
                            .with_params(subscribe_request_any_wrapper)
                            .build();

                        let _ = running_plugin.ipc_sender.as_ref().unwrap().send(TxCmd::SendMessage(protocol_message)).await;
                    }
                }
            }
        }
    }

    pub async fn call_message_published_hook(
        &self,
        message_publish_request: MessagePublishRequest
    ) {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::MessagePublished);
        let running_plugins = self.running_plugins.read().await;

        if let Some(plugins) = plugins {
            for plugin in plugins {
                let running_plugin = running_plugins.get(&plugin.plugin_name);
                if let Some(running_plugin) = running_plugin {
                    if matches!(running_plugin.state, PluginState::Running) {
                        let message_publish_request_any_wrapper = prost_types::Any {
                            type_url: crate::protocol::MQTT_MESSAGE_PUBLISH_REQUEST_TYPE_URL.to_string(),
                            value: message_publish_request.encode_to_vec(),
                        };

                        let protocol_message = ProtocolMessageBuilder::new()
                            .with_method(Method::OnMessagePublish)
                            .with_type(MessageType::Event)
                            .with_params(message_publish_request_any_wrapper)
                            .build();

                        let _ = running_plugin.ipc_sender.as_ref().unwrap().send(TxCmd::SendMessage(protocol_message)).await;
                    }
                }
            }
        }
    }

    // Called when message publish before, it can be used to modify the message.
    pub async fn call_on_message_publish(
        &self,
        message_publish_request: MessagePublishRequest
    ) -> Result<MessagePublishResult, PluginManagerError> {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::OnMessagePublish);
        let running_plugins = self.running_plugins.read().await;

        if let Some(plugins) = plugins {
            for plugin in plugins {
                let running_plugin = running_plugins.get(&plugin.plugin_name);
                if let Some(running_plugin) = running_plugin {
                    if matches!(running_plugin.state, PluginState::Running) {
                        let message_publish_request_any_wrapper = prost_types::Any {
                            type_url: crate::protocol::MQTT_MESSAGE_PUBLISH_REQUEST_TYPE_URL.to_string(),
                            value: message_publish_request.encode_to_vec(),
                        };

                        let protocol_message = ProtocolMessageBuilder::new()
                            .with_method(Method::OnMessagePublish)
                            .with_type(MessageType::Request)
                            .with_params(message_publish_request_any_wrapper)
                            .build();

                        let (request_context_sender, request_context_receiver) = oneshot::channel();
                        self.inflight
                            .write()
                            .await
                            .insert(protocol_message.id.clone(), request_context_sender);
                        if let Err(e) = running_plugin
                            .ipc_sender
                            .as_ref()
                            .unwrap()
                            .send(TxCmd::SendMessage(protocol_message))
                            .await {
                                error!("Plugin {} message receiver droped: {}", running_plugin.name, e);
                                continue;
                        }
                        let timeout_duration = Duration::from_secs(5);
                        let result = timeout(timeout_duration, request_context_receiver).await;

                        match result {
                            std::result::Result::Ok(std::result::Result::Ok(std::result::Result::Ok(response))) => {
                                if let Some(result) = response.result {
                                    if result.type_url == crate::protocol::MQTT_MESSAGE_PUBLISH_RESPONSE_TYPE_URL {
                                        let message_publish_response = MessagePublishResponse::decode(result.value.as_slice()).unwrap();
                                        if !message_publish_response.continue_chain() {
                                            let message_publish_result = MessagePublishResult {
                                                allow: message_publish_response.allow,
                                                modified_message: message_publish_response.modified_message,
                                                error_reason: message_publish_response.error_reason,
                                            };
                                            return std::result::Result::Ok(message_publish_result);
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                warn!("Plugin {} response timeout", running_plugin.name);
                                continue;
                            }
                            std::result::Result::Ok(Err(_))  => {
                                warn!("Plugin {} receiver droped", running_plugin.name);
                                continue;
                            }
                            std::result::Result::Ok(std::result::Result::Ok(Err(e))) => {
                                warn!("Plugin {} invalid response: {}", running_plugin.name, e);
                                continue;
                            }
                        }
                    }
                }
            }
        }
        let default_result = MessagePublishResult {
            allow: true,
            modified_message: message_publish_request.message,
            error_reason: None
        };
        std::result::Result::Ok(default_result)
    }

    // Called when a client subscribe to a topic, if no plugin return the default result that all topics are allowed.
    pub async fn call_on_message_subscribe(
        &self,
        subscribe_request: SubscribeRequest
    ) -> Result<SubscribeResult, PluginManagerError> {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::OnMessageSubscribe);
        let running_plugins = self.running_plugins.read().await;

        let mut final_result = SubscribeResult {
            result: subscribe_request.subscriptions.iter().map(|topic_filter| {
                SubscribeResultItem {
                    topic: topic_filter.topic.clone(),
                    allowed: true,
                    granted_qos: topic_filter.qos as u8,
                    reason: None,
                }
            }).collect(),
        };

        if let Some(plugins) = plugins {
            for plugin in plugins {
                let running_plugin = running_plugins.get(&plugin.plugin_name);
                if let Some(running_plugin) = running_plugin {
                    if matches!(running_plugin.state, PluginState::Running) {
                        let subscribe_request_any_wrapper = prost_types::Any {
                            type_url: crate::protocol::SUBSCRIBE_REQUEST_TYPE_URL.to_string(),
                            value: subscribe_request.encode_to_vec(),
                        };

                        let protocol_message = ProtocolMessageBuilder::new()
                            .with_method(Method::OnMessageSubscribe)
                            .with_type(MessageType::Request)
                            .with_params(subscribe_request_any_wrapper)
                            .build();

                        let (request_context_sender, request_context_receiver) = oneshot::channel();
                        self.inflight
                            .write()
                            .await
                            .insert(protocol_message.id.clone(), request_context_sender);
                        if let Err(e) = running_plugin
                            .ipc_sender
                            .as_ref()
                            .unwrap()
                            .send(TxCmd::SendMessage(protocol_message))
                            .await {
                                error!("Plugin {} message receiver droped: {}", running_plugin.name, e);
                                continue;
                        }
                        let timeout_duration = Duration::from_secs(5);
                        let result = timeout(timeout_duration, request_context_receiver).await;

                        match result {
                            std::result::Result::Ok(std::result::Result::Ok(std::result::Result::Ok(response))) => {
                                if let Some(result) = response.result {
                                    if result.type_url == crate::protocol::SUBSCRIBE_RESPONSE_TYPE_URL.to_string() {
                                        let subscribe_response: SubscribeResponse = SubscribeResponse::decode(result.value.as_slice()).unwrap();
                                        for subscribe_result_item in subscribe_response.results.iter() {
                                            final_result.result.iter_mut().find(|item| item.topic == subscribe_result_item.topic).unwrap().allowed = subscribe_result_item.allowed;
                                            final_result.result.iter_mut().find(|item| item.topic == subscribe_result_item.topic).unwrap().granted_qos = subscribe_result_item.granted_qos as u8;
                                            final_result.result.iter_mut().find(|item| item.topic == subscribe_result_item.topic).unwrap().reason = subscribe_result_item.reason.clone();
                                        }
                                        if !subscribe_response.continue_chain() {
                                            return std::result::Result::Ok(final_result);
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                warn!("Plugin {} response timeout", running_plugin.name);
                                continue;
                            }
                            std::result::Result::Ok(Err(_))  => {
                                warn!("Plugin {} receiver droped", running_plugin.name);
                                continue;
                            }
                            std::result::Result::Ok(std::result::Result::Ok(Err(e))) => {
                                warn!("Plugin {} invalid response: {}", running_plugin.name, e);
                                continue;
                            }
                        }
                    }
                }
            }
        }

        std::result::Result::Ok(final_result)
    }

    // Authorize connect, subscribe, publish
    pub async fn call_authorize_hook(
        &self,
        authorize_request: AuthorizeRequest
    ) -> std::result::Result<AuthorizeResult, PluginManagerError>{
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::Authorize);
        let running_plugins = self.running_plugins.read().await;

        let mut final_result = AuthorizeResult {
            authorized: true,
            reason: None,
            modified_context: HashMap::new(),
        };

        if let Some(plugins) = plugins {
            for plugin in plugins {
                let running_plugin = running_plugins.get(&plugin.plugin_name);
                if let Some(running_plugin) = running_plugin {
                    if matches!(running_plugin.state, PluginState::Running) {
                        let authorize_request_any_wrapper = prost_types::Any {
                            type_url: crate::protocol::AUTHORIZE_REQUEST_TYPE_URL.to_string(),
                            value: authorize_request.encode_to_vec(),
                        };

                        let protocol_message = ProtocolMessageBuilder::new()
                            .with_method(Method::Authorize)
                            .with_params(authorize_request_any_wrapper)
                            .build();

                        let (request_context_sender, request_context_receiver) = oneshot::channel();
                        self.inflight
                            .write()
                            .await
                            .insert(protocol_message.id.clone(), request_context_sender);
                        if let Err(e) = running_plugin
                            .ipc_sender
                            .as_ref()
                            .unwrap()
                            .send(TxCmd::SendMessage(protocol_message))
                            .await {
                                error!("Plugin {} message receiver droped: {}", running_plugin.name, e);
                                continue;
                        }
                        let timeout_duration = Duration::from_secs(5);
                        let result = timeout(timeout_duration, request_context_receiver).await;

                        match result {
                            std::result::Result::Ok(std::result::Result::Ok(std::result::Result::Ok(response))) => {
                                if let Some(result) = response.result {
                                    if result.type_url == crate::protocol::AUTHORIZE_RESPONSE_TYPE_URL.to_string() {
                                        let authorize_response = AuthorizeResponse::decode(result.value.as_slice()).unwrap();
                                        if authorize_response.continue_chain {
                                            continue;
                                        } else {
                                            final_result.authorized = authorize_response.authorized;
                                            final_result.reason = authorize_response.reason.clone();
                                            final_result.modified_context = HashMap::new();
                                            return std::result::Result::Ok(final_result);
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                warn!("Plugin {} response timeout", running_plugin.name);
                                continue;
                            }
                            std::result::Result::Ok(Err(_))  => {
                                warn!("Plugin {} receiver droped", running_plugin.name);
                                continue;
                            }
                            std::result::Result::Ok(std::result::Result::Ok(Err(e))) => {
                                warn!("Plugin {} invalid response: {}", running_plugin.name, e);
                                continue;
                            }

                        }
                    }
                }
            }
            return std::result::Result::Ok(final_result);
        } else {
            info!("No plugins registered for OnAuthenticate hook");
            return std::result::Result::Err(PluginManagerError::NoPluginRegistered("Authenticate".to_string()));
        }
    }

    // Call the OnAuthenticate hook for all registered plugins(for login authentication)
    pub async fn call_authenticate_hook(
        &self,
        authenticate_request: AuthenticateRequest,
    ) -> std::result::Result<AuthenticateResult, PluginManagerError> {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::Authenticate);
        let running_plugins = self.running_plugins.read().await;
        if let Some(plugins) = plugins {
            for plugin in plugins {
                let running_plugin = running_plugins.get(&plugin.plugin_name);
                if let Some(running_plugin) = running_plugin {
                    if matches!(running_plugin.state, PluginState::Running) {
                        let authenticate_request_any_wrapper = prost_types::Any {
                            type_url: crate::protocol::AUTHENTICATE_REQUEST_TYPE_URL.to_string(),
                            value: authenticate_request.encode_to_vec(),
                        };
                        let protocol_message = ProtocolMessageBuilder::new()
                            .with_method(Method::Authenticate)
                            .with_params(authenticate_request_any_wrapper)
                            .build();
                        let (request_context_sender, request_context_receiver) = oneshot::channel();
                        self.inflight
                            .write()
                            .await
                            .insert(protocol_message.id.clone(), request_context_sender);
                        if let Err(e) = running_plugin
                            .ipc_sender
                            .as_ref()
                            .unwrap()
                            .send(TxCmd::SendMessage(protocol_message))
                            .await {
                                error!("Plugin {} message receiver droped: {}", running_plugin.name, e);
                                continue;
                        }
                        let timeout_duration = Duration::from_secs(5);
                        let result = timeout(timeout_duration, request_context_receiver).await;
                        match result {
                            std::result::Result::Ok(std::result::Result::Ok(std::result::Result::Ok(response))) => {
                                if let Some(result) = response.result {
                                    if result.type_url == crate::protocol::AUTHENTICATE_RESPONSE_TYPE_URL {
                                        let authenticate_response =
                                            AuthenticateResponse::decode(result.value.as_slice()).unwrap();
                                        if authenticate_response.continue_chain {
                                            continue;
                                        } else {
                                            return std::result::Result::Ok(AuthenticateResult{
                                                authenticated: authenticate_response.authenticated,
                                                error_reason: authenticate_response.error_reason,
                                                tenant_id: authenticate_response.tenant_id
                                            });
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                warn!("Plugin {} response timeout", running_plugin.name);
                                continue;
                            }
                            std::result::Result::Ok(Err(_))  => {
                                warn!("Plugin {} receiver droped", running_plugin.name);
                                continue;
                            }
                            std::result::Result::Ok(std::result::Result::Ok(Err(e))) => {
                                warn!("Plugin {} invalid response: {}", running_plugin.name, e);
                                continue;
                            }
                        }
                    }
                }
            }
            return std::result::Result::Ok(AuthenticateResult {
                authenticated: false,
                error_reason: Some("No plugin authenticated successfully, reuten default value.".to_string()),
                tenant_id: None,
            })
        } else {
            info!("No plugins registered for OnAuthenticate hook");
            return std::result::Result::Err(PluginManagerError::NoPluginRegistered("Authenticate".to_string()));
        }
    }

    pub async fn start_plugin(&self, name: &str) -> Result<()> {
        let manifest = self
            .plugin_loader
            .get_plugin_manifest(name)
            .ok_or_else(|| anyhow::anyhow!("Plugin '{}' not found", name))?
            .clone();

        let auth_code = generate_auth_code(12);

        let mut command = self
            .plugin_loader
            .get_plugin_command(name, &auth_code, &self.config.local_socket_path)?
            .unwrap();

        let mut process = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;


        let stdout = process.stderr.take().expect("plugin did not have a handle to stdout");

        let mut stdout_reader = BufReader::new(stdout).lines();

        let borrowed_name = name.to_string();

        let (plugin_abort_tx,mut plugin_abort_rx) = tokio::sync::mpsc::channel::<()>(1);

        let rx_cmd_sender = self.rx_cmd_sender.as_ref().unwrap().clone();

        let process_wait_handle = tokio::spawn(async move {
            select! {
                status_result = process.wait() => {
                    match status_result {
                        std::result::Result::Ok(s) => {
                            println!("Plugin '{}' exited with status: {}", borrowed_name, s);
                        }
                        Err(e) => {
                            println!("Plugin '{}' encountered an error while waiting: {}", borrowed_name, e);
                        }
                    }
                    let _ = rx_cmd_sender.send(RxCmd::PluginStatusChanged(borrowed_name, PluginState::Stopped)).await;

                },
                _ = plugin_abort_rx.recv() => {
                    println!("Plugin '{}' aborted", borrowed_name);
                    if let Err(e) = process.kill().await {
                        println!("Failed to kill plugin '{}': {}", borrowed_name, e);
                    } else {
                        println!("Plugin '{}' killed successfully", borrowed_name);
                    }
                }
            }
        });

        
        let logs = Arc::new(RwLock::new(Vec::new()));

        let logs_clone = logs.clone();

        let borrowed_name = name.to_string();

        let plugin_abort_tx_clone = plugin_abort_tx.clone();

        let process_log_handle =tokio::spawn(async move {
            // Capture plugin stdout into logs
            if let Err(e) = async move {
                while let Some(line) = stdout_reader.next_line().await? {
                    let mut logs_guard = logs_clone.write().await;
                    logs_guard.push(line);
                    if logs_guard.len() > 1000 {
                        logs_guard.remove(0); // keep only the last 1000 lines
                    }
                }
                Ok(())
            }.await {
                println!("Error reading stdout of plugin '{}': {}", borrowed_name, e);
                let _ = plugin_abort_tx_clone.send(()).await;
            }
            //
        });

        let running_plugin = RunningPlugin {
            name: name.to_string(),
            manifest,
            state: PluginState::Starting,
            plugin_abort_tx: Some(plugin_abort_tx),
            start_time: Some(std::time::Instant::now()),
            process_log_handle: Some(process_log_handle),
            process_wait_handle: Some(process_wait_handle),
            restart_count: 0,
            last_health_check: None,
            ipc_sender: None,
            auth_code,
            logs,
        };

        {
            let mut plugins = self.running_plugins.write().await;
            plugins.insert(name.to_string(), running_plugin);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

}
