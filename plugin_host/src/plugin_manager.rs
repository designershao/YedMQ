use core::time;
use futures::{SinkExt, StreamExt};
use interprocess::local_socket::{
    tokio::prelude::*, tokio::Stream, GenericNamespaced, ListenerOptions, ToNsName,
};
use log::{error, info, warn};
use std::{collections::HashMap, sync::Arc, time::Duration};

use prost::Message as _;
use tokio::{
    sync::{oneshot, Mutex, RwLock},
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::{
    loader::PluginManifest,
    plugin_host_config::PluginHostConfig,
    protocol::{
        plugin_protocol::{
            AuthenticateRequest, AuthenticateResponse, AuthorizeRequest, AuthorizeResponse, BrokerInfo, InitializeRequest, InitializeResponse, MessagePublishRequest, MessageType, Method, ProtocolMessage, SubscribeRequest
        },
        ProtocolMessageBuilder,
    },
};

use super::loader::PluginLoader;
use anyhow::{anyhow, Ok, Result};
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
    pub process: Option<tokio::process::Child>,
    pub start_time: Option<std::time::Instant>,
    pub restart_count: u32,
    pub last_health_check: Option<std::time::Instant>,
    pub ipc_sender: Option<tokio::sync::mpsc::Sender<TxCmd>>,
    pub auth_code: String,
}

pub struct PluginManager {
    config: PluginHostConfig,
    plugin_loader: PluginLoader,
    running_plugins: Arc<RwLock<HashMap<String, RunningPlugin>>>,
    inflight: Arc<Mutex<HashMap<String, RequestContext>>>,
    hook_manager: Arc<RwLock<crate::hook::manager::HookManager>>,
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
            inflight: Arc::new(Mutex::new(HashMap::new())),
            hook_manager: Arc::new(RwLock::new(crate::hook::manager::HookManager::new())),
        })
    }

    async fn handle_plugin_connection(
        &self,
        s: Stream,
        rx_cmd_sender: tokio::sync::mpsc::Sender<RxCmd>,
    ) -> Result<()> {
        let (tx_cmd_sender, mut tx_cmd_receiver) = tokio::sync::mpsc::channel::<TxCmd>(32);

        let mut framed = Framed::new(
            s,
            crate::protocol::protocol_frame::ProtocolFrameCodec::new(),
        );

        let initialize_request_param = InitializeRequest::new_from_plugin_host_config(&self.config);

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

            loop {
                tokio::select! {
                    msg = framed.next() => {
                        if let Some(msg) = msg {
                            if let std::result::Result::Ok(msg) = msg {
                                let protocol_message = ProtocolMessage::decode(msg.payload);
                                if let std::result::Result::Ok(protocol_message) = protocol_message {
                                    match protocol_message.method {
                                        Some(m) if m == Method::Initialize as i32 => {
                                            let _ = rx_cmd_sender.send(RxCmd::InitMessage{
                                                msg:protocol_message,
                                                tx_cmd_sender: tx_cmd_sender.clone(),
                                            }).await;
                                        },
                                        _ => {
                                            let _ = rx_cmd_sender.send(RxCmd::NormalRecievedMessage(protocol_message)).await;
                                        }
                                    }
                                } else {
                                    warn!("Failed to decode protocol message from plugin");
                                    break;
                                }
                            }
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
                                    MessageType::Unspecified => todo!(),
                                    MessageType::Request => todo!(),
                                    MessageType::Response => {
                                        let msg_id = &msg.id;
                                        {
                                            let mut inflight_guard = inflight.blocking_lock();
                                            if let Some(resp_sender) = inflight_guard.remove(msg_id) {
                                                if let Err(_) = resp_sender.send(Ok(msg)) {
                                                    warn!("Failed to send response : receiver has droped");
                                                }
                                            }
                                        }
                                    },
                                    MessageType::Notification => todo!(),
                                    MessageType::Event => todo!(),
                                    MessageType::Error => todo!(),
                                    MessageType::BatchRequest => todo!(),
                                    MessageType::BatchResponse => todo!(),
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
        let printname = "yedmq_plugin.sock";

        let name = printname.to_ns_name::<GenericNamespaced>().unwrap();

        let (rx_cmd_sender, mut rx_cmd_receiver) = tokio::sync::mpsc::channel::<RxCmd>(32);

        let _ = self.start_handle_plugin_rx_cmd(rx_cmd_receiver).await;

        let opts = ListenerOptions::new().name(name);

        let listener = match opts.create_tokio() {
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                return Err(anyhow::anyhow!(
                    "Socket address already in use: {}",
                    printname
                ));
            }
            listener => listener?,
        };

        loop {
            match listener.accept().await {
                std::result::Result::Ok(c) => {
                    let _ = self
                        .handle_plugin_connection(c, rx_cmd_sender.clone())
                        .await;
                }
                Err(_) => continue,
            };
        }
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
                            .lock()
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

                                        if !authorize_response.authorized {
                                            final_result.authorized = false;
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
            let mut final_tenant_id:Option<String> = None;
            let mut has_success = false;
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
                            .lock()
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
                                        if authenticate_response.authenticated {
                                            if let Some(tenant_id) = authenticate_response.tenant_id {
                                                match &final_tenant_id {
                                                    None => {
                                                        final_tenant_id = Some(tenant_id);
                                                    }
                                                    Some(existing_tenant_id) => {
                                                        if existing_tenant_id != &tenant_id {
                                                            let err_msg = format!("Authenticate tenant id conflict, previes is {} now is {}", existing_tenant_id, tenant_id);
                                                            return std::result::Result::Err(PluginManagerError::PluginAuthenticateTenantIdConflict(err_msg));
                                                        }
                                                    }
                                                }
                                            }
                                            has_success = true;
                                        } else {
                                            return std::result::Result::Ok(AuthenticateResult {
                                                authenticated: false,
                                                error_reason: authenticate_response.error_reason,
                                                tenant_id: None,
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
            if has_success {
                return std::result::Result::Ok(AuthenticateResult {
                    authenticated: true,
                    error_reason: None,
                    tenant_id: final_tenant_id,
                });
            } else {
                return std::result::Result::Ok(AuthenticateResult {
                    authenticated: false,
                    error_reason: Some("No plugin authenticated successfully".to_string()),
                    tenant_id: None,
                })
            }
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
            .get_plugin_command(name, &auth_code)?
            .unwrap();

        let process = command.spawn()?;

        let running_plugin = RunningPlugin {
            name: name.to_string(),
            manifest,
            state: PluginState::Starting,
            process: Some(process),
            start_time: Some(std::time::Instant::now()),
            restart_count: 0,
            last_health_check: None,
            ipc_sender: None,
            auth_code,
        };

        {
            let mut plugins = self.running_plugins.write().await;
            plugins.insert(name.to_string(), running_plugin);
        }

        Ok(())
    }
}
