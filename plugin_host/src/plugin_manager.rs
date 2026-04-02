use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use interprocess::local_socket::{tokio::prelude::*, tokio::Stream, ListenerOptions};
#[cfg(windows)]
use interprocess::os::windows::{
    local_socket::ListenerOptionsExt,
    security_descriptor::{AsSecurityDescriptorMutExt, SecurityDescriptor},
};
use log::{debug, error, info, warn};
#[cfg(windows)]
use std::ptr;
use std::{
    collections::HashMap,
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use prost::Message as _;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    select,
    sync::{oneshot, RwLock},
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::{
    loader::PluginManifest,
    local_socket_name::resolve_local_socket_name,
    plugin_host_config::PluginHostConfig,
    protocol::{
        plugin_protocol::{
            AuthenticateRequest, AuthenticateResponse, AuthorizeRequest, AuthorizeResponse,
            ClientConnectedEvent, ClientDisconnectedEvent, InitializeRequest, InitializeResponse,
            MessagePublishRequest, MessagePublishResponse, MessageType, Method, ProtocolMessage,
            SubscribeRequest, SubscribeResponse,
        },
        ProtocolMessageBuilder,
    },
};

use super::loader::PluginLoader;
use anyhow::Result;
use rand::Rng;

type RequestContext = oneshot::Sender<Result<ProtocolMessage>>;

#[derive(Debug, thiserror::Error)]
enum InflightError {
    #[error("Request timed out")]
    Timeout,

    #[error("Response receiver dropped")]
    RecvError,

    #[error("Failed to send request to plugin, details {0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<TxCmd>),
}

struct InflightManager {
    inflight: DashMap<String, RequestContext>,
}

impl InflightManager {
    pub fn new() -> Self {
        Self {
            inflight: DashMap::new(),
        }
    }

    /// Send a message to plugin and wait for response with timeout
    pub async fn send_and_wait(
        &self,
        plugin_ipc_sender: &tokio::sync::mpsc::Sender<TxCmd>,
        msg: ProtocolMessage,
        timeout_duration: Duration,
    ) -> std::result::Result<ProtocolMessage, InflightError> {
        let msg_id = msg.id.clone();
        let (resp_sender, resp_receiver) = oneshot::channel();

        self.inflight.insert(msg_id.clone(), resp_sender);

        if let Err(e) = plugin_ipc_sender
            .send(TxCmd::SendMessage(Box::new(msg)))
            .await
        {
            self.clean_up(&msg_id);
            return Err(e.into());
        }

        let result = match timeout(timeout_duration, resp_receiver).await {
            std::result::Result::Ok(std::result::Result::Ok(response_msg)) => {
                response_msg.map_err(|_| InflightError::RecvError)
            }
            std::result::Result::Ok(std::result::Result::Err(_)) => Err(InflightError::RecvError),
            std::result::Result::Err(_) => Err(InflightError::Timeout),
        };

        self.clean_up(&msg_id);
        result
    }

    /// Clean up inflight request by message ID
    pub fn clean_up(&self, msg_id: &str) {
        self.inflight.remove(msg_id);
    }

    /// Remove and return the request context for a given message ID
    pub fn remove(&self, msg_id: &str) -> Option<RequestContext> {
        self.inflight.remove(msg_id).map(|(_, v)| v)
    }
}

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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
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
    pub plugin_abort_tx: Option<tokio::sync::mpsc::Sender<tokio::sync::mpsc::Sender<()>>>,
    pub plugin_log_collector_quit_tx: Option<tokio::sync::mpsc::Sender<()>>,
    pub process_wait_handle: Option<tokio::task::JoinHandle<()>>,
    pub process_log_handle: Option<tokio::task::JoinHandle<()>>,
    pub start_time: Option<std::time::Instant>,
    pub restart_count: u32,
    pub last_health_check: Option<std::time::Instant>,
    ping_response_timeout_count: u32,
    pub ipc_sender: Option<tokio::sync::mpsc::Sender<TxCmd>>,
    pub auth_code: String,
    pub logs: Arc<RwLock<Vec<String>>>,
}

impl RunningPlugin {
    pub fn is_healthy(&self) -> bool {
        self.ping_response_timeout_count < 3
    }

    pub fn reset_health_check(&mut self) {
        self.ping_response_timeout_count = 0;
        self.last_health_check = Some(std::time::Instant::now());
    }

    pub fn increment_health_check_failure(&mut self) {
        self.ping_response_timeout_count += 1;
        self.last_health_check = Some(std::time::Instant::now());
    }
}

pub struct PluginManager {
    config: PluginHostConfig,
    plugin_loader: PluginLoader,
    running_plugins: Arc<DashMap<String, RunningPlugin>>,
    inflight_manager: Arc<InflightManager>,
    hook_manager: Arc<RwLock<crate::hook::manager::HookManager>>,
    rx_cmd_sender: Option<tokio::sync::mpsc::Sender<RxCmd>>,
    listener_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    heartbeat_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    rx_cmd_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

pub enum TxCmd {
    SendMessage(Box<ProtocolMessage>),
    Shutdown,
}

pub enum RxCmd {
    InitMessage {
        msg: ProtocolMessage,
        tx_cmd_sender: tokio::sync::mpsc::Sender<TxCmd>,
    },
    NormalRecievedMessage(ProtocolMessage),
    PluginStatusChanged {
        plugin_name: String,
        plugin_auth_code: String,
        plugin_state: PluginState,
        notify_sender: tokio::sync::oneshot::Sender<()>,
    },
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
    pub async fn get_plugin_metadata_list_with_pagination(
        &self,
        offset: u64,
        limit: u64,
    ) -> (u64, Vec<PluginManifest>) {
        let res = self
            .get_running_plugins()
            .iter()
            .skip(offset as usize)
            .take(limit as usize)
            .map(|v| v.manifest.clone())
            .collect::<Vec<PluginManifest>>();
        let total_count = self.get_running_plugins().len() as u64;
        (total_count, res)
    }

    pub async fn new(config: PluginHostConfig) -> Result<Self> {
        let mut loader = PluginLoader::new(&config.plugin_directory);

        let _ = loader.scan_plugins().await?;

        Ok(Self {
            config,
            plugin_loader: loader,
            running_plugins: Arc::new(DashMap::new()),
            inflight_manager: Arc::new(InflightManager::new()),
            hook_manager: Arc::new(RwLock::new(crate::hook::manager::HookManager::new())),
            rx_cmd_sender: None,
            listener_handle: Mutex::new(None),
            heartbeat_handle: Mutex::new(None),
            rx_cmd_handle: Mutex::new(None),
        })
    }

    pub async fn start_all_plugins(&self) -> Result<()> {
        let plugin_names = self.plugin_loader.list_plugins();

        for name in plugin_names {
            self.start_plugin(name).await?;
        }

        Ok(())
    }

    pub async fn start_heartbeat_check_task(&self) {
        let running_plugins = self.running_plugins.clone();
        let inflight_manager = self.inflight_manager.clone();
        let hook_manager = self.hook_manager.clone();
        let interval_secs = self.config.health_check_interval_secs;
        let ping_timeout = self.config.ping_timeout();
        let mut shutdown_rx = self.config.shutdown_signal.subscribe();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

            loop {
                tokio::select! {
                    recv_result = shutdown_rx.recv() => {
                        match recv_result {
                            Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                break;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                continue;
                            }
                        }
                    }
                    _ = interval.tick() => {}
                }

                let target_plugins: Vec<(String, String, tokio::sync::mpsc::Sender<TxCmd>)> =
                    running_plugins
                        .iter()
                        .filter_map(|plugin| {
                            if plugin.state == PluginState::Running {
                                if let Some(ipc_sender) = &plugin.ipc_sender {
                                    Some((
                                        plugin.name.clone(),
                                        plugin.auth_code.clone(),
                                        ipc_sender.clone(),
                                    ))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect();

                for (name, instance_id, ipc_sender) in target_plugins {
                    let ping_message = ProtocolMessageBuilder::new()
                        .with_method(Method::Ping)
                        .with_type(MessageType::Request)
                        .build();

                    let result = inflight_manager
                        .send_and_wait(&ipc_sender, ping_message.clone(), ping_timeout)
                        .await;

                    let mut hook_instance_to_remove = None;
                    if let Some(mut plugin) = running_plugins.get_mut(&name) {
                        if plugin.auth_code != instance_id {
                            continue;
                        }
                        match result {
                            std::result::Result::Ok(_response) => {
                                // Plugin is healthy
                                plugin.reset_health_check();
                            }
                            Err(e) => {
                                println!("Plugin {} heartbeat check failed: {}", plugin.name, e);
                                plugin.increment_health_check_failure();
                            }
                        }
                        if !plugin.is_healthy() {
                            warn!(
                                "Plugin {} failed heartbeat check 3 times, marking as Failed",
                                plugin.name
                            );
                            plugin.state = PluginState::Failed;
                            plugin.ipc_sender = None;
                            hook_instance_to_remove = Some(plugin.auth_code.clone());
                        }
                    }
                    if let Some(instance_id) = hook_instance_to_remove {
                        hook_manager
                            .write()
                            .await
                            .remove_plugin_instance(&instance_id);
                    }
                }
            }
        });

        *self.heartbeat_handle.lock().unwrap() = Some(handle);
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
        let init_timeout = config.init_timeout();

        tokio::spawn(async move {
            let wrap_initialize_param_to_any = prost_types::Any {
                type_url: super::protocol::INIT_REQUEST_TYPE_URL.to_owned(),
                value: initialize_request_param.encode_to_vec(),
            };

            let init_plugin_request_message = ProtocolMessageBuilder::new()
                .with_method(Method::Initialize)
                .with_params(wrap_initialize_param_to_any)
                .build();

            if let Err(e) = framed.send(init_plugin_request_message).await {
                warn!(
                    "Failed to send init message to plugin: {}, terminating connection",
                    e
                );
                return;
            }

            match tokio::time::timeout(init_timeout, framed.next()).await {
                std::result::Result::Ok(Some(std::result::Result::Ok(msg))) => {
                    let protocol_message = ProtocolMessage::decode(msg.payload);
                    println!(
                        "Received init response protocol message: {:?}",
                        protocol_message
                    );
                    if let std::result::Result::Ok(protocol_message) = protocol_message {
                        match &protocol_message.result {
                            Some(r) if r.type_url == super::protocol::INIT_RESPONSE_TYPE_URL => {
                                let _ = rx_cmd_sender
                                    .send(RxCmd::InitMessage {
                                        msg: protocol_message,
                                        tx_cmd_sender: tx_cmd_sender.clone(),
                                    })
                                    .await;
                            }
                            Some(_) => {
                                warn!(
                                    "Plugin init response has invalid result type, closing connection"
                                );
                                return;
                            }
                            None => {
                                warn!("Plugin init response has no result, closing connection");
                                return;
                            }
                        }
                    } else {
                        warn!("Failed to decode protocol message from plugin");
                        return;
                    }
                }

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
                }
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
                                println!("Send message to plugin");
                                if let Err(e) = framed.send(*msg).await {
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

    fn start_handle_plugin_rx_cmd(
        &self,
        mut rx_cmd_receiver: tokio::sync::mpsc::Receiver<RxCmd>,
    ) -> tokio::task::JoinHandle<()> {
        let plugins = self.running_plugins.clone();
        let inflight_manager = self.inflight_manager.clone();
        let hook_manager = self.hook_manager.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = rx_cmd_receiver.recv() => {
                        match msg {
                            Some(RxCmd::PluginStatusChanged{
                                plugin_name: name,
                                plugin_auth_code,
                                plugin_state: state,
                                notify_sender,
                            }) => {
                                if matches!(state, PluginState::Stopped | PluginState::Failed) {
                                    hook_manager
                                        .write()
                                        .await
                                        .remove_plugin_instance(&plugin_auth_code);
                                }
                                if let Some(mut plugin) = plugins.get_mut(&name) {
                                    if plugin.auth_code != plugin_auth_code {
                                        info!(
                                            "Ignore stale plugin status change for '{}' with auth_code '{}'",
                                            name, plugin_auth_code
                                        );
                                        let _ = notify_sender.send(());
                                        continue;
                                    }
                                    info!("Plugin {} status changed to {:?}", name, state.clone(),);
                                    if state == PluginState::Stopped {
                                        plugin.plugin_abort_tx = None;
                                        plugin.plugin_log_collector_quit_tx = None;
                                        plugin.process_log_handle = None;
                                        plugin.process_wait_handle = None;
                                        plugin.ipc_sender = None;
                                    }

                                    plugin.state = state;
                                }
                                let _ = notify_sender.send(());
                            },
                            Some(RxCmd::InitMessage{msg, tx_cmd_sender}) => {
                                // Handle initialization message
                                info!("Received initialization message: {:?}", msg);
                                if let Some(r) = msg.result {
                                    match InitializeResponse::decode(r.value.as_slice()) {
                                        Err(e) => {
                                            warn!("Failed to decode initialize response: {} drop it", e);
                                            return;
                                        },
                                        std::result::Result::Ok(init_response) => {
                                            info!("Plugin initialized with response: {:?}", init_response);
                                            // Process the initialization response
                                            let auth_code = init_response.auth_code;
                                            let mut register_hooks = Vec::new();
                                            for hook in init_response.hooks {
                                                let name = &hook.name;
                                                let hook_type = crate::hook::get_hook_from_name(name);
                                                if let Some(hook_type) = hook_type {
                                                    register_hooks.push((hook_type, hook.priority));
                                                } else {
                                                    warn!("Unknown hook name '{}' from plugin init response", name);
                                                }
                                            }

                                            let mut hook_registration = None;
                                            for mut plugin in plugins.iter_mut() {
                                                // find the plugin with matching auth_code
                                                if plugin.auth_code == auth_code {
                                                    plugin.ipc_sender = Some(tx_cmd_sender.clone());
                                                    plugin.state = PluginState::Running;
                                                    hook_registration = Some((
                                                        plugin.name.clone(),
                                                        plugin.auth_code.clone(),
                                                    ));
                                                    break;
                                                }
                                            }
                                            if let Some((plugin_name, instance_id)) = hook_registration {
                                                hook_manager
                                                    .write()
                                                    .await
                                                    .replace_plugin_hooks(&plugin_name, &instance_id, &register_hooks);
                                            } else {
                                                warn!(
                                                    "No running plugin matched init auth_code '{}', skip hook registration",
                                                    auth_code
                                                );
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
                                            if let Some(resp_sender) = inflight_manager.remove(msg_id) {
                                                if resp_sender.send(Ok(msg)).is_err() {
                                                    warn!("Failed to send response : receiver has dropped");
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
        })
    }

    pub async fn start_listener(&mut self) -> Result<()> {
        let socket_path = self.config.local_socket_path.clone();

        #[cfg(not(windows))]
        {
            let path = std::path::Path::new(&socket_path);
            if path.exists() {
                std::fs::remove_file(path)?; // remove the existing socket file
            }
        }

        let name = resolve_local_socket_name(&socket_path)?;

        let (rx_cmd_sender, rx_cmd_receiver) = tokio::sync::mpsc::channel::<RxCmd>(32);

        self.rx_cmd_sender = Some(rx_cmd_sender.clone());

        let rx_cmd_handle = self.start_handle_plugin_rx_cmd(rx_cmd_receiver);
        *self.rx_cmd_handle.lock().unwrap() = Some(rx_cmd_handle);

        let opts = ListenerOptions::new().name(name);
        #[cfg(windows)]
        let opts = {
            let mut security_descriptor = SecurityDescriptor::new()?;
            unsafe {
                security_descriptor.set_dacl(ptr::null_mut(), false)?;
            }
            opts.security_descriptor(security_descriptor)
        };

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
        let mut shutdown_rx = self.config.shutdown_signal.subscribe();

        let listener_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    recv_result = shutdown_rx.recv() => {
                        match recv_result {
                            Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                break;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                continue;
                            }
                        }
                    }
                    accept_result = listener.accept() => {
                        match accept_result {
                            std::result::Result::Ok(c) => {
                                info!("Plugin connected, start handling connection");
                                let _ = Self::handle_plugin_connection(
                                    &config,
                                    c,
                                    rx_cmd_sender.clone(),
                                )
                                .await;
                            }
                            Err(e) => {
                                warn!("Plugin listener accept failed: {}", e);
                                continue;
                            }
                        };
                    }
                }
            }

            #[cfg(not(windows))]
            {
                let path = std::path::Path::new(&socket_path);
                if path.exists() {
                    if let Err(e) = std::fs::remove_file(path) {
                        warn!("Failed to remove local socket '{}': {}", socket_path, e);
                    }
                }
            }
        });

        *self.listener_handle.lock().unwrap() = Some(listener_handle);

        Ok(())
    }

    async fn await_background_task(task_name: &str, handle: Option<tokio::task::JoinHandle<()>>) {
        if let Some(handle) = handle {
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => info!("{} exited", task_name),
                Ok(Err(e)) => warn!("{} panicked: {:?}", task_name, e),
                Err(_) => warn!("{} shutdown timed out", task_name),
            }
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        info!("shutting down plugin manager");

        let _ = self.config.shutdown_signal.send(());

        let plugin_names = self
            .running_plugins
            .iter()
            .map(|plugin| plugin.name.clone())
            .collect::<Vec<_>>();

        for plugin_name in plugin_names {
            if let Err(e) = self.stop_plugin(&plugin_name).await {
                warn!("Failed to stop plugin '{}': {}", plugin_name, e);
            }
        }

        if let Some(rx_cmd_sender) = &self.rx_cmd_sender {
            let _ = rx_cmd_sender.send(RxCmd::Shutdown).await;
        }

        let listener_handle = self.listener_handle.lock().unwrap().take();
        let heartbeat_handle = self.heartbeat_handle.lock().unwrap().take();
        let rx_cmd_handle = self.rx_cmd_handle.lock().unwrap().take();

        Self::await_background_task("plugin listener", listener_handle).await;
        Self::await_background_task("plugin heartbeat task", heartbeat_handle).await;
        Self::await_background_task("plugin rx task", rx_cmd_handle).await;

        Ok(())
    }

    pub fn get_running_plugins(&self) -> Arc<DashMap<String, RunningPlugin>> {
        self.running_plugins.clone()
    }

    fn get_registered_plugin_sender(
        &self,
        registered_hook: &crate::hook::manager::RegisteredHook,
        hook_name: &str,
    ) -> Option<(tokio::sync::mpsc::Sender<TxCmd>, String)> {
        let running_plugin = self.running_plugins.get(&registered_hook.plugin_name)?;

        if running_plugin.auth_code != registered_hook.instance_id {
            debug!(
                "Skip stale hook registration for plugin '{}' when calling {} hook",
                registered_hook.plugin_name, hook_name
            );
            return None;
        }

        if !matches!(running_plugin.state, PluginState::Running) {
            return None;
        }

        if let Some(ipc_sender) = running_plugin.ipc_sender.as_ref() {
            Some((ipc_sender.clone(), running_plugin.name.clone()))
        } else {
            error!(
                "Plugin {} ipc sender not found when calling {} hook",
                running_plugin.name, hook_name
            );
            None
        }
    }

    pub async fn call_subscribe_removed_hook(&self, subscribe_request: SubscribeRequest) {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::SubscribeRemoved);

        if let Some(plugins) = plugins {
            for plugin in plugins {
                if let Some((ipc_sender, _plugin_name)) =
                    self.get_registered_plugin_sender(plugin, "subscribe removed")
                {
                    let subscribe_request_any_wrapper = prost_types::Any {
                        type_url: crate::protocol::SUBSCRIBE_REQUEST_TYPE_URL.to_string(),
                        value: subscribe_request.encode_to_vec(),
                    };

                    let protocol_message = ProtocolMessageBuilder::new()
                        .with_method(Method::SubscriptionRemoved)
                        .with_type(MessageType::Event)
                        .with_params(subscribe_request_any_wrapper)
                        .build();

                    let _ = ipc_sender
                        .send(TxCmd::SendMessage(Box::new(protocol_message)))
                        .await;
                }
            }
        }
    }

    pub async fn call_client_connected_hook(&self, client_connected_event: ClientConnectedEvent) {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::ClientConnected);

        if let Some(plugins) = plugins {
            for plugin in plugins {
                if let Some((ipc_sender, _plugin_name)) =
                    self.get_registered_plugin_sender(plugin, "client connected")
                {
                    let client_connected_event_any_wrapper = prost_types::Any {
                        type_url: crate::protocol::CLIENT_CONNECTED_EVENT_TYPE_URL.to_string(),
                        value: client_connected_event.encode_to_vec(),
                    };

                    let protocol_message = ProtocolMessageBuilder::new()
                        .with_method(Method::ClientConnected)
                        .with_type(MessageType::Event)
                        .with_params(client_connected_event_any_wrapper)
                        .build();

                    let _ = ipc_sender
                        .send(TxCmd::SendMessage(Box::new(protocol_message)))
                        .await;
                }
            }
        }
    }

    pub async fn call_client_disconnected_hook(
        &self,
        client_disconnected_event: ClientDisconnectedEvent,
    ) {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::ClientDisconnected);

        if let Some(plugins) = plugins {
            for plugin in plugins {
                if let Some((ipc_sender, _plugin_name)) =
                    self.get_registered_plugin_sender(plugin, "client disconnected")
                {
                    let client_disconnected_event_any_wrapper = prost_types::Any {
                        type_url: crate::protocol::CLIENT_DISCONNECTED_EVENT_TYPE_URL.to_string(),
                        value: client_disconnected_event.encode_to_vec(),
                    };

                    let protocol_message = ProtocolMessageBuilder::new()
                        .with_method(Method::ClientDisconnected)
                        .with_type(MessageType::Event)
                        .with_params(client_disconnected_event_any_wrapper)
                        .build();

                    let _ = ipc_sender
                        .send(TxCmd::SendMessage(Box::new(protocol_message)))
                        .await;
                }
            }
        }
    }

    pub async fn call_subscribe_added_hook(&self, subscribe_request: SubscribeRequest) {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::SubscribeAdded);

        if let Some(plugins) = plugins {
            for plugin in plugins {
                if let Some((ipc_sender, _plugin_name)) =
                    self.get_registered_plugin_sender(plugin, "subscribe added")
                {
                    let subscribe_request_any_wrapper = prost_types::Any {
                        type_url: crate::protocol::SUBSCRIBE_REQUEST_TYPE_URL.to_string(),
                        value: subscribe_request.encode_to_vec(),
                    };

                    let protocol_message = ProtocolMessageBuilder::new()
                        .with_method(Method::SubscriptionAdded)
                        .with_type(MessageType::Event)
                        .with_params(subscribe_request_any_wrapper)
                        .build();

                    let _ = ipc_sender
                        .send(TxCmd::SendMessage(Box::new(protocol_message)))
                        .await;
                }
            }
        }
    }

    pub async fn call_message_published_hook(
        &self,
        message_publish_request: MessagePublishRequest,
    ) {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::MessagePublished);

        if let Some(plugins) = plugins {
            for plugin in plugins {
                if let Some((ipc_sender, _plugin_name)) =
                    self.get_registered_plugin_sender(plugin, "message published")
                {
                    let message_publish_request_any_wrapper = prost_types::Any {
                        type_url: crate::protocol::MQTT_MESSAGE_PUBLISH_REQUEST_TYPE_URL
                            .to_string(),
                        value: message_publish_request.encode_to_vec(),
                    };

                    let protocol_message = ProtocolMessageBuilder::new()
                        .with_method(Method::MessagePublished)
                        .with_type(MessageType::Event)
                        .with_params(message_publish_request_any_wrapper)
                        .build();

                    let _ = ipc_sender
                        .send(TxCmd::SendMessage(Box::new(protocol_message)))
                        .await;
                }
            }
        }
    }

    // Called when message publish before, it can be used to modify the message.
    pub async fn call_on_message_publish(
        &self,
        message_publish_request: MessagePublishRequest,
    ) -> Result<MessagePublishResult, PluginManagerError> {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::OnMessagePublish);

        if let Some(plugins) = plugins {
            for plugin in plugins {
                let (ipc_sender, plugin_name) = {
                    if let Some(plugin_sender) =
                        self.get_registered_plugin_sender(plugin, "on message publish")
                    {
                        plugin_sender
                    } else {
                        continue;
                    }
                };

                let message_publish_request_any_wrapper = prost_types::Any {
                    type_url: crate::protocol::MQTT_MESSAGE_PUBLISH_REQUEST_TYPE_URL.to_string(),
                    value: message_publish_request.encode_to_vec(),
                };

                let protocol_message = ProtocolMessageBuilder::new()
                    .with_method(Method::OnMessagePublish)
                    .with_type(MessageType::Request)
                    .with_params(message_publish_request_any_wrapper)
                    .build();

                let result = self
                    .inflight_manager
                    .send_and_wait(&ipc_sender, protocol_message, self.config.request_timeout())
                    .await;

                match result {
                    std::result::Result::Ok(response) => {
                        if let Some(result) = response.result {
                            if result.type_url
                                == crate::protocol::MQTT_MESSAGE_PUBLISH_RESPONSE_TYPE_URL
                            {
                                match MessagePublishResponse::decode(result.value.as_slice()) {
                                    Err(e) => {
                                        warn!(
                                            "Failed to decode message publish response: {} drop it",
                                            e
                                        );
                                        continue;
                                    }
                                    std::result::Result::Ok(message_publish_response) => {
                                        if !message_publish_response.continue_chain() {
                                            let message_publish_result = MessagePublishResult {
                                                allow: message_publish_response.allow,
                                                modified_message: message_publish_response
                                                    .modified_message,
                                                error_reason: message_publish_response.error_reason,
                                            };
                                            return std::result::Result::Ok(message_publish_result);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Plugin {} call failed: {}", plugin_name, e);
                        continue;
                    }
                }
            }
        }
        let default_result = MessagePublishResult {
            allow: true,
            modified_message: message_publish_request.message,
            error_reason: None,
        };
        std::result::Result::Ok(default_result)
    }

    // Called when a client subscribe to a topic, if no plugin return the default result that all topics are allowed.
    pub async fn call_on_message_subscribe(
        &self,
        subscribe_request: SubscribeRequest,
    ) -> Result<SubscribeResult, PluginManagerError> {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::OnMessageSubscribe);

        let mut final_result = SubscribeResult {
            result: subscribe_request
                .subscriptions
                .iter()
                .map(|topic_filter| SubscribeResultItem {
                    topic: topic_filter.topic.clone(),
                    allowed: true,
                    granted_qos: topic_filter.qos as u8,
                    reason: None,
                })
                .collect(),
        };

        if let Some(plugins) = plugins {
            for plugin in plugins {
                let (ipc_sender, plugin_name) = {
                    if let Some(plugin_sender) =
                        self.get_registered_plugin_sender(plugin, "on message subscribe")
                    {
                        plugin_sender
                    } else {
                        continue;
                    }
                };

                let subscribe_request_any_wrapper = prost_types::Any {
                    type_url: crate::protocol::SUBSCRIBE_REQUEST_TYPE_URL.to_string(),
                    value: subscribe_request.encode_to_vec(),
                };

                let protocol_message = ProtocolMessageBuilder::new()
                    .with_method(Method::OnMessageSubscribe)
                    .with_type(MessageType::Request)
                    .with_params(subscribe_request_any_wrapper)
                    .build();

                let result = self
                    .inflight_manager
                    .send_and_wait(&ipc_sender, protocol_message, self.config.request_timeout())
                    .await;

                match result {
                    std::result::Result::Ok(response) => {
                        if let Some(result) = response.result {
                            if result.type_url == crate::protocol::SUBSCRIBE_RESPONSE_TYPE_URL {
                                match SubscribeResponse::decode(result.value.as_slice()) {
                                    Err(e) => {
                                        warn!("Failed to decode subscribe response: {} drop it", e);
                                        continue;
                                    }
                                    std::result::Result::Ok(subscribe_response) => {
                                        for subscribe_result_item in
                                            subscribe_response.results.iter()
                                        {
                                            if let Some(item) =
                                                final_result.result.iter_mut().find(|item| {
                                                    item.topic == subscribe_result_item.topic
                                                })
                                            {
                                                item.allowed = subscribe_result_item.allowed;
                                                item.granted_qos =
                                                    subscribe_result_item.granted_qos as u8;
                                                item.reason = subscribe_result_item.reason.clone();
                                            } else {
                                                warn!(
                                                    "Topic '{}' not found in final_result",
                                                    subscribe_result_item.topic
                                                );
                                            }
                                        }
                                        if !subscribe_response.continue_chain() {
                                            return std::result::Result::Ok(final_result);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Plugin {} call failed: {}", plugin_name, e);
                        continue;
                    }
                }
            }
        }

        std::result::Result::Ok(final_result)
    }

    // Authorize connect, subscribe, publish
    pub async fn call_authorize_hook(
        &self,
        authorize_request: AuthorizeRequest,
    ) -> std::result::Result<AuthorizeResult, PluginManagerError> {
        let hook_manager = self.hook_manager.read().await;
        let plugins = hook_manager.get_hooks(&crate::hook::Hook::Authorize);

        let mut final_result = AuthorizeResult {
            authorized: true,
            reason: None,
            modified_context: HashMap::new(),
        };

        if let Some(plugins) = plugins {
            for plugin in plugins {
                let (ipc_sender, plugin_name) = {
                    if let Some(plugin_sender) =
                        self.get_registered_plugin_sender(plugin, "authorize")
                    {
                        plugin_sender
                    } else {
                        continue;
                    }
                };

                let authorize_request_any_wrapper = prost_types::Any {
                    type_url: crate::protocol::AUTHORIZE_REQUEST_TYPE_URL.to_string(),
                    value: authorize_request.encode_to_vec(),
                };

                let protocol_message = ProtocolMessageBuilder::new()
                    .with_method(Method::Authorize)
                    .with_params(authorize_request_any_wrapper)
                    .build();

                let result = self
                    .inflight_manager
                    .send_and_wait(&ipc_sender, protocol_message, self.config.request_timeout())
                    .await;

                match result {
                    std::result::Result::Ok(response) => {
                        if let Some(result) = response.result {
                            if result.type_url == crate::protocol::AUTHORIZE_RESPONSE_TYPE_URL {
                                match AuthorizeResponse::decode(result.value.as_slice()) {
                                    Err(e) => {
                                        warn!("Failed to decode authorize response: {} drop it", e);
                                        continue;
                                    }
                                    std::result::Result::Ok(authorize_response) => {
                                        if !authorize_response.authorized {
                                            final_result.authorized = authorize_response.authorized;
                                            final_result.reason = authorize_response.reason.clone();
                                            final_result.modified_context = HashMap::new();
                                            return std::result::Result::Ok(final_result);
                                        } else {
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => match e {
                        InflightError::Timeout => {
                            warn!("Plugin {} response timeout", plugin_name);
                            return std::result::Result::Ok(AuthorizeResult {
                                authorized: false,
                                reason: Some(format!("Plugin {} response timeout", plugin_name)),
                                modified_context: HashMap::new(),
                            });
                        }
                        _ => {
                            warn!("Plugin {} call failed: {}", plugin_name, e);
                            continue;
                        }
                    },
                }
            }
            std::result::Result::Ok(final_result)
        } else {
            // return default authorize result
            debug!("no plugins registered for OnAuthorize hook");
            std::result::Result::Ok(AuthorizeResult {
                authorized: self.config.default_authorize_result,
                reason: None,
                modified_context: HashMap::new(),
            })
        }
    }

    // Call the OnAuthenticate hook for all registered plugins(for login authentication)
    pub async fn call_authenticate_hook(
        &self,
        authenticate_request: AuthenticateRequest,
    ) -> std::result::Result<AuthenticateResult, PluginManagerError> {
        let hook_manager = self.hook_manager.read().await;
        let Some(plugins) = hook_manager.get_hooks(&crate::hook::Hook::Authenticate) else {
            debug!("no plugins registered for OnAuthenticate hook");
            return std::result::Result::Ok(AuthenticateResult {
                authenticated: self.config.default_authenticate_result,
                error_reason: None,
                tenant_id: None,
            });
        };

        let mut final_result = AuthenticateResult {
            authenticated: false,
            error_reason: None,
            tenant_id: None,
        };
        let mut is_first_called = true;
        let mut called_any_plugin = false;

        for plugin in plugins {
            let (ipc_sender, plugin_name) = {
                if let Some(plugin_sender) =
                    self.get_registered_plugin_sender(plugin, "authenticate")
                {
                    called_any_plugin = true;
                    info!("Authenticate plugin: {}", plugin_sender.1);
                    plugin_sender
                } else {
                    continue;
                }
            };

            let authenticate_request_any_wrapper = prost_types::Any {
                type_url: crate::protocol::AUTHENTICATE_REQUEST_TYPE_URL.to_string(),
                value: authenticate_request.encode_to_vec(),
            };
            let protocol_message = ProtocolMessageBuilder::new()
                .with_method(Method::Authenticate)
                .with_params(authenticate_request_any_wrapper)
                .build();

            let result = self
                .inflight_manager
                .send_and_wait(&ipc_sender, protocol_message, self.config.request_timeout())
                .await;

            match result {
                std::result::Result::Ok(response) => {
                    if let Some(result) = response.result {
                        if result.type_url == crate::protocol::AUTHENTICATE_RESPONSE_TYPE_URL {
                            match AuthenticateResponse::decode(result.value.as_slice()) {
                                Err(e) => {
                                    warn!("Failed to decode authenticate response: {} drop it", e);
                                    continue;
                                }
                                std::result::Result::Ok(authenticate_response) => {
                                    if authenticate_response.authenticated {
                                        if !is_first_called {
                                            if authenticate_response.tenant_id
                                                != final_result.tenant_id
                                            {
                                                return std::result::Result::Ok(
                                                    AuthenticateResult {
                                                        authenticated: false,
                                                        error_reason: Some(
                                                            "Tenant ID mismatch".to_string(),
                                                        ),
                                                        tenant_id: None,
                                                    },
                                                );
                                            }
                                        } else {
                                            is_first_called = false;
                                        }
                                        final_result.authenticated = true;
                                        final_result.error_reason = None;
                                        final_result.tenant_id =
                                            authenticate_response.tenant_id.clone();
                                        continue;
                                    } else {
                                        return std::result::Result::Ok(AuthenticateResult {
                                            authenticated: authenticate_response.authenticated,
                                            error_reason: authenticate_response.error_reason,
                                            tenant_id: authenticate_response.tenant_id,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => match e {
                    InflightError::Timeout => {
                        warn!("Plugin {} response timeout", plugin_name);
                        return std::result::Result::Ok(AuthenticateResult {
                            authenticated: false,
                            error_reason: Some(format!("Plugin {} response timeout", plugin_name)),
                            tenant_id: None,
                        });
                    }
                    _ => {
                        warn!("Plugin {} call failed: {}", plugin_name, e);
                        continue;
                    }
                },
            }
        }

        if !called_any_plugin {
            warn!("authenticate hooks are registered but no active plugin instances are available");
            return std::result::Result::Ok(AuthenticateResult {
                authenticated: false,
                error_reason: None,
                tenant_id: None,
            });
        }

        std::result::Result::Ok(final_result)
    }

    pub async fn stop_plugin(&self, name: &str) -> Result<()> {
        let (abort_tx, log_collector_quit_tx) = {
            let running_plugin = self.running_plugins.get(name);
            if let Some(running_plugin) = running_plugin {
                if matches!(
                    running_plugin.state,
                    PluginState::Running | PluginState::Starting
                ) {
                    match &running_plugin.plugin_abort_tx {
                        None => {
                            return Err(anyhow::anyhow!(
                                "plugin '{}' abort channel not found",
                                name
                            ));
                        }
                        Some(abort_tx) => (
                            abort_tx.clone(),
                            running_plugin.plugin_log_collector_quit_tx.clone(),
                        ),
                    }
                } else {
                    return Ok(());
                }
            } else {
                return Err(anyhow::anyhow!("plugin '{}' not found", name));
            }
        };

        if let Some(mut running_plugin) = self.running_plugins.get_mut(name) {
            running_plugin.state = PluginState::Stopping;
        }

        info!("stopping plugin '{}'...", name);
        let (notify_sender, mut notify_receiver) = tokio::sync::mpsc::channel::<()>(1);
        if let Err(e) = abort_tx.send(notify_sender).await {
            return Err(anyhow::anyhow!(
                "failed to send abort signal to plugin '{}': {}",
                name,
                e
            ));
        }
        let _ = notify_receiver.recv().await;

        if let Some(log_collector_quit_tx) = log_collector_quit_tx {
            let _ = log_collector_quit_tx.send(()).await;
        }

        info!("plugin '{}' stopped successfully", name);

        Ok(())
    }

    pub async fn restart_plugin(&self, name: &str) -> Result<()> {
        {
            let running_plugin = self.running_plugins.get(name);
            if running_plugin.is_none() {
                return Err(anyhow::anyhow!("Plugin '{}' not found", name));
            }
        }

        if let Err(e) = self.stop_plugin(name).await {
            return Err(anyhow::anyhow!(
                "failed to stop plugin '{}' before restart: {}",
                name,
                e
            ));
        } else {
            info!("plugin '{}' stopped successfully, starting...", name);
            if let Err(e) = self.start_plugin(name).await {
                return Err(anyhow::anyhow!(
                    "failed to start plugin '{}' during restart: {}",
                    name,
                    e
                ));
            }
            info!("plugin '{}' restarted successfully", name);
        }

        Ok(())
    }

    pub async fn start_plugin(&self, name: &str) -> Result<()> {
        let manifest = self
            .plugin_loader
            .get_plugin_manifest(name)
            .ok_or_else(|| anyhow::anyhow!("plugin '{}' not found", name))?
            .clone();

        let auth_code = generate_auth_code(12);

        let mut command = self
            .plugin_loader
            .get_plugin_command(name, &auth_code, &self.config.local_socket_path)?
            .ok_or_else(|| anyhow::anyhow!("plugin {} start command not exsited", name))?;

        let mut process = match command
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Err(e) => {
                warn!("failed to start plugin '{}': {}", name, e);

                let running_plugin = RunningPlugin {
                    name: name.to_string(),
                    manifest,
                    state: PluginState::Failed,
                    plugin_abort_tx: None,
                    plugin_log_collector_quit_tx: None,
                    start_time: Some(std::time::Instant::now()),
                    process_log_handle: None,
                    process_wait_handle: None,
                    restart_count: 0,
                    last_health_check: None,
                    ipc_sender: None,
                    auth_code,
                    logs: Arc::new(RwLock::new(Vec::new())),
                    ping_response_timeout_count: 0,
                };

                {
                    self.running_plugins
                        .insert(name.to_string(), running_plugin);
                }
                return Err(anyhow::anyhow!("failed to start plugin '{}': {}", name, e));
            }
            std::result::Result::Ok(p) => p,
        };

        let stdout = process
            .stderr
            .take()
            .expect("plugin did not have a handle to stdout");

        let mut stdout_reader = BufReader::new(stdout).lines();

        let borrowed_name = name.to_string();
        let borrowed_auth_code = auth_code.clone();

        let (plugin_abort_tx, mut plugin_abort_rx) =
            tokio::sync::mpsc::channel::<tokio::sync::mpsc::Sender<()>>(1);

        let rx_cmd_sender = self
            .rx_cmd_sender
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("rx_cmd_sender not found"))?
            .clone();

        let process_wait_handle = tokio::spawn(async move {
            let mut pending_notify: Option<tokio::sync::mpsc::Sender<()>> = None;

            loop {
                select! {
                    status_result = process.wait() => {
                        match status_result {
                            std::result::Result::Ok(s) => {
                                info!("plugin '{}' exited with status: {}", borrowed_name, s);
                            }
                            Err(e) => {
                                info!("plugin '{}' encountered an error while waiting: {}", borrowed_name, e);
                            }
                        }

                        let (changed_notify_sender, changed_notify_receiver) = tokio::sync::oneshot::channel::<()>();
                        let _ = rx_cmd_sender.send(RxCmd::PluginStatusChanged{
                            plugin_name: borrowed_name,
                            plugin_auth_code: borrowed_auth_code.clone(),
                            plugin_state: PluginState::Stopped,
                            notify_sender: changed_notify_sender
                        }).await;

                        let _ = changed_notify_receiver.await;

                        if let Some(notify_sender) = pending_notify.take() {
                            let _ = notify_sender.send(()).await;
                        }

                        break;
                    },
                    notify_sender = plugin_abort_rx.recv() => {
                        info!("plugin '{}' aborted", borrowed_name);

                        pending_notify = notify_sender;

                        if let Err(e) = process.kill().await {
                            info!("failed to kill plugin '{}': {}", borrowed_name, e);
                        } else {
                            info!("plugin '{}' killed successfully", borrowed_name);
                        }
                    }
                }
            }
            info!("process_wait_handle exit");
        });

        let logs = Arc::new(RwLock::new(Vec::new()));

        let logs_clone = logs.clone();

        let borrowed_name = name.to_string();

        let (plugin_log_collector_quit_tx, mut plugin_log_collector_quit_rx) =
            tokio::sync::mpsc::channel::<()>(1);

        let process_log_handle = tokio::spawn(async move {
            // Capture plugin stdout into logs
            loop {
                select! {
                    _ = plugin_log_collector_quit_rx.recv() => {
                        info!("plugin '{}' log collector quit", borrowed_name);
                        break;
                    },
                    std::result::Result::Ok(Some(line)) = stdout_reader.next_line() => {
                        let mut logs_guard = logs_clone.write().await;
                        logs_guard.push(line);
                        if logs_guard.len() > 1000 {
                            logs_guard.remove(0); // keep only the last 1000 lines
                        }
                    }
                }
            }
            println!("process_log_handle exit");
        });

        let running_plugin = RunningPlugin {
            name: name.to_string(),
            manifest,
            state: PluginState::Starting,
            plugin_abort_tx: Some(plugin_abort_tx),
            plugin_log_collector_quit_tx: Some(plugin_log_collector_quit_tx),
            start_time: Some(std::time::Instant::now()),
            process_log_handle: Some(process_log_handle),
            process_wait_handle: Some(process_wait_handle),
            restart_count: 0,
            last_health_check: None,
            ipc_sender: None,
            auth_code,
            logs,
            ping_response_timeout_count: 0,
        };

        {
            self.running_plugins
                .insert(name.to_string(), running_plugin);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_config(base_dir: &std::path::Path, socket_name: &str) -> PluginHostConfig {
        let plugin_dir = base_dir.join("plugins");
        std::fs::create_dir_all(&plugin_dir).unwrap();

        let (shutdown_signal, _) = tokio::sync::broadcast::channel(1);

        PluginHostConfig {
            broker_version: "test".to_string(),
            broker_node_id: 1,
            cluster_name: "test-cluster".to_string(),
            plugin_directory: plugin_dir.to_string_lossy().into_owned(),
            local_socket_path: base_dir.join(socket_name).to_string_lossy().into_owned(),
            max_restart_attempts: 1,
            health_check_interval_secs: 1,
            init_timeout_secs: 1,
            request_timeout_secs: 1,
            ping_timeout_secs: 1,
            shutdown_signal,
            default_authorize_result: false,
            default_authenticate_result: false,
        }
    }

    #[tokio::test]
    async fn send_and_wait_should_cleanup_inflight_after_timeout() {
        let inflight_manager = InflightManager::new();
        let (tx_cmd_sender, mut tx_cmd_receiver) = tokio::sync::mpsc::channel::<TxCmd>(1);
        let protocol_message = ProtocolMessageBuilder::new()
            .with_method(Method::Ping)
            .with_type(MessageType::Request)
            .build();

        let recv_handle = tokio::spawn(async move { tx_cmd_receiver.recv().await });

        let result = inflight_manager
            .send_and_wait(&tx_cmd_sender, protocol_message, Duration::from_millis(10))
            .await;

        assert!(matches!(result, Err(InflightError::Timeout)));
        assert_eq!(inflight_manager.inflight.len(), 0);

        let _ = recv_handle.await;
    }

    #[tokio::test]
    async fn send_and_wait_should_cleanup_inflight_after_send_error() {
        let inflight_manager = InflightManager::new();
        let (tx_cmd_sender, tx_cmd_receiver) = tokio::sync::mpsc::channel::<TxCmd>(1);
        drop(tx_cmd_receiver);

        let protocol_message = ProtocolMessageBuilder::new()
            .with_method(Method::Ping)
            .with_type(MessageType::Request)
            .build();

        let result = inflight_manager
            .send_and_wait(&tx_cmd_sender, protocol_message, Duration::from_millis(10))
            .await;

        assert!(matches!(result, Err(InflightError::SendError(_))));
        assert_eq!(inflight_manager.inflight.len(), 0);
    }

    #[tokio::test]
    async fn shutdown_should_release_listener_for_reuse_of_same_socket_path() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path(), "plugin_host.sock");

        let mut first_manager = PluginManager::new(config.clone()).await.unwrap();
        first_manager.start_listener().await.unwrap();
        first_manager.shutdown().await.unwrap();

        let mut second_manager = PluginManager::new(config).await.unwrap();
        second_manager.start_listener().await.unwrap();
        second_manager.shutdown().await.unwrap();
    }
}
