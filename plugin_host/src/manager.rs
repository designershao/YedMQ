use std::{collections::HashMap, sync::Arc};
use futures::{SinkExt, StreamExt, TryStreamExt};
use interprocess::{local_socket::{tokio::prelude::*, GenericNamespaced, ListenerOptions, ToNsName, tokio::Stream}};
use log::{info, warn};

use prost::Message as _;
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio_util::codec::Framed;

use crate::{loader::PluginManifest, plugin_host_config::PluginHostConfig, ProtocolMessage};

use super::loader::PluginLoader;
use anyhow::Result;
use rand::Rng;

type RequestContext = oneshot::Sender<Result<ProtocolMessage>>;

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
    Starting,  // Plugin is in the process of starting
    Running,   // Plugin is currently running
    Stopping,  // Plugin is in the process of stopping
    Stopped,   // Plugin has been stopped
    Failed,    // Plugin failed to start
}

pub struct RunningPlugin {
    pub name: String,
    pub manifest: PluginManifest, pub state: PluginState,
    pub process: Option<tokio::process::Child>,
    pub start_time: Option<std::time::Instant>,
    pub restart_count: u32,
    pub last_health_check: Option<std::time::Instant>,
    pub ipc_sender: Option<tokio::sync::mpsc::Sender<TxCmd>>,
    pub auth_code: String,
}

pub struct PluginManager {
    plugin_loader: PluginLoader,
    running_plugins: Arc<RwLock<HashMap<String, RunningPlugin>>>,
    shutdown_signal: tokio::sync::broadcast::Sender<()>,
    inflight: Arc<Mutex<HashMap<uuid::Uuid, RequestContext>>>,
}


pub enum TxCmd {
    SendMessage(ProtocolMessage),
    Shutdown,
}

pub enum RxCmd {
    InitMessage{
        msg: ProtocolMessage,
        tx_cmd_sender: tokio::sync::mpsc::Sender<TxCmd>,
    },
    NormalRecievedMessage(ProtocolMessage),
    Shutdown
}

impl PluginManager {

    pub async fn new(config: PluginHostConfig) -> Result<Self> {
        let mut loader = PluginLoader::new(&config.plugin_directory);

        let _ = loader.scan_plugins().await?;
        
        Ok(Self {
            plugin_loader: loader,
            running_plugins: Arc::new(RwLock::new(HashMap::new())),
            shutdown_signal: config.shutdown_signal,
            inflight: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn handle_plugin_connection(&self, s: Stream, rx_cmd_sender: tokio::sync::mpsc::Sender<RxCmd>) -> Result<()> {
        let plugins = self.running_plugins.clone();
        let inflight = self.inflight.clone();

        let (tx_cmd_sender, mut tx_cmd_receiver) = tokio::sync::mpsc::channel::<TxCmd>(32);

        let mut framed = Framed::new(s, crate::protocol::protocol_frame::ProtocolFrameCodec::new());

        let connection_join_handle =tokio::spawn(async move {
            let initialize_param = crate::InitializeRequest {
                plugin_config: None,
                broker_info: Some(crate::BrokerInfo {
                    version: "1.0".to_string(),
                    node_id: 1.to_string(),
                    cluster_name: "yedmq_cluster".to_string(),
                    properties: None,
                }),
                required_capabilities: vec![],
            };

            let wrap_initialize_param_to_any = prost_types::Any {
                type_url: super::protocol::INIT_REQUEST_TYPE_URL.to_owned(),
                value: initialize_param.encode_to_vec(),
            };

            let init_plugin_request_message = ProtocolMessage {
                id: crate::create_message_id(),
                version: "1.0".to_string(),
                r#type: crate::MessageType::Request.into(),
                timestamp: Some(crate::create_timestamp()),
                source: "plugin_host".to_string(),
                target: "plugin".to_string(),
                method: Some(crate::Method::Initialize.into()),
                params: Some(wrap_initialize_param_to_any),
                result: None,
                error: None,
                metadata: HashMap::new(),
            };

            framed.send(init_plugin_request_message).await.unwrap();

            let mut initialized = false;

            loop {
                tokio::select! {
                    msg = framed.next() => {
                        if let Some(msg) = msg {
                            if let Ok(msg) = msg {
                                let protocol_message = ProtocolMessage::decode(msg.payload);
                                if let Ok(protocol_message) = protocol_message {
                                    match protocol_message.method {
                                        Some(m) if m == crate::Method::Initialize as i32 => {
                                            initialized = true;
                                            let _ = rx_cmd_sender.send(RxCmd::InitMessage{
                                                msg:protocol_message,
                                                tx_cmd_sender: tx_cmd_sender.clone(),
                                            }).await;
                                        },
                                        _ => {
                                            if initialized {
                                                let _ = rx_cmd_sender.send(RxCmd::NormalRecievedMessage(protocol_message)).await;
                                            } else {
                                                warn!("Received message before initialization");
                                            }
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

    async fn start_handle_plugin_rx_cmd(&self, mut rx_cmd_receiver: tokio::sync::mpsc::Receiver<RxCmd>) -> Result<()> {

        let plugins = self.running_plugins.clone();
        let inflight = self.inflight.clone();

        let rx_cmd_join_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = rx_cmd_receiver.recv() => {
                        match msg {
                            Some(RxCmd::InitMessage{msg, tx_cmd_sender}) => {
                                // Handle initialization message
                                info!("Received initialization message: {:?}", msg);
                                let r = msg.result;
                                if let Some(r) = r {
                                    let init_response = crate::InitializeResponse::decode(r.value.as_slice()).unwrap();
                                    let auth_code = init_response.auth_code;
                                    plugins.write().await.iter_mut().for_each(|(_, plugin)| {
                                        if plugin.auth_code == auth_code {
                                            plugin.ipc_sender = Some(tx_cmd_sender.clone());
                                            plugin.state = PluginState::Running;
                                        }
                                    });
                                }
                            },
                            Some(RxCmd::NormalRecievedMessage(msg)) => {
                                // Handle normal received message
                                info!("Received normal message: {:?}", msg);
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
                return Err(anyhow::anyhow!("Socket address already in use: {}", printname));
            }
            listener => listener?,
        };

        loop {
            match listener.accept().await {
                Ok(c) =>  {
                    let _ = self.handle_plugin_connection(c, rx_cmd_sender.clone()).await;
                },
                Err(_) => continue,
            };
        }

    }

    pub async fn start_plugin(&self, name: &str) -> Result<()> {
        let manifest = self.plugin_loader.get_plugin_manifest(name)
            .ok_or_else(|| anyhow::anyhow!("Plugin '{}' not found", name))?.clone();

        let auth_code = generate_auth_code(12);

        let mut command = self.plugin_loader.get_plugin_command(name, &auth_code)?.unwrap();

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



