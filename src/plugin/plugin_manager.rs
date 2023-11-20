use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{anyhow, Ok, Result};
use log::{info, warn};
use nom::Err;
use thiserror::Error;
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{error::SendError, Sender},
        RwLock,
    },
};

use crate::{
    plugin::plugin::RegisterHookResponse,
    protocol::v3::{connect::ConnectPacket, publish::PublishPacket},
};

use super::{
    plugin::{ConnectInfo, Plugin, PluginAuthResult, PluginMessage, PluginResponse, RejectResult},
    session_context::SessionContext,
};

pub enum OnConnectAuthResult {
    Pass(String, String),
    Forbidden,
    Error(anyhow::Error),
}

pub enum OnAclCheckResult {
    Pass,
    Forbidden,
    Error(anyhow::Error),
}

#[derive(Error, Debug)]
pub enum PluginManagerError {
    #[error("plugin dir not existed")]
    PluginDirNotExisted,

    #[error("hook {0} no plugin register")]
    NoPluginRegisterHook(String),

    #[error("invalid plugin response")]
    InvalidPluginResponse,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PluginStatus {
    Running,
    Stop,
}

#[derive(Debug)]
pub struct PluginWrapper {
    status: PluginStatus,
    plugin_path: PathBuf,
    plugin_sender: tokio::sync::mpsc::Sender<PluginMessage>,
}

impl PluginWrapper {
    pub async fn stop(&mut self) -> Result<()> {
        self.plugin_sender.send(PluginMessage::Quit).await?;
        self.status = PluginStatus::Stop;
        Ok(())
    }

    pub async fn reload(&mut self) -> Result<()> {
        self.plugin_sender.send(PluginMessage::Quit).await?;
        let plugin = Plugin::new(&self.plugin_path, &tokio::task::LocalSet::new())?;
        self.plugin_sender = plugin;
        self.status = PluginStatus::Running;
        Ok(())
    }

    pub async fn send(&self, msg: PluginMessage) -> Result<()> {
        self.plugin_sender.send(msg).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct PluginSenderWrapper {
    pub sender: tokio::sync::mpsc::Sender<PluginMessage>,
    pub priority: i64,
}

pub struct PluginManager {
    tx: tokio::sync::mpsc::Sender<PluginManagerMessage>,
    hook_sender_table: HashMap<String, BTreeMap<i64, PluginSenderWrapper>>, // key: hook_name  value: sender to plugin
}

#[derive(Debug)]
pub enum PluginManagerMessage {
    GetHookTable(tokio::sync::oneshot::Sender<HashMap<String, BTreeMap<i64, PluginSenderWrapper>>>),
    LoadPlugin(String),
    StartPlugin(String),
    StopPlugin(String),
    GetPlugin(
        String,
        tokio::sync::oneshot::Sender<Option<Arc<RwLock<PluginWrapper>>>>,
    ),
    GetPluginNames(tokio::sync::oneshot::Sender<Vec<String>>),
    Unload(String),
    Quit,
}

impl PluginManager {
    // Call the connection auth hook
    // Call the highest priority plugin get the final decision
    // Lower priority plugin will not be called
    pub async fn call_hook_on_connect_auth(
        &self,
        remote_addr: &String,
        connect_packet: &ConnectPacket,
    ) -> Result<OnConnectAuthResult> {
        if self.hook_sender_table.contains_key("OnConnectAuth") {
            let wrapper = self.hook_sender_table.get("OnConnectAuth").unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let connect_info = ConnectInfo {
                remote_addr: remote_addr.clone(),
                connect_packet: connect_packet.clone(),
            };
            wrapper
                .iter()
                .next_back()
                .unwrap()
                .1
                .sender
                .send(PluginMessage::OnConnectAuth(connect_info, tx))
                .await?;
            match rx.await {
                std::result::Result::Ok(r) => match r {
                    PluginResponse::AuthResult(PluginAuthResult::Reject(
                        RejectResult::Forbidden,
                    )) => return Ok(OnConnectAuthResult::Forbidden),
                    PluginResponse::AuthResult(PluginAuthResult::Reject(RejectResult::Error(
                        e,
                    ))) => {
                        return Ok(OnConnectAuthResult::Error(e));
                    }
                    PluginResponse::AuthResult(PluginAuthResult::Pass(tenant_id, user_id)) => {
                        return Ok(OnConnectAuthResult::Pass(tenant_id, user_id));
                    }
                    _ => {
                        warn!("invalid OnConnectAuth response");
                        return Err(anyhow!(PluginManagerError::InvalidPluginResponse));
                    }
                },
                std::result::Result::Err(_) => {
                    panic!("call hook on connect auth plugin receiver error")
                }
            };
        } else {
            Err(anyhow!(PluginManagerError::NoPluginRegisterHook(
                "OnConnectAuth".to_string()
            )))
        }
    }

    // Call the subscribe acl check hook
    // Call the highest priority plugin get the final decision
    // Lower priority plugin will not be called
    pub async fn call_hook_on_subscribe_acl_check(
        &self,
        session_ctx: &SessionContext,
        topic: &String,
        qos: i32,
    ) -> Result<bool> {
        if self.hook_sender_table.contains_key("OnSubscribeACLCheck") {
            let wrapper = self.hook_sender_table.get("OnConnectAuth").unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel();
            wrapper
                .into_iter()
                .next_back()
                .unwrap()
                .1
                .sender
                .send(PluginMessage::OnSubscribeACLCheck(
                    session_ctx.clone(),
                    topic.clone(),
                    qos,
                    tx,
                ))
                .await?;
            match rx.await {
                std::result::Result::Ok(r) => match r {
                    PluginResponse::ACLCheckResult(result) => match result {
                        super::plugin::PluginACLCheckResult::Pass => return Ok(true),
                        super::plugin::PluginACLCheckResult::Reject(reason) => match reason {
                            RejectResult::Forbidden => return Ok(false),
                            RejectResult::Error(e) => return Err(e),
                        },
                    },
                    _ => {
                        warn!("invalid OnSubscribeACLCheck response");
                        return Err(anyhow!(PluginManagerError::InvalidPluginResponse));
                    }
                },
                std::result::Result::Err(_) => panic!("call hook on subscribe acl check error"),
            };
        } else {
            Err(anyhow!(PluginManagerError::NoPluginRegisterHook("OnSubscribeACLCheck".to_string())))
        }
    }

    // Call the publish hook
    pub async fn call_hook_on_publish(&self, session_ctx: &SessionContext, packet: &PublishPacket) -> Result<()> {
        if self.hook_sender_table.contains_key("OnPublish") {
            for (_, wrapper) in self.hook_sender_table.get("OnPublish").unwrap() {
                wrapper
                    .sender
                    .send(PluginMessage::OnPublish(session_ctx.clone(), packet.clone()))
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn new(plugin_path: &String) -> Result<PluginManager> {
        let path = PathBuf::from(plugin_path);
        if !path.exists() {
            return Err(anyhow!(PluginManagerError::PluginDirNotExisted));
        } else {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();

            let (tx, mut rx) = tokio::sync::mpsc::channel(1);

            std::thread::spawn(move || {
                let local_set = tokio::task::LocalSet::new();
                local_set.spawn_local(async move {
                    let mut plugin_table = HashMap::new();
                    let paths = path.read_dir().unwrap();
                    for path in paths {
                        let local_set = tokio::task::LocalSet::new();
                        let path = path.unwrap().path();
                        let plugin = Plugin::new(&path, &local_set);
                        if let Err(err) = plugin {
                            warn!(
                                "load plugin error skip ! path {} error: {}",
                                path.to_str().unwrap(),
                                err
                            );
                        } else {
                            let plugin_tx = plugin.unwrap();
                            let (plugin_name_tx, plugin_name_rx) = tokio::sync::oneshot::channel();
                            plugin_tx
                                .send(PluginMessage::GetPluginName(plugin_name_tx))
                                .await
                                .unwrap();
                            let plugin_name = plugin_name_rx.await.unwrap();
                            let plugin_wrapper = PluginWrapper {
                                status: PluginStatus::Running,
                                plugin_path: path,
                                plugin_sender: plugin_tx,
                            };
                            plugin_table.insert(plugin_name, Arc::new(RwLock::new(plugin_wrapper)));
                        }
                    }
                    loop {
                        let msg = rx.recv().await;
                        match msg {
                            Some(PluginManagerMessage::Quit) => {
                                break;
                            }
                            Some(PluginManagerMessage::LoadPlugin(plugin_path)) => {
                                let local_set = tokio::task::LocalSet::new();
                                let plugin =
                                    Plugin::new(&PathBuf::from(plugin_path.clone()), &local_set);
                                if let Err(err) = plugin {
                                    warn!(
                                        "load plugin error skip ! path {} error: {}",
                                        path.to_str().unwrap(),
                                        err
                                    );
                                } else {
                                    let plugin_tx = plugin.unwrap();
                                    let (plugin_name_tx, plugin_name_rx) =
                                        tokio::sync::oneshot::channel();
                                    plugin_tx
                                        .send(PluginMessage::GetPluginName(plugin_name_tx))
                                        .await
                                        .unwrap();
                                    let plugin_name = plugin_name_rx.await.unwrap();
                                    let plugin_wrapper = PluginWrapper {
                                        status: PluginStatus::Running,
                                        plugin_path: PathBuf::from(plugin_path),
                                        plugin_sender: plugin_tx,
                                    };
                                    plugin_table
                                        .insert(plugin_name, Arc::new(RwLock::new(plugin_wrapper)));
                                }
                            }
                            Some(PluginManagerMessage::StartPlugin(plugin_name)) => {
                                if let Some(plugin) = plugin_table.get_mut(&plugin_name) {
                                    if let Err(e) = plugin.write().await.reload().await {
                                        warn!("plugin {} reload error: {}", plugin_name, e);
                                    }
                                }
                            }
                            Some(PluginManagerMessage::StopPlugin(plugin_name)) => {
                                if let Some(plugin) = plugin_table.get_mut(&plugin_name) {
                                    if let Err(e) = plugin.write().await.stop().await {
                                        warn!("plugin {} stop error: {}", plugin_name, e);
                                    }
                                }
                            }
                            Some(PluginManagerMessage::GetPlugin(plugin_name, tx)) => {
                                if let Some(plugin) = plugin_table.get(&plugin_name) {
                                    tx.send(Some(plugin.clone())).unwrap();
                                } else {
                                    tx.send(None).unwrap();
                                }
                            }
                            Some(PluginManagerMessage::Unload(plugin_name)) => {
                                if let Some(sender) = plugin_table.get(&plugin_name) {
                                    if let Err(e) =
                                        sender.read().await.send(PluginMessage::Quit).await
                                    {
                                        warn!("plugin {} quit error: {}", plugin_name, e);
                                    }
                                    plugin_table.remove(&plugin_name);
                                }
                            }
                            Some(PluginManagerMessage::GetPluginNames(tx)) => {
                                let i = plugin_table.keys().map(|k| k.clone()).collect();
                                tx.send(i).unwrap();
                            }
                            Some(PluginManagerMessage::GetHookTable(tx)) => {
                                let mut result: HashMap<
                                    String,
                                    BTreeMap<i64, PluginSenderWrapper>,
                                > = HashMap::new();

                                for (plugin_name, plugin) in plugin_table.iter() {
                                    let plugin = plugin.read().await;
                                    let (tx, rx) = tokio::sync::oneshot::channel();
                                    plugin
                                        .plugin_sender
                                        .send(PluginMessage::GetRegisterHooks(tx))
                                        .await;
                                    let hook_vec = match rx.await {
                                        std::result::Result::Ok(v) => v,
                                        std::result::Result::Err(_) => {
                                            warn!("call hook GetRegisterHooks error");
                                            RegisterHookResponse {
                                                hook_names: vec![],
                                                priority: 0,
                                            }
                                        }
                                    };
                                    for hook in hook_vec.hook_names {
                                        if !result.contains_key(&hook) {
                                            result.insert(hook.clone(), BTreeMap::new());
                                        }
                                        result.get_mut(&hook).unwrap().insert(
                                            hook_vec.priority,
                                            PluginSenderWrapper {
                                                priority: hook_vec.priority,
                                                sender: plugin.plugin_sender.clone(),
                                            },
                                        );
                                    }
                                }
                                tx.send(result).unwrap();
                            }
                            None => {
                                break;
                            }
                        }
                    }
                });
                rt.block_on(local_set);
            });
            let hook_table = Self::get_hook_table(tx.clone()).await?;
            return Ok(PluginManager {
                tx,
                hook_sender_table: hook_table,
            });
        }
    }

    async fn get_hook_table(
        plugin_manager: Sender<PluginManagerMessage>,
    ) -> Result<HashMap<String, BTreeMap<i64, PluginSenderWrapper>>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        plugin_manager
            .send(PluginManagerMessage::GetHookTable(tx))
            .await?;
        Ok(rx.await?)
    }

    pub async fn get_plugin_names(&self) -> Result<Vec<String>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(PluginManagerMessage::GetPluginNames(tx))
            .await?;
        Ok(rx.await?)
    }

    pub async fn load(&self, plugin_path: &String) -> Result<()> {
        self.tx
            .send(PluginManagerMessage::LoadPlugin(plugin_path.clone()))
            .await?;
        Ok(())
    }

    pub async fn get_plugin(
        &self,
        plugin_name: &String,
    ) -> Result<Option<Arc<RwLock<PluginWrapper>>>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(PluginManagerMessage::GetPlugin(plugin_name.clone(), tx))
            .await?;
        Ok(rx.await?)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{plugin::plugin_manager::PluginStatus, protocol::{v3::{connect::{VariableHeader, Payload, ConnectPacket}, fixed_header::FixHeader}, PacketType}};

    use super::PluginManager;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_manager_init() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string())
            .await
            .unwrap();

        let plugin_names = plugin_manager.get_plugin_names().await.unwrap();

        assert_eq!(1, plugin_names.len());
        assert_eq!(true, plugin_names.contains(&"demo_plugin".to_string()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_manager_load() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string())
            .await
            .unwrap();
        plugin_manager
            .load(
                &plugin_path
                    .join("demo_plugin")
                    .to_str()
                    .unwrap()
                    .to_string(),
            )
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_status_get() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string())
            .await
            .unwrap();

        let plugin = plugin_manager
            .get_plugin(&"demo_plugin".to_string())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(PluginStatus::Running, plugin.read().await.status);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_manager_get_hook_table() {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string())
            .await
            .unwrap();

        assert_eq!(2, plugin_manager.hook_sender_table.len());
        assert!(plugin_manager
            .hook_sender_table
            .contains_key("OnSubscribeACLCheck"));
        assert!(plugin_manager
            .hook_sender_table
            .contains_key("OnConnectAuth"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_plugin_manager_call_on_connect_auth() {
        let variable_header = VariableHeader {
                protocol_name: "MQTT".to_string(),
                protocol_level: 0x04,
                username_flag: true,
                password_flag: true,
                will_retain: true,
                will_qos: 1,
                will_flag: true,
                clean_session: true,
                keep_alive: 0,
            };
        let payload = Payload {
            client_identifier: "MQTT".to_string(),
            will_topic: Some("MQTT".to_string()),
            will_message: Some("MQTT".to_string()),
            username: Some("MQTT".to_string()),
            password: Some("MQTT".to_string()),
        };

        let fix_header = FixHeader{
                packet_type: PacketType::CONNECT,
                qos: None,
                retain: None,
                dup: None,
                remaining_length: variable_header.get_length() + payload.get_length(),
            };

        let connect_packet = ConnectPacket {
            fix_header,
            variable_header,
            payload
        };
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string())
            .await
            .unwrap();

        let result = plugin_manager.call_hook_on_connect_auth(&"127.0.0.1".to_string(), &connect_packet).await;

        match result {
            Ok(r) => {
                match r {
                    crate::plugin::plugin_manager::OnConnectAuthResult::Pass(tenant_id, user_id) => {
                        assert_eq!("t-123".to_string(), tenant_id);
                        assert_eq!("123".to_string(), user_id);
                    },
                    crate::plugin::plugin_manager::OnConnectAuthResult::Forbidden => assert!(false),
                    crate::plugin::plugin_manager::OnConnectAuthResult::Error(_) => assert!(false),
                }
            }
            Err(_) => {
                assert!(false)
            }
        }

    }
}
