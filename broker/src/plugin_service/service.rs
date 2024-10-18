use std::{collections::BTreeMap, path::PathBuf, sync::{Arc, RwLock}};

use crate::{plugin_service::plugin::plugin_context::{Authentication, PermissionType}};
use samoye_mqtt::v3::publish::PublishPacket;

use super::plugin::{plugin_host::PluginHost, plugin_context::{self, Authorization, CallPluginError, ClientInfo, ConnectInfo, TopicInfo, TopicPermission}};
use log::{warn, info};
use thiserror::Error;
use anyhow::{anyhow, Ok};
use tokio::runtime::Builder;

pub enum PluginServiceMessage {
    OnConnectAuth(ConnectInfo, tokio::sync::oneshot::Sender<anyhow::Result<Authentication>>),
    OnPublish(ClientInfo, PublishPacket),
    OnTopicPermissionCheck(ClientInfo, TopicInfo, tokio::sync::oneshot::Sender<anyhow::Result<Authorization>>),
    Quit,
}

#[derive(Error, Debug)]
pub enum PluginServiceError {
    #[error("plugin dir not existed")]
    PluginDirNotExisted,
}

