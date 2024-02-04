use std::collections::HashMap;

use rune::{runtime::Function, Any};
use crate::protocol::v3::publish::PublishPacket;
use thiserror::Error;

pub struct SessionContext {
    pub tenant_id: String,
    pub client_identifier: String,
    pub username: String,
    pub remote_addr: String
}

#[derive(Error, Debug)]
pub enum CallPluginError {

    #[error("hook not register: {0}")]
    HookNotRegister(String)
}

// Connect the client info
#[derive(Debug, Default, Any, Clone)]
#[rune(item=::samoye::hook)]
pub struct ConnectInfo {
    pub username: Option<String>,
    pub password: Option<String>,
    pub remote_addr: String,
}

// Connected the client info
#[derive(Debug, Default, Any, Clone)]
#[rune(item=::samoye::hook)]
pub struct ClientInfo {
    pub tenant_id: String,
    pub client_identifier: String,
    pub username: String,
    pub remote_addr: String,
}



#[derive(Debug, Clone, PartialEq, Eq, Hash, Any)]
#[rune(item=::samoye::hook)]
pub enum Hooks {

    #[rune(constructor)]
    OnConnect,

    #[rune(constructor)]
    OnConnectAuth,

    #[rune(constructor)]
    OnTopicPermissionCheck,

    #[rune(constructor)]
    OnPublish,

    #[rune(constructor)]
    OnDisconnect,
}

#[derive(Any, Clone)]
#[rune(item=::samoye::hook)]
pub struct TopicInfo {
    #[rune(get)]
    pub topic_filter: String,

    #[rune(get)]
    pub qos: i64,

    #[rune(get)]
    pub operation: TopicOperation
}

#[derive(Default)]
pub struct TopicPermissionBuilder {
    topic_filter: String,
    qos: i64,
    permission: PermissionType,
    operation: TopicOperation
}

impl TopicPermissionBuilder {
    pub fn new(topic_filter: String, qos: i64, permission: PermissionType, operation: TopicOperation) -> Self {
        TopicPermissionBuilder {
            topic_filter,
            qos,
            permission,
            operation
        }
    }

    pub fn topic_filter(mut self, topic_filter: String) -> Self {
        self.topic_filter = topic_filter;
        self
    }

    pub fn qos(mut self, qos: i64) -> Self {
        self.qos = qos;
        self
    }

    pub fn permission(mut self, permission: PermissionType) -> Self {
        self.permission = permission;
        self
    }

    pub fn operation(mut self, operation: TopicOperation) -> Self {
        self.operation = operation;
        self
    }

    pub fn build(self) -> TopicPermission {
        TopicPermission {
            topic_filter: self.topic_filter,
            qos: self.qos,
            permission: self.permission,
            operation: self.operation
        }
    }
}

#[derive(Any)]
pub struct TopicPermission {
    pub topic_filter: String,
    pub qos: i64,
    pub permission: PermissionType,
    pub operation: TopicOperation
}

impl TopicPermission {
    pub fn builder() -> TopicPermissionBuilder {
        TopicPermissionBuilder::default()
    }
}

#[derive(Default, Debug, Any, Clone)]
#[rune(item=::samoye::hook)]
pub enum TopicOperation {
    #[default]
    #[rune(constructor)]
    Publish,

    #[rune(constructor)]
    Subscribe,
}

#[derive(Default, Any, Clone)]
#[rune(item=::samoye::hook)]
pub enum PermissionType {
    #[default]
    #[rune(constructor)]
    Allow,

    #[rune(constructor)]
    Deny
}

#[derive(Debug, Any, Clone)]
#[rune(item=::samoye::hook)]
pub enum Authorization {

    #[rune(constructor)]
    Allow,

    #[rune(constructor)]
    Deny
}


#[derive(Debug, Any, Clone)]
#[rune(item=::samoye::hook)]
pub enum Authentication {
    #[rune(constructor)]
    Allow(#[rune(get)] String),

    #[rune(constructor)]
    Deny(#[rune(get)] String, #[rune(get)] ReturnCode)
}

#[derive(Debug, Any, Clone)]
#[rune(item=::samoye::hook)]
pub enum ReturnCode {
    #[rune(constructor)]
    RefusedUnsupportProtocolVersion,

    #[rune(constructor)]
    RefusedInvalidClientIdentifier,

    #[rune(constructor)]
    RefusedInteralError,

    #[rune(constructor)]
    RefusedInvalidUsernameOrPassword,

    #[rune(constructor)]
    RefusedNotAuthorized,
}



#[derive(Any)]
#[rune(item=::samoye::hook)]
pub struct Context {
    hook_table: HashMap<Hooks, Function>,
}

impl Context {
    pub fn new() -> Self {
        Context {
            hook_table: HashMap::new()
        }
    }

    #[rune::function]
    pub fn hook_subscribe(&mut self, hook_type: Hooks, f: Function) {
        self.hook_table.insert(hook_type, f);
    }

    //The function is called during client connection authentication.
    pub fn on_connect_auth(&mut self, connect_info: ConnectInfo) -> anyhow::Result<Authentication> {
        if self.hook_table.contains_key(&Hooks::OnConnectAuth) {
            let i = self.hook_table.get(&Hooks::OnConnectAuth).unwrap().call((connect_info,)).unwrap();
            rune::from_value(i)?
        } else {
            Err(anyhow::anyhow!(CallPluginError::HookNotRegister("OnConnectAuth".into())))
        }
    }

    // The function is called when the broker receives a message
    pub async fn on_publish(&mut self, client_info: ClientInfo, packet: PublishPacket) -> anyhow::Result<()> {
        let _= self.hook_table.get(&Hooks::OnPublish).unwrap().async_send_call::<(ClientInfo, PublishPacket), ()>((client_info, packet)).await;
        Ok(())
    }

    // The function is called for permission checking when subscribing to MQTT topics or sending messages on a specific topic.
    pub async fn on_topic_permission_check(&mut self,client_info: ClientInfo, topic_info:TopicInfo) -> anyhow::Result<Authorization> {
        let i = self.hook_table.get(&Hooks::OnTopicPermissionCheck).unwrap().call((client_info, topic_info,)).unwrap();
        rune::from_value(i)?
    }

}
