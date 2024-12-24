use std::{any::Any, ffi::c_int};
use anyhow::Result;
use thiserror::Error;


#[derive(Error, Debug)]
pub enum PluginError {

    #[error("plugin hook not implement")]
    PluginHookNotImplement(),

    #[error("plugin hook error: {0}")]
    PluginHookExecutionError(#[from] anyhow::Error)

}


pub enum Action {

    Subscribe,
    
    Publish

}

pub trait Plugin: Any + Send + Sync {

    fn on_activate(&mut self) -> Result<()>;

    fn on_deactivate(&self) -> Result<()>;

    fn connect_authenticate(&self, packet: &yedmq_mqtt::v3::connect::ConnectPacket) -> Result<AuthenticationResult>;

    fn on_publish(&self, client: &Client, packet: &yedmq_mqtt::v3::publish::PublishPacket);

    fn on_disconnect(&self, client: &Client);

    fn authorizate_acl_check(&self, client: &Client, topic: &String, action: Action) -> Result<AuthorizationResult>;

}

#[repr(C)]
pub struct RegisterPluginResult {

    pub plugin: *mut dyn Plugin,

    pub error_code: c_int,

    pub error_msg: *mut std::os::raw::c_char

}

pub struct NullPlugin {}

impl Plugin for NullPlugin {
    fn on_activate(&mut self) -> Result<()> {
        Ok(())
    }

    fn on_deactivate(&self) -> Result<()> {
        Ok(())
    }

    fn connect_authenticate(&self, packet: &yedmq_mqtt::v3::connect::ConnectPacket) -> Result<AuthenticationResult> {
        Err(PluginError::PluginHookNotImplement().into())
    }

    fn on_publish(&self, client: &Client, packet: &yedmq_mqtt::v3::publish::PublishPacket) {
    }

    fn on_disconnect(&self, client: &Client) {
    }

    fn authorizate_acl_check(&self, client: &Client, topic: &String, action: Action) -> Result<AuthorizationResult> {
        Err(PluginError::PluginHookNotImplement().into())
    }
}

pub struct Client {

    pub tenant_id: String,

    pub client_identifier: String,

    pub properties: ClientProperties,

    pub socket_addr: std::net::SocketAddr
}

pub struct ClientProperties {

    pub username: Option<String>,

    pub clean_session: bool,

    pub will_retain: bool,

    pub will_topic: Option<String>,

    pub will_message: Option<Vec<u8>>,

}

pub enum ConnectReturnCode {

    ConnectAccepted,

    ConnectionForbidenUnSupportMqttVersion,

    ConnectionForbidenInvalidClientIdentifier,

    ConnectionForbidenServerUnavailable,

    ConnectionForbidenUnsupportUsernameOrPasswordFormat,

    ConnectionForbidenUnauth

}

pub enum AuthenticationResultValue {

    Success(String), // Allow connect with tenant id

    Fail(ConnectReturnCode) // Deny connect with connect return code

}

pub enum AuthenticationResult {

    Result(AuthenticationResultValue), // Direct return the authorization value

    Next() // Call next plugin which ordered by priority

}

pub enum AuthorizationResult {

    Result(bool), // direct return the authorization value

    Next(), // Call next plugin which ordered by priority

}

pub struct TopicFilter {

    pub topic_name: String,

    pub qos: u8

}
