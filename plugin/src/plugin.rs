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


// Represent client topic action
pub enum Action {

    // Subscribe topic
    Subscribe,
    
    // Publish to topic
    Publish

}

// The plugin trait
// The plugin must implement the following methods
//
// # Example
// ```rust
// use yedmq_plugin::plugin::Plugin;
// use yedmq_plugin::{plugin::{AuthenticationResult, AuthenticationResultValue, AuthorizationResult}, register_plugin};
// pub struct ExamplePlugin {}
// impl ExamplePlugin {
//    pub fn new(_context: yedmq_plugin::context::Context) -> Result<ExamplePlugin, anyhow::Error> {
//        Ok(
//            ExamplePlugin {}
//        )
//    }
// }
// impl yedmq_plugin::plugin::Plugin for ExamplePlugin {
//
//    fn on_activate(&mut self) -> Result<()> {
//        println!("example plugin on_activate example-plugin");
//        Ok(())
//    }
//
//    fn on_deactivate(&self) -> Result<()> {
//        println!("example plugin on_deactivate");
//        Ok(())
//    }
//
//    fn connect_authenticate(&self, _packet: &yedmq_mqtt::v3::connect::ConnectPacket) -> Result<AuthenticationResult> {
//        return Ok(AuthenticationResult::Result(AuthenticationResultValue::Success("tenant_id".into())));
//    }
//
//    fn authorizate_acl_check(&self, _client: &yedmq_plugin::plugin::Client, _topic: &String, _action: yedmq_plugin::plugin::Action) -> Result<AuthorizationResult> {
//        return Ok(AuthorizationResult::Result(true));
//    }
//
//    fn on_publish(&self, client: &yedmq_plugin::plugin::Client, packet: &yedmq_mqtt::v3::publish::PublishPacket) {
//        println!("client {} receive publish packet {:?}", client.client_identifier, packet)
//    }
//    
//    fn on_disconnect(&self, client: &yedmq_plugin::plugin::Client) {
//
//        println!("client {} disconnect", client.client_identifier)
//
//    }
//
// }
//
// impl Drop for ExamplePlugin {
//
//    fn drop(&mut self) {
//        println!("example plugin drop");
//    }
//
// }
// register_plugin!(ExamplePlugin, ExamplePlugin::new);
// ```
pub trait Plugin: Any + Send + Sync {

    // Called when the plugin is activated
    fn on_activate(&mut self) -> Result<()>;

    // Called when the plugin is deactivated
    fn on_deactivate(&self) -> Result<()>;

    // Called when the client connect
    fn connect_authenticate(&self, packet: &yedmq_mqtt::v3::connect::ConnectPacket) -> Result<AuthenticationResult>;

    // Called when the client publish
    fn on_publish(&self, client: &Client, packet: &yedmq_mqtt::v3::publish::PublishPacket);

    // Called when the client disconnect
    fn on_disconnect(&self, client: &Client);

    // Called when the client publish or subscribe
    fn authorizate_acl_check(&self, client: &Client, topic: &String, action: Action) -> Result<AuthorizationResult>;

}

#[repr(C)]
pub struct RegisterPluginResult {

    pub plugin: *mut dyn Plugin,

    pub error_code: c_int,

    pub error_msg: *mut std::os::raw::c_char

}

// Null plugin
// Do not use it, it is used by YedMQ plugin host.
pub struct NullPlugin {}

impl Plugin for NullPlugin {
    fn on_activate(&mut self) -> Result<()> {
        Ok(())
    }

    fn on_deactivate(&self) -> Result<()> {
        Ok(())
    }

    fn connect_authenticate(&self, _packet: &yedmq_mqtt::v3::connect::ConnectPacket) -> Result<AuthenticationResult> {
        Err(PluginError::PluginHookNotImplement().into())
    }

    fn on_publish(&self, _client: &Client, _packet: &yedmq_mqtt::v3::publish::PublishPacket) {
    }

    fn on_disconnect(&self, _client: &Client) {
    }

    fn authorizate_acl_check(&self, _client: &Client, _topic: &String, _action: Action) -> Result<AuthorizationResult> {
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