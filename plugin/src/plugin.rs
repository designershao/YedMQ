use std::any::Any;
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

    fn on_activate(&self);

    fn on_deactivate(&self);

    fn connect_authenticate(&self, packet: &samoye_mqtt::v3::connect::ConnectPacket) -> Result<AuthenticationResult>;

    fn on_publish(&self, client: &Client, packet: &samoye_mqtt::v3::publish::PublishPacket);

    fn on_disconnect(&self, client: &Client);

    fn authorizate_acl_check(&self, client: &Client, topic: &String, action: Action) -> Result<AuthorizationResult>;

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
