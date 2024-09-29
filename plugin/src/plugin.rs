use std::any::Any;
use anyhow::Result;

pub trait Plugin: Any + Send + Sync {

    fn on_activate();

    fn on_deactivate();

    fn connect_authenticate() -> Result<AuthenticationResult>;

    fn publish_authorizate(client: &Client, packet: &samoye_mqtt::v3::publish::PublishPacket) -> Result<bool>;

    fn subscribe_authorizate(client: &Client, packet: &samoye_mqtt::v3::subscribe::SubscribePacket) -> Result<SubscribeAuthorizationResult>;

    fn on_publish(client: &Client, packet: &samoye_mqtt::v3::publish::PublishPacket);

    fn on_disconnect(client: &Client);

}

pub struct Client {

    pub client_identifier: String,

    pub properties: ClientProperties

}

pub struct ClientProperties {

    pub username:String,

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

pub enum AuthenticationResult {

    Success,

    Fail(ConnectReturnCode)

}

pub struct TopicFilter {

    pub topic_name: String,

    pub qos: u8

}


pub enum SubscribeReturnCode {

    MaxQosMostOnce,

    MaxQosLeastOnce,

    MaxQosExactlyOnce,
    
    Failure,

    Invalid

}

pub struct SubscribeAuthorizationResult {

    pub return_code: Vec<SubscribeReturnCode>

}
