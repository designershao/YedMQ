use std::ops::Sub;

use anyhow::Result;
use samoye_plugin::{plugin::SubscribeReturnCode, register_plugin};

pub struct ExamplePlugin {

}

impl ExamplePlugin {
    pub fn new() -> Self {
        ExamplePlugin {}
    }
}

impl samoye_plugin::plugin::Plugin for ExamplePlugin {
    fn on_activate(&self) {
        println!("example plugin on_activate");
    }

    fn on_deactivate(&self) {
        println!("example plugin on_deactivate");
    }

    fn connect_authenticate(&self, packet: &samoye_mqtt::v3::connect::ConnectPacket) -> Result<samoye_plugin::plugin::AuthenticationResult> {
        return Ok(samoye_plugin::plugin::AuthenticationResult::Success("tenant_id".into()))
    }

    fn publish_authorizate(&self, client: &samoye_plugin::plugin::Client, packet: &samoye_mqtt::v3::publish::PublishPacket) -> Result<bool> {
        return Ok(true)
    }

    fn subscribe_authorizate(&self, client: &samoye_plugin::plugin::Client, packet: &samoye_mqtt::v3::subscribe::SubscribePacket) -> Result<samoye_plugin::plugin::SubscribeAuthorizationResult> {
        let return_code = SubscribeReturnCode::MaxQosMostOnce;
        return Ok(samoye_plugin::plugin::SubscribeAuthorizationResult{
            return_code: vec![return_code]
        })
    }

    fn on_publish(&self, client: &samoye_plugin::plugin::Client, packet: &samoye_mqtt::v3::publish::PublishPacket) {
        println!("client {} receive publish packet {:?}", client.client_identifier, packet)
    }

    fn on_disconnect(&self, client: &samoye_plugin::plugin::Client) {
        println!("client {} disconnect", client.client_identifier)
    }
}

register_plugin!(ExamplePlugin, ExamplePlugin::new);