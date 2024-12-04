use anyhow::Result;
use samoye_plugin::{plugin::{AuthenticationResult, AuthenticationResultValue, AuthorizationResult}, register_plugin};

pub struct ExamplePlugin {

}

impl ExamplePlugin {
    pub fn new() -> Self {
        ExamplePlugin {}
    }
}

impl samoye_plugin::plugin::Plugin for ExamplePlugin {
    fn on_activate(&self) -> Result<()> {
        println!("example plugin on_activate");
        Ok(())
    }

    fn on_deactivate(&self) -> Result<()> {
        println!("example plugin on_deactivate");
        Ok(())
    }

    fn connect_authenticate(&self, _packet: &samoye_mqtt::v3::connect::ConnectPacket) -> Result<AuthenticationResult> {
        return Ok(AuthenticationResult::Result(AuthenticationResultValue::Success("tenant_id".into())));
    }

    fn authorizate_acl_check(&self, _client: &samoye_plugin::plugin::Client, _topic: &String, _action: samoye_plugin::plugin::Action) -> Result<AuthorizationResult> {
        return Ok(AuthorizationResult::Result(true));
    }

    fn on_publish(&self, client: &samoye_plugin::plugin::Client, packet: &samoye_mqtt::v3::publish::PublishPacket) {
        println!("client {} receive publish packet {:?}", client.client_identifier, packet)
    }

    fn on_disconnect(&self, client: &samoye_plugin::plugin::Client) {
        println!("client {} disconnect", client.client_identifier)
    }
}

impl Drop for ExamplePlugin {
    fn drop(&mut self) {
        println!("example plugin drop");
    }
}

register_plugin!(ExamplePlugin, ExamplePlugin::new);