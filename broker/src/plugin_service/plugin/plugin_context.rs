use std::collections::HashMap;

use rune::{runtime::Function, Any};
use samoye_mqtt::v3::publish::PublishPacket;
use thiserror::Error;

pub struct SessionContext {
    pub tenant_id: String,
    pub client_identifier: String,
    pub username: String,
    pub remote_addr: String
}