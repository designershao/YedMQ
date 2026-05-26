pub mod session_actor;
pub mod session_actor_map_service;
pub mod session_actor_map_storage;
pub mod session_manager_actor;
pub mod session_registry;
pub mod session_state_service;
pub mod session_state_storage;

use yedmq_mqtt::packet::{Properties, ProtocolVersion};

#[derive(Clone)]
pub struct WillMessage {
    pub will_topic: String,
    pub will_message: Vec<u8>,
    pub will_qos: u8,
    pub will_retain: bool,
    pub protocol_version: ProtocolVersion,
    pub properties: Properties,
}
