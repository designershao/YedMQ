pub mod session_actor;
pub mod session_manager_actor;

pub struct WillMessage {
    pub will_topic: String,
    pub will_message: Vec<u8>,
    pub will_qos: u8,
    pub will_retain: bool,
}
