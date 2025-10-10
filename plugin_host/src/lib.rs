pub mod plugin_manager;
pub mod loader;
pub mod protocol;
pub mod hook;
pub mod plugin_host_config;

use prost_types::Timestamp;
use uuid::Uuid;
use chrono::Utc;

pub fn create_timestamp() -> Timestamp {
    let now = Utc::now();
    Timestamp {
        seconds: now.timestamp(),
        nanos: now.timestamp_subsec_nanos() as i32,
    }
}

pub fn create_message_id() -> String {
    Uuid::new_v4().to_string()
}