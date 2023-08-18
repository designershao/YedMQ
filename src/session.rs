use std::{sync::{RwLock, Arc}, collections::HashMap};

use crate::{connection::Connection, protocol::MqttPacketV3};
use anyhow::Result;

// Represent mqtt session
pub struct Session {
    client_identifier: String,
    tenant_identifier: String,
    connection: Arc<RwLock<Connection>>,
    subscription_topics: Arc<RwLock<Vec<String>>>,
}

impl Session {
    pub async fn write(&self, packet: MqttPacketV3) -> Result<()> {
        self.connection.write().unwrap().write_packet(packet).await?;
        Ok(())
    }
}

pub struct SessionManager {
    session_table: HashMap<String, Arc<Session>>,
    tenant_id: String
}

impl SessionManager {

}