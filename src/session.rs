use std::{sync::{RwLock, Arc, mpsc::Sender}, collections::{HashMap, VecDeque}, time::Duration};

use crate::{connection::Connection, protocol::MqttPacketV3};
use anyhow::Result;
use tokio::{sync::mpsc::Receiver, select};

// Represent mqtt session
pub struct Session {
    client_identifier: String,
    tenant_identifier: String,
    connection: Arc<RwLock<Connection>>,
    subscription_topics: Arc<RwLock<Vec<String>>>,
    qos2_pub_rec_resend_queue: Arc<RwLock<HashMap<u16, ResendPacketItem>>>,
    qos2_rec_resend_task_quit_sender: Sender<()>
}


struct ResendPacketItem {
    packet: MqttPacketV3,
    resend_time: u64,
}

impl Session {

    fn close(&self) {
        self.qos2_rec_resend_task_quit_sender.send(()); // notify qos2 rec resend task quit
        todo!("close session should close connection")
    }

    fn remove_from_qos2_pub_rec_resend_queue(&self, packet_id: u16) {
        let mut qos2_pub_rec_resend_queue = self.qos2_pub_rec_resend_queue.write().unwrap();
        qos2_pub_rec_resend_queue.remove(&packet_id);
    }

    // Qos2 PubRec resend task
    // When session write pubrec packet to the underlying stream, the broker should wait the pubrel packet
    // if reach the wait pubrel timeout, session should rewrite the pubrec which set dup to 1
    async fn run_qos2_rec_resend_task(&self, mut quit_receiver: Receiver<()>) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            select! {
                _ = interval.tick() => {
                    for (_, value) in self.qos2_pub_rec_resend_queue.read().unwrap().iter() {
                        if value.resend_time < std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)?.as_secs() {
                            self.write(&value.packet).await?
                        }
                    }
                },
                _ = quit_receiver.recv() => {
                    break // when session close disable resend task
                }
            }
        }
        Ok(())
    }

    // Write a single packet to the underlying stream.
    pub async fn write(&self, packet: &MqttPacketV3) -> Result<()> {
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