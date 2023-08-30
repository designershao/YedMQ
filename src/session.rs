use std::{sync::{Arc}, collections::{HashMap, VecDeque}, time::Duration};

use crate::{connection::Connection, protocol::{MqttPacketV3, v3::{publish::PublishPacket, pubcomp::PubCompPacket, pubrec::PubRecPacket, pubrel::{self, PubRelPacket}}}};
use anyhow::Result;
use tokio::{sync::mpsc::{Receiver, Sender}, select};
use tokio::sync::{RwLock};

const RESEND_DURATION_TIME: u64 = 10;

// Represent mqtt session
pub struct Session {
    client_identifier: String,
    tenant_identifier: String,
    connection: Option<Arc<RwLock<Connection>>>,
    subscription_topics: Arc<RwLock<Vec<String>>>,
    qos_state_table: Arc<RwLock<HashMap<u16, QosPacketItem>>>,
    qos_resend_task_quit_sender: Sender<()>,
    qos_tx_publish_packet_cache: RwLock<HashMap<u16, PublishPacket>>,
    qos_rx_publish_packet_cache: RwLock<HashMap<u16, PublishPacket>>
}

#[derive(PartialEq, Eq, Debug)]
enum QosItemState {
    WaitPubrel,
    WaitPubcomp,
    WaitPubrec,
    WaitPuback,
    Finish // all qos process has complete
}

enum QosType{
    Qos1,
    Qos2
}

// Represent the qos packet status
struct QosPacketItem {
    qos_type: QosType,
    pub resend_time: u64,
    pub packet_identifier: u16,
    pub state: QosItemState,
    pub resend_packet: Option<MqttPacketV3>,
}

impl QosPacketItem {

    fn new_qos_2_rx_state(packet_identifier: u16, resend_time: u64) -> QosPacketItem {
        let mut pubrec_packet = PubRecPacket::new(packet_identifier);
        pubrec_packet.fix_header.dup = Some(1);
        QosPacketItem {
            qos_type: QosType::Qos2,
            resend_time,
            packet_identifier,
            state: QosItemState::WaitPubrel,
            resend_packet: Some(MqttPacketV3::Pubrec(pubrec_packet))
        }
    }

    fn new_qos_2_tx_state(mut resend_packet: PublishPacket, packet_identifier: u16, resend_time: u64) -> QosPacketItem {
        resend_packet.fix_header.dup = Some(1);
        QosPacketItem {
            qos_type: QosType::Qos2,
            resend_time,
            packet_identifier,
            state: QosItemState::WaitPubrec,
            resend_packet:Some(MqttPacketV3::Publish(resend_packet)),
        }
    }

    fn new_qos_1_tx_state(mut resend_packet: PublishPacket, packet_identifier: u16, resend_time: u64) -> QosPacketItem {
        resend_packet.fix_header.dup = Some(1);
        QosPacketItem {
            qos_type: QosType::Qos1,
            resend_time,
            packet_identifier,
            state: QosItemState::WaitPuback,
            resend_packet: Some(MqttPacketV3::Publish(resend_packet))
        }
    }

    fn next_state(&mut self) {
        let (next_state, resend_packet) = match self.state {
            QosItemState::WaitPubrel => {
                (QosItemState::Finish, None)
            }
            QosItemState::WaitPubcomp => {
                (QosItemState::Finish, None)
            }
            QosItemState::WaitPubrec => {
                (QosItemState::WaitPubcomp, Some(MqttPacketV3::Pubcomp(PubCompPacket::new(self.packet_identifier))))
            }
            QosItemState::WaitPuback => {
                (QosItemState::Finish, None)
            }
            _ => {
                (QosItemState::Finish, None)
            }
        };
        self.state = next_state;
        self.resend_packet = resend_packet;
    }
}

impl Session {
    pub fn new(
        client_identifier: String, 
        tenant_identifier: String, 
        subscription_topics: Arc<RwLock<Vec<String>>>, 
        connection: Arc<RwLock<Connection>>) -> Arc<Session> {

        let (quit_sender, quit_receiver) = tokio::sync::mpsc::channel(1);

        let session = Arc::new(Session {
            client_identifier,
            tenant_identifier,
            connection: Some(connection),
            subscription_topics,
            qos_state_table: Arc::new(RwLock::new(HashMap::new())),
            qos_resend_task_quit_sender: quit_sender,
            qos_tx_publish_packet_cache: RwLock::new(HashMap::new()),
            qos_rx_publish_packet_cache: RwLock::new(HashMap::new())
        });

        let session_cloned = session.clone();
        tokio::spawn(async move {
            let _ = session_cloned.run_qos_resend_task(quit_receiver).await;
        });
        session
    }

    // Do process qos packet
    async fn do_process_qos_packet(&self, packet: &MqttPacketV3) -> Result<()> {
        let r:Result<Option<u16>>= match packet {
            MqttPacketV3::Publish(publish_packet) => {
                if publish_packet.fix_header.qos.unwrap_or(0) > 0 {
                    let mut qos_stable_table = self.qos_state_table.write().await;
                    if !qos_stable_table.contains_key(&publish_packet.variable_header.packet_identifier.unwrap()) {
                        // first received cached the publish packet
                        if publish_packet.fix_header.qos.unwrap() == 2 {
                            qos_stable_table.insert(
                                publish_packet.variable_header.packet_identifier.unwrap(), 
                                QosPacketItem::new_qos_2_tx_state(
                                    publish_packet.clone(), 
                                    publish_packet.variable_header.packet_identifier.unwrap(), 
                                    std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)?.as_secs() + RESEND_DURATION_TIME
                                )
                            );
                        }
                        if publish_packet.fix_header.qos.unwrap() == 1 {
                            qos_stable_table.insert(
                                publish_packet.variable_header.packet_identifier.unwrap(), 
                                QosPacketItem::new_qos_1_tx_state(
                                    publish_packet.clone(), 
                                    publish_packet.variable_header.packet_identifier.unwrap(), 
                                    std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)?.as_secs() + RESEND_DURATION_TIME
                                )
                            );
                        }
                        self.write(packet).await?;
                        let mut qos_rx_publish_packet_cache = self.qos_rx_publish_packet_cache.write().await;
                        qos_rx_publish_packet_cache.insert(
                            publish_packet.variable_header.packet_identifier.unwrap(), 
                            publish_packet.clone()
                        );
                    }
                }
                Ok(publish_packet.variable_header.packet_identifier)
            }
            MqttPacketV3::Pubrel(pubrel_packet) => {
                if self.packet_identifier_in_used(pubrel_packet.variable_header.packet_identifier).await {
                    let mut state = self.qos_state_table.write().await;
                    let packet_identifier = pubrel_packet.variable_header.packet_identifier;
                    let pubcomp_packet = PubCompPacket::new(pubrel_packet.variable_header.packet_identifier);
                    self.write(&&MqttPacketV3::Pubcomp(pubcomp_packet)).await?;
                    let state = state.get_mut(&packet_identifier).unwrap();
                    state.next_state();
                }
                // not seen the packet id , ignore
                Ok(Some(pubrel_packet.variable_header.packet_identifier))
            }
            MqttPacketV3::Pubrec(pubrec_packet) => {
                if self.packet_identifier_in_used(pubrec_packet.variable_header.packet_identifier).await {
                    let mut state = self.qos_state_table.write().await;
                    let pubrel_packet = PubRelPacket::new(pubrec_packet.variable_header.packet_identifier);
                    self.write(&&MqttPacketV3::Pubrel(pubrel_packet)).await?;
                    let state = state.get_mut(&pubrec_packet.variable_header.packet_identifier).unwrap();
                    state.next_state();
                } 
                Ok(Some(pubrec_packet.variable_header.packet_identifier))
            }
            MqttPacketV3::Pubcomp(pubcomp_packet) => {
                // received pubcomp packet from clients, all qos2 process finished, delete publish packet from cache
                if self.packet_identifier_in_used(pubcomp_packet.variable_header.packet_identifier).await {
                    let mut state = self.qos_state_table.write().await;
                    let mut qos_tx_publish_packet_cache = self.qos_tx_publish_packet_cache.write().await;
                    qos_tx_publish_packet_cache.remove(&pubcomp_packet.variable_header.packet_identifier);
                    let state = state.get_mut(&pubcomp_packet.variable_header.packet_identifier).unwrap();
                    state.next_state();
                } 
                Ok(Some(pubcomp_packet.variable_header.packet_identifier))
            }
            MqttPacketV3::Puback(puback_packet) => {
                if self.packet_identifier_in_used(puback_packet.variable_header.packet_identifier).await {
                    let mut state = self.qos_state_table.write().await;
                    let mut qos_tx_publish_packet_cache = self.qos_tx_publish_packet_cache.write().await;
                    qos_tx_publish_packet_cache.remove(&puback_packet.variable_header.packet_identifier);
                    let state = state.get_mut(&puback_packet.variable_header.packet_identifier).unwrap();
                    state.next_state();
                } 
                Ok(Some(puback_packet.variable_header.packet_identifier))
            }
            _ => {
                Ok(None)
            }
        };

        if let Ok(Some(packet_id)) = r {
            let mut should_release_packet_identifier = false;
            {
                let qos_state_table = self.qos_state_table.read().await;
                should_release_packet_identifier = qos_state_table.get(&packet_id).unwrap().state == QosItemState::Finish;
            }
            if  should_release_packet_identifier {
                self.release_packet_identifier(&packet_id).await;
            }
        }

        Ok(())
    }

    // Close the connection voluntarily
    pub async fn shutdown(&self) -> Result<()> {
        self.qos_resend_task_quit_sender.send(()).await?; // notify qos2 rec resend task quit
        let connection = self.connection.as_ref().unwrap().clone();
        let mut connection = connection.write().await;
        connection.shutdown().await?;
        Ok(())
    }

    async fn release_packet_identifier(&self, packet_id:&u16) {
        let mut qos_state_table = self.qos_state_table.write().await;
        let mut qos_tx_publish_packet_cache = self.qos_tx_publish_packet_cache.write().await;
        let mut qos_rx_publish_packet_cache = self.qos_rx_publish_packet_cache.write().await;
        qos_state_table.remove(packet_id);
        qos_tx_publish_packet_cache.remove(packet_id);
        qos_rx_publish_packet_cache.remove(packet_id);
    }

    async fn packet_identifier_in_used(&self, packet_id: u16) -> bool {
        let qos2_pub_rec_resend_queue = self.qos_state_table.read().await;
        qos2_pub_rec_resend_queue.contains_key(&packet_id)
    }

    // Qos2 PubRec resend task
    // When session write pubrec packet to the underlying stream, the broker should wait the pubrel packet
    // if reach the wait pubrel timeout, session should rewrite the pubrec which set dup to 1
    async fn run_qos_resend_task(&self, mut quit_receiver: Receiver<()>) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            select! {
                _ = interval.tick() => {
                    let qos_state_table = self.qos_state_table.read().await;
                    for (_, state) in qos_state_table.iter() {
                        if state.resend_time < std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)?.as_secs() {
                            if let Some(packet) = &state.resend_packet {
                                self.write(packet).await? 
                            } 
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
    async fn write(&self, packet: &MqttPacketV3) -> Result<()> {
        if let Some(connection) = &self.connection {
            let mut connection = connection.write().await;
            connection.write_packet(packet).await?
        }
        Ok(())
    }

    pub async fn write_packet(&self, packet: &MqttPacketV3) -> Result<()> {
        // if the packet is publish packet and the qos is 1 or 2, should run the qos process logic
        // else send the packet directly
        if let MqttPacketV3::Publish(publish_packet) = packet {
            if let Some(qos) = publish_packet.fix_header.qos {
                if qos > 0 {
                    self.do_process_qos_packet(packet).await?;
                }
            }
        } else {
            self.write(packet).await?;
        }
        Ok(())
    }

}

pub struct SessionManager {
    session_table: HashMap<String, Arc<Session>>,
    tenant_id: String
}

impl SessionManager {
    
    pub fn register(&mut self, client_identifier: String, session:Session) -> Arc<Session> {
        let session = Arc::new(session);
        self.session_table.insert(client_identifier, session.clone());
        return session;
    }

    pub fn get(&self, client_identifier: String) -> Option<Arc<Session>> {
        if let Some(session) = self.session_table.get(&client_identifier){
            Some(session.clone())
        } else {
            None
        }
    }

    pub fn unregister(&mut self, client_identifier: String) {
        self.session_table.remove(&client_identifier);
    }

}


#[cfg(test)]
mod tests {
    #[test]
    fn test_state_table() {
    }
}