use std::{sync::{RwLock, Arc, mpsc::Sender}, collections::{HashMap, VecDeque}, time::Duration};

use crate::{connection::Connection, protocol::{MqttPacketV3, v3::{publish::PublishPacket, pubcomp::PubCompPacket, pubrec::PubRecPacket, pubrel::{self, PubRelPacket}}}};
use anyhow::Result;
use tokio::{sync::mpsc::Receiver, select};

const RESEND_DURATION_TIME: u64 = 10;

// Represent mqtt session
pub struct Session {
    client_identifier: String,
    tenant_identifier: String,
    connection: Arc<RwLock<Connection>>,
    subscription_topics: Arc<RwLock<Vec<String>>>,
    qos_state_table: Arc<RwLock<HashMap<u16, QosPacketItem>>>,
    qos_resend_task_quit_sender: Sender<()>,
    qos_tx_publish_packet_cache: RwLock<HashMap<u16, PublishPacket>>,
    qos_rx_publish_packet_cache: RwLock<HashMap<u16, PublishPacket>>
}

#[derive(PartialEq, Eq)]
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

    async fn process_qos_packet(&self, packet: &MqttPacketV3) -> Result<()> {
        match packet {
            MqttPacketV3::Publish(publish_packet) => {
                if publish_packet.fix_header.qos.unwrap_or(0) > 0 {
                    if !self.qos_state_table.read().unwrap().contains_key(&publish_packet.variable_header.packet_identifier.unwrap()) {
                        // first received cached the publish packet
                        if publish_packet.fix_header.qos.unwrap() == 2 {
                            self.qos_state_table.write().unwrap().insert(
                                publish_packet.variable_header.packet_identifier.unwrap(), 
                                QosPacketItem::new_qos_2_tx_state(
                                    publish_packet.clone(), 
                                    publish_packet.variable_header.packet_identifier.unwrap(), 
                                    std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)?.as_secs() + RESEND_DURATION_TIME
                                )
                            );
                        }
                        if publish_packet.fix_header.qos.unwrap() == 1 {
                            self.qos_state_table.write().unwrap().insert(
                                publish_packet.variable_header.packet_identifier.unwrap(), 
                                QosPacketItem::new_qos_1_tx_state(
                                    publish_packet.clone(), 
                                    publish_packet.variable_header.packet_identifier.unwrap(), 
                                    0
                                )
                            );
                        }
                        self.qos_rx_publish_packet_cache.write().unwrap().insert(
                            publish_packet.variable_header.packet_identifier.unwrap(), 
                            publish_packet.clone()
                        );
                    }
                }
                Ok(())
            }
            MqttPacketV3::Pubrel(pubrel_packet) => {
                if self.packet_identifier_in_used(pubrel_packet.variable_header.packet_identifier) {
                    let packet_identifier = pubrel_packet.variable_header.packet_identifier;
                    let pubcomp_packet = PubCompPacket::new(pubrel_packet.variable_header.packet_identifier);
                    self.write(&&MqttPacketV3::Pubcomp(pubcomp_packet)).await?;
                    let mut state = self.qos_state_table.write().unwrap();
                    let state = state.get_mut(&packet_identifier).unwrap();
                    state.next_state();
                }
                // not seen the packet id , ignore
                Ok(())
            }
            MqttPacketV3::Pubrec(pubrec_packet) => {
                if self.packet_identifier_in_used(pubrec_packet.variable_header.packet_identifier) {
                    let pubrel_packet = PubRelPacket::new(pubrec_packet.variable_header.packet_identifier);
                    self.write(&&MqttPacketV3::Pubrel(pubrel_packet)).await?;
                    let mut state = self.qos_state_table.write().unwrap();
                    let state = state.get_mut(&pubrec_packet.variable_header.packet_identifier).unwrap();
                    state.next_state();
                } 
                Ok(())
            }
            MqttPacketV3::Pubcomp(pubcomp_packet) => {
                // received pubcomp packet from clients, all qos2 process finished, delete publish packet from cache
                if self.packet_identifier_in_used(pubcomp_packet.variable_header.packet_identifier) {
                    self.qos_tx_publish_packet_cache.write().unwrap().remove(&pubcomp_packet.variable_header.packet_identifier);
                    let mut state = self.qos_state_table.write().unwrap();
                    let state = state.get_mut(&pubcomp_packet.variable_header.packet_identifier).unwrap();
                    state.next_state();
                } 
                Ok(())
            }
            MqttPacketV3::Puback(puback_packet) => {
                if self.packet_identifier_in_used(puback_packet.variable_header.packet_identifier) {
                    self.qos_tx_publish_packet_cache.write().unwrap().remove(&puback_packet.variable_header.packet_identifier);
                    let mut state = self.qos_state_table.write().unwrap();
                    let state = state.get_mut(&puback_packet.variable_header.packet_identifier).unwrap();
                    state.next_state();
                } 
                Ok(())
            }
            _ => {
                Ok(())
            }
        }
    }

    fn close(&self) {
        self.qos_resend_task_quit_sender.send(()); // notify qos2 rec resend task quit
        todo!("close session should close connection")
    }

    fn packet_identifier_in_used(&self, packet_id: u16) -> bool {
        let mut qos2_pub_rec_resend_queue = self.qos_state_table.read().unwrap();
        qos2_pub_rec_resend_queue.contains_key(&packet_id)
    }

    fn remove_from_qos2_pub_rec_resend_queue(&self, packet_id: u16) {
        let mut qos2_pub_rec_resend_queue = self.qos_state_table.write().unwrap();
        qos2_pub_rec_resend_queue.remove(&packet_id);
    }

    // Qos2 PubRec resend task
    // When session write pubrec packet to the underlying stream, the broker should wait the pubrel packet
    // if reach the wait pubrel timeout, session should rewrite the pubrec which set dup to 1
    async fn run_qos_resend_task(&self, mut quit_receiver: Receiver<()>) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            select! {
                _ = interval.tick() => {
                    for (_, state) in self.qos_state_table.read().unwrap().iter() {
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