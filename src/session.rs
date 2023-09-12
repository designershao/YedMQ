use std::{sync::{Arc}, collections::HashMap, time::Duration, ops::Deref};

use crate::{connection::Connection, protocol::{MqttPacketV3, v3::{publish::{PublishPacket, self}, pubcomp::PubCompPacket, pubrec::PubRecPacket, pubrel::{self, PubRelPacket}, pingresp::PingrespPacket, suback::SubackPacket}}, router::RouterCmd, topic::TopicManager};
use anyhow::Result;
use tokio::{sync::mpsc::{Receiver, Sender}, select, net::TcpStream};
use tokio::sync::{RwLock};
use log::{warn};

const RESEND_DURATION_TIME: u64 = 10;

pub struct WillMessage {
    will_topic: String,
    will_message: Vec<u8>,
    will_qos: u8
}

// Represent mqtt session
pub struct Session {
    will_message: Option<WillMessage>,
    client_identifier: String,
    tenant_identifier: String,
    connection: Option<Connection<TcpStream>>,
    subscription_topics: RwLock<Vec<String>>,
    qos_state_table: Arc<RwLock<HashMap<u16, QosPacketItem>>>,
    logic_loop_quit_sender: Sender<()>,
    qos_tx_publish_packet_cache: RwLock<HashMap<u16, PublishPacket>>,
    qos_rx_publish_packet_cache: RwLock<HashMap<u16, PublishPacket>>,
    router_sender: tokio::sync::mpsc::Sender<RouterCmd>, // send command to router
    topic_tree: Arc<RwLock<TopicManager>>,
    keep_alive: u16,
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
        connection: Connection<TcpStream>,
        topic_tree: Arc<RwLock<TopicManager>>,
        router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
        will_message: Option<WillMessage>,
        keep_alive: u16
    ) -> Arc<Session> {

        let session = Arc::new(Session {
            client_identifier,
            tenant_identifier,
            connection: Some(connection),
            subscription_topics: RwLock::new(vec![]),
            qos_state_table: Arc::new(RwLock::new(HashMap::new())),
            qos_tx_publish_packet_cache: RwLock::new(HashMap::new()),
            qos_rx_publish_packet_cache: RwLock::new(HashMap::new()),
            logic_loop_quit_sender: todo!(),
            router_sender,
            topic_tree,
            will_message,
            keep_alive
        });

        session
    }

    pub async fn process_route_packet(&mut self, packet: &MqttPacketV3) -> Result<()> {
        // if the packet is qos 1  or qos 2 publish packet, do qos process
        // else write to clietn directly
        if let MqttPacketV3::Publish(publish_packet) = packet {
            if publish_packet.fix_header.qos > Some(0) {
                self.do_process_qos_packet(packet).await?;
            }
        } else {
            self.write_to_client(&packet).await?;
        }
        Ok(())
    }

    // Session core logic loop
    async fn run_logic_loop(&mut self, mut quit_receiver:Receiver<()>) -> Result<()> {

        let mut resend_check_interval = tokio::time::interval(Duration::from_secs(10));

        let mut keep_alive_interval = tokio::time::interval(Duration::from_secs(self.keep_alive.into()));

        let mut keep_alive_timeout_flag = true;

        loop {
            select! {
                _ = keep_alive_interval.tick() => {
                    if keep_alive_timeout_flag {
                        self.shutdown().await?;
                        break;
                    }
                }
                _ = resend_check_interval.tick() => {
                    // Qos2 PubRec resend task
                    // When session write pubrec packet to the underlying stream, the broker should wait the pubrel packet
                    // if reach the wait pubrel timeout, session should rewrite the pubrec which set dup to 1
                    let qos_state_table = self.qos_state_table.clone();
                    let qos_state_table = qos_state_table.read().await;
                    for (_, state) in qos_state_table.iter() {
                        if state.resend_time < std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH)?.as_secs() {
                            if let Some(packet) = &state.resend_packet {
                                self.write_to_client(packet).await? 
                            } 
                        }
                    }
                }
                packet = self.connection.as_mut().unwrap().read_packet() => {
                    keep_alive_timeout_flag = false;
                    if let Ok(packet) = packet {
                        self.do_process_rx_packet(&packet).await?
                    }
                }
                _ = quit_receiver.recv() => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn do_process_rx_packet(&mut self, packet: &MqttPacketV3) -> Result<()> {
        match packet {
            MqttPacketV3::Publish(publish_packet) => {
                let cmd = RouterCmd::RoutePacket(self.tenant_identifier.clone(), MqttPacketV3::Publish(publish_packet.clone()));
                self.router_sender.send(cmd).await?;
            }
            MqttPacketV3::Disconnect(disconnect_packet) => {
                self.shutdown().await?; //shutdown the connection
            }
            MqttPacketV3::Pingreq(_) => {
                self.write_to_client(&MqttPacketV3::Pingresp(PingrespPacket::new())).await?;
            }
            MqttPacketV3::Puback(_) => {
                self.do_process_qos_packet(packet).await?;
            }
            MqttPacketV3::Pubrec(_) => {
                self.do_process_qos_packet(packet).await?;
            }
            MqttPacketV3::Pubrel(_) => {
                self.do_process_qos_packet(packet).await?;
            }
            MqttPacketV3::Subscribe(subscribe_packet) => {
                let subscriptions = &subscribe_packet.payload.topic_filters;
                let packet_identifier = &subscribe_packet.variable_header.packet_identifier;

                let mut retain_messages:Vec<Arc<MqttPacketV3>> = vec![];

                let mut return_code:Vec<crate::protocol::v3::suback::ReturnCode> = vec![];
                {
                    let mut topic_manager = self.topic_tree.write().await;

                    for topic in subscriptions.iter() {
                        let sub_result = topic_manager.subscription(
                            self.tenant_identifier.clone(), 
                            self.client_identifier.clone(), 
                            topic.topic_name.clone(),
                            topic.qos 
                        );
                        if let Ok(_) = sub_result {
                            if topic.qos == 0 {
                                return_code.push(crate::protocol::v3::suback::ReturnCode::MaxQos0); 
                            }
                            if topic.qos == 1 {
                                return_code.push(crate::protocol::v3::suback::ReturnCode::MaxQos1); 
                            }
                            if topic.qos == 2 {
                                return_code.push(crate::protocol::v3::suback::ReturnCode::MaxQos2); 
                            }
                            let mut subscription_topics = self.subscription_topics.write().await;
                            subscription_topics.push(topic.topic_name.clone());
                            let packets = topic_manager.get_retain_publish_packet(
                                self.tenant_identifier.clone(), 
                                self.client_identifier.clone(), 
                                topic.topic_name.clone()
                            );
                            if let Ok(packets) = packets {
                                for packet in packets {
                                    retain_messages.push(packet);
                                }
                            }
                        } else {
                            return_code.push(crate::protocol::v3::suback::ReturnCode::Failure);
                        }
                    }
                }
                for packet in retain_messages {
                    self.write_to_client(&packet).await?; 
                }
                self.write_to_client(&MqttPacketV3::Suback(
                    SubackPacket::new(*packet_identifier, return_code))).await?;
            }
            MqttPacketV3::Unsubscribe(unsubscribe_packet) => {
                let unsub_topic_filters = &unsubscribe_packet.payload.topic_filters;
                {
                    let mut topic_manager = self.topic_tree.write().await;
                    for topic in unsub_topic_filters {
                        let _ = topic_manager.unsubscription(self.tenant_identifier.clone(), self.client_identifier.clone(), topic.topic_name.clone());
                    }
                }
                let unsub_ack = MqttPacketV3::Unsuback(crate::protocol::v3::unsuback::UnSubackPacket::new(unsubscribe_packet.variable_header.packet_identifier));
                self.write_to_client(&unsub_ack).await?
            }
            MqttPacketV3::Pubcomp(_) => {
                self.do_process_qos_packet(packet).await;
            }
            _ => {
                // Do nothing
            }
        }
        Ok(())
    }

    // Do process qos packet
    async fn do_process_qos_packet(&mut self, packet: &MqttPacketV3) -> Result<()> {
        let qos_state_table = self.qos_state_table.clone();
        let r:Result<Option<u16>>= match packet {
            MqttPacketV3::Publish(publish_packet) => {
                if publish_packet.fix_header.qos.unwrap_or(0) > 0 {
                    let mut qos_stable_table = qos_state_table.write().await;
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
                        self.write_to_client(packet).await?;
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
                    let mut state = qos_state_table.write().await;
                    let packet_identifier = pubrel_packet.variable_header.packet_identifier;
                    let pubcomp_packet = PubCompPacket::new(pubrel_packet.variable_header.packet_identifier);
                    self.write_to_client(&&MqttPacketV3::Pubcomp(pubcomp_packet)).await?;
                    let state = state.get_mut(&packet_identifier).unwrap();
                    state.next_state();
                }
                // not seen the packet id , ignore
                Ok(Some(pubrel_packet.variable_header.packet_identifier))
            }
            MqttPacketV3::Pubrec(pubrec_packet) => {
                if self.packet_identifier_in_used(pubrec_packet.variable_header.packet_identifier).await {
                    let mut state = qos_state_table.write().await;
                    let pubrel_packet = PubRelPacket::new(pubrec_packet.variable_header.packet_identifier);
                    self.write_to_client(&&MqttPacketV3::Pubrel(pubrel_packet)).await?;
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
    pub async fn shutdown(&mut self) -> Result<()> {
        self.logic_loop_quit_sender.send(()).await?;
        let connection = self.connection.as_mut().unwrap();
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

    // Write a single packet to the underlying stream.
    async fn write_to_client(&mut self, packet: &MqttPacketV3) -> Result<()> {
        if let Some(connection) = &mut self.connection {
            connection.write_packet(packet).await?
        }
        Ok(())
    }

}

pub struct SessionManager {
    session_table: RwLock<HashMap<String, Arc<RwLock<Session>>>>,
    tenant_id: String
}

impl SessionManager {
    
    pub async fn register(&mut self, client_identifier: String, session:Session) -> Arc<RwLock<Session>> {
        let session = Arc::new(RwLock::new(session));
        let mut session_table = self.session_table.write().await;
        session_table.insert(client_identifier, session.clone());
        return session;
    }

    pub async fn get(&self, client_identifier: String) -> Option<Arc<RwLock<Session>>> {
        let session_table = self.session_table.read().await;
        if let Some(session) = session_table.get(&client_identifier){
            Some(session.clone())
        } else {
            None
        }
    }

    pub async fn unregister(&mut self, client_identifier: String) {
        let mut session_table = self.session_table.write().await;
        session_table.remove(&client_identifier);
    }

}


#[cfg(test)]
mod tests {
    #[test]
    fn test_state_table() {
    }
}