use std::{sync::{Arc}, collections::HashMap, time::Duration, ops::Deref};

use crate::{connection::Connection, protocol::{MqttPacketV3, v3::{publish::{PublishPacket, self, VariableHeader, Payload, PublishPacketBuilder}, pubcomp::PubCompPacket, pubrec::PubRecPacket, pubrel::{self, PubRelPacket}, pingresp::PingrespPacket, suback::SubackPacket, fixed_header::FixHeader}, PacketType}, router::RouterCmd, topic::TopicManager};
use anyhow::{Result, Context};
use tokio::{sync::mpsc::{Receiver, Sender}, select, net::TcpStream};
use tokio::sync::{RwLock};
use log::{warn, info, error};

const RESEND_DURATION_TIME: u64 = 10;

pub enum SessionCmd {
    Send(MqttPacketV3),
    Disconnect
}

pub struct WillMessage {
    will_topic: String,
    will_message: Vec<u8>,
    will_qos: u8,
    will_retain: bool
}

// Represent mqtt session
pub struct Session {

    // MQTT Will Message
    will_message: Option<WillMessage>,

    // MQTT Client Identifier, unique in the tenant
    client_identifier: String,

    // Tenant Identifier, unique in the system
    tenant_identifier: String,

    connection: Option<Connection<TcpStream>>,

    // The session subscribed topics
    subscription_topics: RwLock<Vec<String>>,

    // The QOS state table, it represent the QOS state of the packet identifier
    qos_state_table: Arc<RwLock<HashMap<u16, QosPacketItem>>>,

    // Session cmd receiver
    session_cmd_receiver: Receiver<SessionCmd>,

    // TX packet identifier cache
    qos_tx_publish_packet_cache: RwLock<HashMap<u16, PublishPacket>>,

    // RX packet identifer cache
    qos_rx_publish_packet_cache: RwLock<HashMap<u16, PublishPacket>>,

    // Main logic quit signal sender
    main_logic_quit_signal_sender: tokio::sync::mpsc::Sender<()>,

    // Main logic quit signal receiver
    main_logic_quit_signal_receiver: tokio::sync::mpsc::Receiver<()>,

    // Router cmd sender 
    router_sender: tokio::sync::mpsc::Sender<RouterCmd>,

    // Topic tree
    topic_tree: Arc<RwLock<TopicManager>>,

    // MQTT Keep alive
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
        session_cmd_receiver: Receiver<SessionCmd>,
        will_message: Option<WillMessage>,
        keep_alive: u16
    ) -> Arc<Session> {

        let (main_logic_quit_signal_sender, main_logic_quit_signal_receiver) = tokio::sync::mpsc::channel::<()>(1);

        let session = Arc::new(Session {
            client_identifier,
            tenant_identifier,
            connection: Some(connection),
            subscription_topics: RwLock::new(vec![]),
            qos_state_table: Arc::new(RwLock::new(HashMap::new())),
            qos_tx_publish_packet_cache: RwLock::new(HashMap::new()),
            qos_rx_publish_packet_cache: RwLock::new(HashMap::new()),
            router_sender,
            session_cmd_receiver,
            main_logic_quit_signal_receiver,
            main_logic_quit_signal_sender,
            topic_tree,
            will_message,
            keep_alive
        });

        session
    }

    // Session core logic loop
    async fn run_logic_loop(&mut self) -> Result<()> {

        let mut resend_check_interval = tokio::time::interval(Duration::from_secs(10));

        let mut keep_alive_interval = tokio::time::interval(Duration::from_secs(self.keep_alive.into()));

        let mut keep_alive_timeout_flag = true;

        loop {
            select! {
                _ = self.main_logic_quit_signal_receiver.recv() => {
                    info!("tenant {} session {} exit main logic", self.tenant_identifier, self.client_identifier);
                    break; 
                }
                Some(cmd) = self.session_cmd_receiver.recv() => {
                    match cmd {
                        SessionCmd::Send(packet)=>{
                            if let MqttPacketV3::Publish(publish_packet) = packet {
                                if publish_packet.fix_header.qos > Some(0) {
                                    if let Err(e) = self.do_process_qos_packet(&MqttPacketV3::Publish(publish_packet)).await {
                                        error!("tenant {} session {} do process qos packet error: {}", self.tenant_identifier, self.client_identifier, e);
                                        self.main_logic_quit_signal_sender.send(()).await?;
                                    }
                                }
                            } else {
                                if let Err(e) = self.write_to_client(&packet).await {
                                    error!("tenant {} session {} write to client error: {}", self.tenant_identifier, self.client_identifier, e);
                                    self.main_logic_quit_signal_sender.send(()).await?;
                                }
                            }
                        }
                        SessionCmd::Disconnect => {
                            if let Err(e) = self.shutdown().await {
                                error!("tenant {} session {} shutdown error: {}", self.tenant_identifier, self.client_identifier, e);
                                self.main_logic_quit_signal_sender.send(()).await?;
                            }
                        }, 
                    }
                }
                _ = keep_alive_interval.tick() => {
                    if keep_alive_timeout_flag {
                        self.send_will_packet().await?;
                        if let Err(e) = self.shutdown().await {
                            error!("tenant {} session {} shutdown error: {}", self.tenant_identifier, self.client_identifier, e);
                        }
                        self.main_logic_quit_signal_sender.send(()).await?; // notfiy  exit the main logic loop
                    }
                }
                _ = resend_check_interval.tick() => {
                    // Qos2 PubRec resend task
                    // When session write pubrec packet to the underlying stream, the broker should wait the pubrel packet
                    // if reach the wait pubrel timeout, session should rewrite the pubrec which set dup to 1
                    let qos_state_table = self.qos_state_table.clone();
                    let qos_state_table = qos_state_table.read().await;
                    for (_, state) in qos_state_table.iter() {
                        if state.resend_time < std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs() {
                            if let Some(packet) = &state.resend_packet {
                                if let Err(e) = self.write_to_client(packet).await  {
                                    error!("tenant {} session {} write to client error: {}", self.tenant_identifier, self.client_identifier, e);
                                    self.main_logic_quit_signal_sender.send(()).await?;
                                }
                            } 
                        }
                    }
                }
                packet = self.connection.as_mut().unwrap().read_packet() => {
                    keep_alive_timeout_flag = false;
                    if let Ok(packet) = packet {
                        let _ = self.do_process_rx_packet(&packet).await;
                    } else {
                        // read packet io error, send will packet and shutdown the session
                        error!("tenant {} session {} read packet io error",self.tenant_identifier, self.client_identifier);
                        self.send_will_packet().await?;

                        if let Err(e) = self.shutdown().await {
                            error!("tenant {} session {} shutdown error: {}", self.tenant_identifier, self.client_identifier, e);
                        }

                        self.main_logic_quit_signal_sender.send(()).await?; // notfiy  exit the main logic loop
                    }
                }
            }
        }
        todo!("Close the connection and drop the connection");
    }

    async fn do_process_rx_packet(&mut self, packet: &MqttPacketV3) -> Result<()> {
        match packet {
            MqttPacketV3::Publish(publish_packet) => {
                let cmd = RouterCmd::RoutePacket(self.tenant_identifier.clone(), MqttPacketV3::Publish(publish_packet.clone()));
                self.router_sender.send(cmd).await?;
            }
            MqttPacketV3::Disconnect(disconnect_packet) => {
                self.shutdown().await?; //shutdown the connection
                self.main_logic_quit_signal_sender.send(()).await?; // notfiy exit the main logic loop
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
    
    // Send will packet
    async fn send_will_packet(&mut self) -> Result<()> {
        if let Some(will_message) = &self.will_message {
            let topic_name = will_message.will_topic.clone();

            let publish_packet = PublishPacketBuilder::new(
                topic_name,
                will_message.will_message.clone(),
            ).retain(will_message.will_retain).qos(will_message.will_qos).build();

            self.router_sender.send(RouterCmd::RoutePacket(self.tenant_identifier.clone(), MqttPacketV3::Publish(publish_packet))).await?;
        }
        Ok(())
    }

    // Close the connection voluntarily
    async fn shutdown(&mut self) -> Result<()> {
        if let Some(connection) = self.connection.as_mut() {
            connection.shutdown().await?;
        }
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

struct SessionWrapper {
    session: Arc<RwLock<Session>>,
    session_sender: Sender<SessionCmd>
}

pub struct SessionManager {
    session_table: RwLock<HashMap<String, SessionWrapper>>,
    tenant_id: String
}

impl SessionManager {
    
    pub async fn register(&mut self, client_identifier: String, mut session:Session) -> Arc<RwLock<Session>> {
        let (session_sender, session_receiver) = tokio::sync::mpsc::channel::<SessionCmd>(1);
        session.session_cmd_receiver = session_receiver;
        let session = Arc::new(RwLock::new(session));
        let mut session_table = self.session_table.write().await;

        session_table.insert(client_identifier, SessionWrapper { session: session.clone(), session_sender });
        return session;
    }

    pub async fn get(&self, client_identifier: String) -> Option<Arc<RwLock<Session>>> {
        let session_table = self.session_table.read().await;
        if let Some(session_wrapper) = session_table.get(&client_identifier){
            Some(session_wrapper.session.clone())
        } else {
            None
        }
    }

    pub async fn send(&self, client_identifier: String, cmd: SessionCmd) -> Result<()> {
        let session_table = self.session_table.read().await;
        if let Some(session_wrapper) = session_table.get(&client_identifier) {
            session_wrapper.session_sender.send(cmd).await.with_context(|| format!("failed to send cmd to session {}", client_identifier))?;
        }
        Ok(())
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