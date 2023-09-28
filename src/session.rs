use std::{sync::{Arc}, collections::HashMap, time::Duration};

use crate::{connection::Connection, protocol::{MqttPacketV3, v3::{publish::{PublishPacket, PublishPacketBuilder}, pingresp::PingrespPacket, suback::SubackPacket, fixed_header::FixHeader}, PacketType}, router::RouterCmd, topic::TopicManager};
use crate::qos_context::QosContext;
use anyhow::{Result, Context};
use tokio::{sync::mpsc::{Receiver, Sender}, select, net::TcpStream, io::{AsyncRead, AsyncWrite}};
use tokio::sync::{RwLock};
use log::{warn, info, error};

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
pub struct Session<T: AsyncRead + AsyncWrite + Unpin> {

    // MQTT Will Message
    will_message: Option<WillMessage>,

    // MQTT Client Identifier, unique in the tenant
    client_identifier: String,

    // Tenant Identifier, unique in the system
    tenant_identifier: String,

    connection: Option<Connection<T>>,

    // The session subscribed topics
    subscription_topics: RwLock<Vec<String>>,

    // The QOS context, track all in flights qos packet.
    qos_context: QosContext,

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

    // Connection has shutdown
    has_shutdown: bool,

    // Resend qos packet check interval
    resend_check_interval: Duration
}


impl <T> Session<T>
where
    T: AsyncRead + AsyncWrite + Unpin
 {

    pub fn new(
        client_identifier: String, 
        tenant_identifier: String, 
        connection: Connection<T>,
        topic_tree: Arc<RwLock<TopicManager>>,
        router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
        session_cmd_receiver: Receiver<SessionCmd>,
        will_message: Option<WillMessage>,
        keep_alive: u16,
        resend_check_interval: Duration
    ) -> Session<T> {

        let (main_logic_quit_signal_sender, main_logic_quit_signal_receiver) = tokio::sync::mpsc::channel::<()>(1);

        Session {
            client_identifier,
            tenant_identifier,
            connection: Some(connection),
            subscription_topics: RwLock::new(vec![]),
            qos_tx_publish_packet_cache: RwLock::new(HashMap::new()),
            qos_rx_publish_packet_cache: RwLock::new(HashMap::new()),
            router_sender,
            session_cmd_receiver,
            main_logic_quit_signal_receiver,
            main_logic_quit_signal_sender,
            qos_context: QosContext::new(Duration::from_secs(10)),
            topic_tree,
            will_message,
            keep_alive,
            has_shutdown: false,
            resend_check_interval
        }
    }

    // Session core logic loop
    async fn run_logic_loop(&mut self) -> Result<()> {

        let mut resend_check_interval = tokio::time::interval(self.resend_check_interval);

        let mut keep_alive_interval = tokio::time::interval(Duration::from_secs(self.keep_alive.into()));

        let mut keep_alive_timeout_flag = true;

        let mut _interval_first_tick = true;

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
                                    let packet_identifier = publish_packet.variable_header.packet_identifier.unwrap();
                                    let packet = MqttPacketV3::Publish(publish_packet);
                                    self.new_tx_qos_state_ctx(&packet).await;
                                    if let Err(e) = self.write_to_client(&packet).await {
                                        error!("tenant {} session {} do process qos packet error: {}", self.tenant_identifier, self.client_identifier, e);
                                        self.main_logic_quit_signal_sender.send(()).await?;
                                    }                                     
                                    self.qos_context.next_state(packet_identifier).await;
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
                        if !_interval_first_tick {
                            self.send_will_packet().await?;
                            if let Err(e) = self.shutdown().await {
                                error!("tenant {} session {} shutdown error: {}", self.tenant_identifier, self.client_identifier, e);
                            }
                            self.main_logic_quit_signal_sender.send(()).await?; // notfiy  exit the main logic loop
                        } else {
                            _interval_first_tick = false;
                            keep_alive_timeout_flag = true;
                        }
                    } else {
                        keep_alive_timeout_flag = true;
                    }
                }
                _ = resend_check_interval.tick() => {
                    // Qos2 PubRec resend task
                    // When session write pubrec packet to the underlying stream, the broker should wait the pubrel packet
                    // if reach the wait pubrel timeout, session should rewrite the pubrec which set dup to 1
                    if !self.has_shutdown {
                        let packets = self.qos_context.get_all_expired_packets().await;
                        for packet in packets {
                            if let Err(e) = self.write_to_client(&packet).await  {
                                error!("tenant {} session {} write to client error: {}", self.tenant_identifier, self.client_identifier, e);
                                self.main_logic_quit_signal_sender.send(()).await?;
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
        if !self.has_shutdown {
            info!("tenant {} session {} shutdown", self.tenant_identifier, self.client_identifier);
            self.shutdown().await?;
        }
        Ok(())
    }

    async fn do_process_rx_packet(&mut self, packet: &MqttPacketV3) -> Result<()> {
        match packet {
            MqttPacketV3::Publish(publish_packet) => {
                let cmd = RouterCmd::RoutePacket(self.tenant_identifier.clone(), MqttPacketV3::Publish(publish_packet.clone()));
                self.router_sender.send(cmd).await?;
                self.new_rx_qos_state_ctx(packet).await;
                let packet = self.qos_context.get_current_packet(publish_packet.variable_header.packet_identifier.unwrap()).await.unwrap();
                self.write_to_client(&packet).await?;
            }
            MqttPacketV3::Disconnect(_) => {
                self.shutdown().await?; //shutdown the connection
                self.main_logic_quit_signal_sender.send(()).await?; // notfiy exit the main logic loop
            }
            MqttPacketV3::Pingreq(_) => {
                self.write_to_client(&MqttPacketV3::Pingresp(PingrespPacket::new())).await?;
            }
            MqttPacketV3::Puback(puback_packet) => {
                self.qos_context.next_state(puback_packet.variable_header.packet_identifier).await;
                if let Some(p) = self.qos_context.get_current_packet(puback_packet.variable_header.packet_identifier).await {
                   self.write_to_client(&p).await?;
                }
            }
            MqttPacketV3::Pubrec(pubrec_packet) => {
                self.qos_context.next_state(pubrec_packet.variable_header.packet_identifier).await;
                if let Some(p) = self.qos_context.get_current_packet(pubrec_packet.variable_header.packet_identifier).await {
                   self.write_to_client(&p).await?;
                }
            }
            MqttPacketV3::Pubrel(pubrel_packet) => {
                self.qos_context.next_state(pubrel_packet.variable_header.packet_identifier).await;
                if let Some(p) = self.qos_context.get_current_packet(pubrel_packet.variable_header.packet_identifier).await {
                   self.write_to_client(&p).await?;
                }
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
            MqttPacketV3::Pubcomp(pubcomp_packet) => {
                self.qos_context.next_state(pubcomp_packet.variable_header.packet_identifier).await;
                if let Some(p) = self.qos_context.get_current_packet(pubcomp_packet.variable_header.packet_identifier).await {
                   self.write_to_client(&p).await?;
                }
            }
            _ => {
                // Do nothing
            }
        }
        Ok(())
    }

    async fn new_tx_qos_state_ctx(&mut self, packet: &MqttPacketV3) {
        if let MqttPacketV3::Publish(_) = packet {
           self.qos_context.register_with_tx_packet(packet).await;
        }
    }

    async fn new_rx_qos_state_ctx(&mut self, packet: &MqttPacketV3) {
        if let MqttPacketV3::Publish(_) = packet {
           self.qos_context.register_with_rx_packet(packet).await;
        }
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
            self.has_shutdown = true;
        }
        Ok(())
    }

    async fn release_packet_identifier(&mut self, packet_id:&u16) {
        let mut qos_tx_publish_packet_cache = self.qos_tx_publish_packet_cache.write().await;
        let mut qos_rx_publish_packet_cache = self.qos_rx_publish_packet_cache.write().await;
        self.qos_context.del(packet_id.clone());
        qos_tx_publish_packet_cache.remove(packet_id);
        qos_rx_publish_packet_cache.remove(packet_id);
    }

    fn packet_identifier_in_used(&self, packet_id: u16) -> bool {
        self.qos_context.contains_packet_identifier(packet_id)
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
    session: Arc<RwLock<Session<TcpStream>>>,
    session_sender: Sender<SessionCmd>
}

pub struct SessionManager {
    session_table: RwLock<HashMap<String, SessionWrapper>>,
    tenant_id: String
}

impl SessionManager {
    
    pub async fn register(&mut self, client_identifier: String, mut session:Session<TcpStream>) -> Arc<RwLock<Session<TcpStream>>> {
        let (session_sender, session_receiver) = tokio::sync::mpsc::channel::<SessionCmd>(1);
        session.session_cmd_receiver = session_receiver;
        let session = Arc::new(RwLock::new(session));
        let mut session_table = self.session_table.write().await;

        session_table.insert(client_identifier, SessionWrapper { session: session.clone(), session_sender });
        return session;
    }

    pub async fn get(&self, client_identifier: String) -> Option<Arc<RwLock<Session<TcpStream>>>> {
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
    use std::{sync::{Arc}, time::Duration};

    use nom::AsBytes;
    use tokio::sync::RwLock;

    use crate::{session::{Session, SessionManager, SessionCmd}, connection::Connection, topic::TopicManager, router::RouterCmd, protocol::{v3::{publish::PublishPacketBuilder, puback::{PubAckPacket, VariableHeader}, fixed_header::FixHeader}, MqttPacketV3, PacketType}};


    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_keep_alive_timeout() {
        let mock_io = tokio_test::io::Builder::new().wait(Duration::from_secs(11)).build();
        let mut connection = Connection::new(mock_io); 

        let (router_sender, router_receiver) = tokio::sync::mpsc::channel::<RouterCmd>(1);
        let (session_sender, session_receiver) = tokio::sync::mpsc::channel::<SessionCmd>(1);

        let mut session = Session::new(
            "client_a".to_string(),
            "tenant_a".to_string(),
            connection,
            Arc::new(RwLock::new(TopicManager::new())),
            router_sender,
            session_receiver,
            None,
            6,
            Duration::from_secs(10)
        );
        let _ = session.run_logic_loop().await;
    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_qos_1_process() {
        // Read Qos1 publish packet should return puback packet
        let qos_1_publish_packet = PublishPacketBuilder::new("a/b".to_string(), vec![0x01])
            .qos(1)
            .packet_identifier(0x01)
            .build();

        let packet = MqttPacketV3::Publish(qos_1_publish_packet);
        
        let fix_header = FixHeader {
            packet_type: PacketType::PUBACK, 
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 2,
        };

        let variable_header = VariableHeader{
            packet_identifier: 0x01
        };

        let puback_packet = PubAckPacket {
            fix_header,
            variable_header
        };

        let puback_packet = MqttPacketV3::Puback(puback_packet);

        let mock_io = tokio_test::io::Builder::new()
            .wait(Duration::from_secs(3))
            .read(packet.to_bytes().as_bytes())
            .write(&puback_packet.to_bytes().as_bytes())
            .build();

        let mut connection = Connection::new(mock_io); 

        let (router_sender, router_receiver) = tokio::sync::mpsc::channel::<RouterCmd>(1);
        let (session_sender, session_receiver) = tokio::sync::mpsc::channel::<SessionCmd>(1);

        let mut session = Session::new(
            "client_a".to_string(),
            "tenant_a".to_string(),
            connection,
            Arc::new(RwLock::new(TopicManager::new())),
            router_sender,
            session_receiver,
            None,
            6, 
            Duration::from_secs(13) // because firt write occurs 3 seconds later and the mock io has no other expect write, so resend check interval should twice the keep alive interval
        );
        let _ = session.run_logic_loop().await;

    }

}