use std::{sync::Arc, time::Duration, collections::HashMap};

use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use tokio::{sync::{mpsc::Sender, RwLock}, net::TcpStream};

use crate::{
    protocol::{
        v3::{pingresp::PingrespPacket, publish::PublishPacketBuilder, suback::SubackPacket},
        MqttPacketV3,
    },
    qos_context::QosContext,
    topic::TopicManager, connection::Connection, router::RouterCmd,
};

// Represent the message which send from the session
pub enum SenderMessage {
    WritePacket(MqttPacketV3), // Write packet to the client
    ForwardToRouter(String, MqttPacketV3), // Packet from router
    ShutdownConnection, // Notfiy the connection shutdown
}

// Represent the message which send to the session
pub enum ReceiverMessage {
    ForwardFromRouter(MqttPacketV3), //Receive packet from router
    Packet(MqttPacketV3), // Receive packet from client connection
    ConnectionHasShutdown, // Connection shutdown notify
    KickOff, // Kick off the session
}

pub struct WillMessage {
    will_topic: String,
    will_message: Vec<u8>,
    will_qos: u8,
    will_retain: bool,
}

// Represent mqtt session
pub struct Session {
    // MQTT Will Message
    will_message: Option<WillMessage>,

    // MQTT Client Identifier, unique in the tenant
    client_identifier: String,

    // Tenant Identifier, unique in the system
    tenant_identifier: String,

    // The session subscribed topics
    subscription_topics: RwLock<Vec<String>>,

    // The QOS context, track all in flights qos packet.
    qos_context: QosContext,

    // Session cmd receiver
    deliver_packet_tx: Option<Sender<SenderMessage>>,

    // Topic tree
    topic_tree: Arc<RwLock<TopicManager>>,
}

impl Session {
    async fn run_offline_loop(mut self) -> Sender<ReceiverMessage> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ReceiverMessage>(1);
        loop {
            tokio::select! {
                msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                match msg {
                                    ReceiverMessage::ForwardFromRouter(packet) => {
                                        if let MqttPacketV3::Publish(publish_packet) = packet {
                                           if publish_packet.fix_header.qos > Some(0) {
                                               self.new_tx_qos_state_ctx(&MqttPacketV3::Publish(publish_packet)).await;
                                           } 
                                        }
                                    }
                                    _ => {
                                        // Do Nothing
                                    }
                                }
                            }
                            None => {
                                break;
                            }
                        }
                    }
                }
            }
        tx
    }

    async fn run_online_loop(
        mut self,
        keep_alive: u64,
        resend_check: u64,
    ) -> Sender<ReceiverMessage> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ReceiverMessage>(1);

        tokio::spawn(async move {
            let mut resend_check_interval =
                tokio::time::interval(Duration::from_secs(resend_check));

            let mut keep_alive_interval = tokio::time::interval(Duration::from_secs(keep_alive));

            let mut keep_alive_timeout_flag = false;

            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                match msg {
                                    ReceiverMessage::ForwardFromRouter(packet) => {
                                        if let MqttPacketV3::Publish(publish_packet) = packet {
                                            let qos = publish_packet.fix_header.qos;
                                            let packet = MqttPacketV3::Publish(publish_packet);
                                            if qos > Some(0) {
                                                self.new_tx_qos_state_ctx(&packet).await;
                                            }
                                            if let Some(deliver_packet_tx) = self.deliver_packet_tx.as_mut() {
                                                if let Err(send_err) = deliver_packet_tx.send(SenderMessage::WritePacket(packet)).await {
                                                    warn!("tenant {} session {} write packet error, details: {}", self.tenant_identifier, self.client_identifier, send_err);
                                                }
                                            } else {
                                                warn!("tenant {} session {} deliver packet tx is None, break from the online loop", self.tenant_identifier, self.client_identifier);
                                                rx.close();
                                            }
                                        }
                                    }
                                    ReceiverMessage::Packet(packet) => {
                                        keep_alive_timeout_flag = false;
                                        if let Err(err) = self.do_process_rx_packet(&packet).await {
                                            warn!("tenant {} session {} process rx packet error, details: {}", self.tenant_identifier, self.client_identifier, err);
                                            rx.close();
                                        }
                                    }
                                    ReceiverMessage::ConnectionHasShutdown => {
                                        if let Err(err) = self.send_will_packet().await {
                                            warn!("tenant {} session {} send will packet error, details: {}", self.tenant_identifier, self.client_identifier, err);
                                        }
                                        info!("tenant {} session {} connection has shutdown, break from the online loop", self.tenant_identifier, self.client_identifier);
                                        rx.close();
                                    }
                                    ReceiverMessage::KickOff => {
                                        if let Some(deliver_packet_tx) = self.deliver_packet_tx.as_mut() {
                                            deliver_packet_tx.send(SenderMessage::ShutdownConnection).await.unwrap_or_default();
                                            rx.close();
                                        }
                                    }
                                }
                            }
                            None => {
                                println!("session receive channel closed, break from the online loop");
                                info!("session receive channel closed, break from the online loop");
                                break; // channle closed and has consumerd all bufferd message break the loop
                            }
                        }
                    }
                    _ = keep_alive_interval.tick() => {
                        if keep_alive_timeout_flag {
                            if let Err(err) = self.send_will_packet().await {
                                warn!("tenant {} session {} send will packet error, details: {}", self.tenant_identifier, self.client_identifier, err);
                            }
                            info!("tenant {} session {} keep alive timeout", self.tenant_identifier, self.client_identifier);
                            if let Some(deliver_packet_tx) = self.deliver_packet_tx.as_mut() {
                                if let Err(send_err) = deliver_packet_tx.send(SenderMessage::ShutdownConnection).await {
                                    warn!("tenant {} session {}  send connection shutdown error, details: {}", self.tenant_identifier, self.client_identifier, send_err);
                                }
                            }
                            rx.close(); // close receiver waiting to consumer buffered message
                        } else {
                            keep_alive_timeout_flag = true;
                        }
                    }
                    _ = resend_check_interval.tick() => {
                        let packets = self.qos_context.get_all_expired_packets_and_refresh_expired_time().await;
                        for packet in packets {
                            println!("resend packet {:?}", packet);
                            if let Some(deliver_packet_tx) = self.deliver_packet_tx.as_mut() {
                                if let Err(send_err) = deliver_packet_tx.send(SenderMessage::WritePacket(packet)).await {
                                    warn!("tenant {} session {} resend packet error, details: {}", self.tenant_identifier, self.client_identifier, send_err);
                                }
                            }
                        }
                    }
                }
            }
        });
        tx
    }

    async fn send_will_packet(&mut self) -> Result<()> {
        if let Some(will_message) = &self.will_message {
            let topic_name = will_message.will_topic.clone();

            let publish_packet =
                PublishPacketBuilder::new(topic_name, will_message.will_message.clone())
                    .retain(will_message.will_retain)
                    .qos(will_message.will_qos)
                    .build();

            assert!(self.deliver_packet_tx.is_some());

            self.deliver_packet_tx
                .as_ref()
                .unwrap()
                .send(SenderMessage::ForwardToRouter(
                    self.tenant_identifier.clone(),
                    MqttPacketV3::Publish(publish_packet),
                ))
                .await?;
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

    async fn do_process_rx_packet(&mut self, packet: &MqttPacketV3) -> Result<()> {
        assert!(self.deliver_packet_tx.is_some());
        match packet {
            MqttPacketV3::Publish(publish_packet) => {
                let cmd = SenderMessage::ForwardToRouter(
                    self.tenant_identifier.clone(),
                    MqttPacketV3::Publish(publish_packet.clone()),
                );
                self.deliver_packet_tx.as_mut().unwrap().send(cmd).await?;
                self.new_rx_qos_state_ctx(packet).await;
                let packet = self
                    .qos_context
                    .get_current_packet(publish_packet.variable_header.packet_identifier.unwrap())
                    .await
                    .unwrap();
                self.deliver_packet_tx
                    .as_mut()
                    .unwrap()
                    .send(SenderMessage::WritePacket(packet))
                    .await?;
            }
            MqttPacketV3::Disconnect(_) => {
                self.deliver_packet_tx
                    .as_mut()
                    .unwrap()
                    .send(SenderMessage::ShutdownConnection)
                    .await?;
            }
            MqttPacketV3::Pingreq(_) => {
                self.deliver_packet_tx
                    .as_mut()
                    .unwrap()
                    .send(SenderMessage::WritePacket(MqttPacketV3::Pingresp(
                        PingrespPacket::new(),
                    )))
                    .await?;
            }
            MqttPacketV3::Puback(puback_packet) => {
                self.qos_context
                    .next_state(puback_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .qos_context
                    .get_current_packet(puback_packet.variable_header.packet_identifier)
                    .await
                {
                    self.deliver_packet_tx
                        .as_mut()
                        .unwrap()
                        .send(SenderMessage::WritePacket(p))
                        .await?;
                }
            }
            MqttPacketV3::Pubrec(pubrec_packet) => {
                self.qos_context
                    .next_state(pubrec_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .qos_context
                    .get_current_packet(pubrec_packet.variable_header.packet_identifier)
                    .await
                {
                    self.deliver_packet_tx
                        .as_mut()
                        .unwrap()
                        .send(SenderMessage::WritePacket(p))
                        .await?;
                }
            }
            MqttPacketV3::Pubrel(pubrel_packet) => {
                self.qos_context
                    .next_state(pubrel_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .qos_context
                    .get_current_packet(pubrel_packet.variable_header.packet_identifier)
                    .await
                {
                    self.deliver_packet_tx
                        .as_mut()
                        .unwrap()
                        .send(SenderMessage::WritePacket(p))
                        .await?;
                }
            }
            MqttPacketV3::Subscribe(subscribe_packet) => {
                let subscriptions = &subscribe_packet.payload.topic_filters;
                let packet_identifier = &subscribe_packet.variable_header.packet_identifier;

                let mut retain_messages: Vec<Arc<MqttPacketV3>> = vec![];

                let mut return_code: Vec<crate::protocol::v3::suback::ReturnCode> = vec![];
                {
                    let mut topic_manager = self.topic_tree.write().await;

                    for topic in subscriptions.iter() {
                        let sub_result = topic_manager.subscription(
                            self.tenant_identifier.clone(),
                            self.client_identifier.clone(),
                            topic.topic_name.clone(),
                            topic.qos,
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
                                topic.topic_name.clone(),
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
                    self.deliver_packet_tx
                        .as_mut()
                        .unwrap()
                        .send(SenderMessage::WritePacket(packet.as_ref().clone()))
                        .await?;
                }
                self.deliver_packet_tx
                    .as_mut()
                    .unwrap()
                    .send(SenderMessage::WritePacket(MqttPacketV3::Suback(
                        SubackPacket::new(*packet_identifier, return_code),
                    )))
                    .await?;
            }
            MqttPacketV3::Unsubscribe(unsubscribe_packet) => {
                let unsub_topic_filters = &unsubscribe_packet.payload.topic_filters;
                {
                    let mut topic_manager = self.topic_tree.write().await;
                    for topic in unsub_topic_filters {
                        let _ = topic_manager.unsubscription(
                            self.tenant_identifier.clone(),
                            self.client_identifier.clone(),
                            topic.topic_name.clone(),
                        );
                    }
                }
                let unsub_ack =
                    MqttPacketV3::Unsuback(crate::protocol::v3::unsuback::UnSubackPacket::new(
                        unsubscribe_packet.variable_header.packet_identifier,
                    ));
                self.deliver_packet_tx
                    .as_mut()
                    .unwrap()
                    .send(SenderMessage::WritePacket(unsub_ack))
                    .await?;
            }
            MqttPacketV3::Pubcomp(pubcomp_packet) => {
                self.qos_context
                    .next_state(pubcomp_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .qos_context
                    .get_current_packet(pubcomp_packet.variable_header.packet_identifier)
                    .await
                {
                    self.deliver_packet_tx
                        .as_mut()
                        .unwrap()
                        .send(SenderMessage::WritePacket(p))
                        .await?;
                }
            }
            _ => {
                // Do nothing
            }
        }
        self.qos_context.clean_finished_items().await;
        Ok(())
    }
}

pub struct SessionHandle {
    session_sender: tokio::sync::mpsc::Sender<ReceiverMessage>,
    router_sender: tokio::sync::mpsc::Sender<RouterCmd>
}

impl SessionHandle {
    pub async fn new(mut session: Session, mut connection: Connection<TcpStream>, mut router_sender: tokio::sync::mpsc::Sender<RouterCmd>) -> SessionHandle {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        session.deliver_packet_tx = Some(tx);
        let sender = session.run_online_loop(10,10).await;
        let sender_cloned = sender.clone();
        let router_sender_cloned = router_sender.clone();
        tokio::spawn(async move {
            let sender = sender.clone();
            let router_sender = router_sender.clone();
            loop {
                tokio::select! {
                    packet = connection.read_packet() => {
                        if let Ok(packet) = packet {
                            if let Err(error) = sender.send(ReceiverMessage::Packet(packet)).await {
                                warn!("send packet to session error: {}", error);
                                break;
                            }
                        } else {
                            if let Err(error) = connection.shutdown().await {
                                warn!("shutdown connection error: {}", error);
                            }
                            if let Err(error) = sender.send(ReceiverMessage::ConnectionHasShutdown).await {
                                warn!("read packet error, send packet to session error: {}", error);
                            }
                            break;
                        }
                    }
                    msg_from_session = rx.recv() => {
                        match msg_from_session {
                            Some(SenderMessage::ShutdownConnection) => {
                                info!("receive shutdown connection");
                                if let Err(error) = connection.shutdown().await {
                                    warn!("shutdown connection error: {}", error);
                                }
                                rx.close(); // close waiting to consumer buffered data
                            }
                            Some(SenderMessage::WritePacket(packet)) => {
                                if let Err(error) = connection.write_packet(&packet).await {
                                    warn!("write packet to connection error: {}", error);
                                    if let Err(error) = sender.send(ReceiverMessage::ConnectionHasShutdown).await {
                                        warn!("write packet error, send packet to session error: {}", error);
                                    }
                                    rx.close();
                                }
                            }
                            Some(SenderMessage::ForwardToRouter(tenant_identifier,packet)) => {
                                if let Err(error) = router_sender.send(RouterCmd::RoutePacket(tenant_identifier, packet)).await {
                                    panic!("forward packet to router error: {}", error);
                                }
                            }
                            None => {
                                info!("session deliver channel has closed, exit");
                                break;
                            }
                            
                        }
                    }
                }
            }
        });

        SessionHandle {
            session_sender: sender_cloned,
            router_sender: router_sender_cloned
        }
    }

    pub async fn forward_packet_to_session(&self, packet: MqttPacketV3) -> Result<()> {
        self.session_sender.send(ReceiverMessage::ForwardFromRouter(packet)).await?;
        Ok(())
    }

}

pub struct SessionManager {
    session_table: RwLock<HashMap<String, SessionHandle>>,
    tenant_identifier: String
}

impl SessionManager {
    pub async fn register(&mut self, client_identifier: String, session_handle:SessionHandle) {
        let mut session_table = self.session_table.write().await;
        session_table.insert(client_identifier, session_handle);
    }

    pub async fn send_packet(&self, client_identifier: String, packet: &MqttPacketV3) -> Result<()> {
        let session_table = self.session_table.read().await;
        if let Some(handle) = session_table.get(&client_identifier) {
            handle.forward_packet_to_session(packet.clone()).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::RwLock;

    use crate::{session::{Session, SenderMessage, ReceiverMessage}, qos_context::QosContext, topic::TopicManager, protocol::{v3::{publish::PublishPacketBuilder, fixed_header::FixHeader, puback::{VariableHeader, PubAckPacket}, pubrec::PubRecPacket, pubcomp::PubCompPacket, pubrel::PubRelPacket}, MqttPacketV3, PacketType}};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_keep_alive_timeout_without_will_message() {
        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (deliver_packet_tx,mut deliver_packet_rx) = tokio::sync::mpsc::channel(10);

        let mut session = Session{
            will_message: None,
            client_identifier: "clinet_a".to_string(),
            tenant_identifier: "tenant_a".to_string(),
            topic_tree: Arc::new(RwLock::new(TopicManager::new())),
            qos_context: QosContext::new(Duration::from_secs(resend_duration_secs)),
            subscription_topics: RwLock::new(vec![]),
            deliver_packet_tx: Some(deliver_packet_tx)
        };

        let rx = session.run_online_loop(keep_live_duration_secs,resend_duration_secs).await;

        let msg = deliver_packet_rx.recv().await.unwrap();
        match msg {
            SenderMessage::ShutdownConnection => {
                assert!(true);
            }
            _ => {
                assert!(false);
            }
        };
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_receive_connection_shutdown_exit_online_loop() {
        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (deliver_packet_tx,mut deliver_packet_rx) = tokio::sync::mpsc::channel(10);

        let mut session = Session{
            will_message: None,
            client_identifier: "clinet_a".to_string(),
            tenant_identifier: "tenant_a".to_string(),
            topic_tree: Arc::new(RwLock::new(TopicManager::new())),
            qos_context: QosContext::new(Duration::from_secs(resend_duration_secs)),
            subscription_topics: RwLock::new(vec![]),
            deliver_packet_tx: Some(deliver_packet_tx)
        };

        let tx = session.run_online_loop(keep_live_duration_secs,resend_duration_secs).await;
        tx.send(ReceiverMessage::ConnectionHasShutdown).await.unwrap();
        let receive_msg = deliver_packet_rx.recv().await;

        assert!(receive_msg.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_qos_1_receive_process() {
        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (deliver_packet_tx,mut deliver_packet_rx) = tokio::sync::mpsc::channel(10);

        let mut session = Session{
            will_message: None,
            client_identifier: "clinet_a".to_string(),
            tenant_identifier: "tenant_a".to_string(),
            topic_tree: Arc::new(RwLock::new(TopicManager::new())),
            qos_context: QosContext::new(Duration::from_secs(10)),
            subscription_topics: RwLock::new(vec![]),
            deliver_packet_tx: Some(deliver_packet_tx)
        };

        let qos_1_publish_packet = PublishPacketBuilder::new("a/b".to_string(), vec![0x01])
            .qos(1)
            .packet_identifier(0x01)
            .build();

        let packet = MqttPacketV3::Publish(qos_1_publish_packet);
        
        let tx = session.run_online_loop(keep_live_duration_secs,resend_duration_secs).await;

        tx.send(ReceiverMessage::Packet(packet)).await.unwrap();

        // assert forward publish packet to other client
        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::ForwardToRouter(teanant_identifier,packet) => {
                match packet {
                    MqttPacketV3::Publish(publish_packet) => {
                        assert_eq!(publish_packet.variable_header.packet_identifier.unwrap(), 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }
        //

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Puback(packet) => {
                        assert_eq!(packet.variable_header.packet_identifier, 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }

    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_qos_2_receive_process() {
        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (deliver_packet_tx,mut deliver_packet_rx) = tokio::sync::mpsc::channel(10);

        let mut session = Session{
            will_message: None,
            client_identifier: "clinet_a".to_string(),
            tenant_identifier: "tenant_a".to_string(),
            topic_tree: Arc::new(RwLock::new(TopicManager::new())),
            qos_context: QosContext::new(Duration::from_secs(10)),
            subscription_topics: RwLock::new(vec![]),
            deliver_packet_tx: Some(deliver_packet_tx)
        };
        let qos_2_publish_packet = PublishPacketBuilder::new("a/b".to_string(), vec![0x01])
            .qos(2)
            .packet_identifier(0x01)
            .build();
        
        let packet = MqttPacketV3::Publish(qos_2_publish_packet);

        let pubrel_packet = MqttPacketV3::Pubrel(PubRelPacket::new(0x01));

        let tx = session.run_online_loop(keep_live_duration_secs,resend_duration_secs).await;

        tx.send(ReceiverMessage::Packet(packet)).await.unwrap();

        // assert forward publish packet to other client
        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::ForwardToRouter(teanant_identifier,packet) => {
                match packet {
                    MqttPacketV3::Publish(publish_packet) => {
                        assert_eq!(publish_packet.variable_header.packet_identifier.unwrap(), 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }
        //

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Pubrec(pubrec_packet) => {
                        assert_eq!(pubrec_packet.variable_header.packet_identifier, 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }


        tx.send(ReceiverMessage::Packet(pubrel_packet)).await.unwrap(); // send pubrel packet

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Pubcomp(pubcomp_packet) => {
                        assert_eq!(pubcomp_packet.variable_header.packet_identifier, 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }

    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_qos_2_receive_resend_pubrec_process() {
        let keep_live_duration_secs = 10;

        let resend_duration_secs = 5;

        let (deliver_packet_tx,mut deliver_packet_rx) = tokio::sync::mpsc::channel(10);

        let mut session = Session{
            will_message: None,
            client_identifier: "clinet_a".to_string(),
            tenant_identifier: "tenant_a".to_string(),
            topic_tree: Arc::new(RwLock::new(TopicManager::new())),
            qos_context: QosContext::new(Duration::from_secs(2)),
            subscription_topics: RwLock::new(vec![]),
            deliver_packet_tx: Some(deliver_packet_tx)
        };

        let qos_2_publish_packet = PublishPacketBuilder::new("a/b".to_string(), vec![0x01])
            .qos(2)
            .packet_identifier(0x01)
            .build();

        let packet = MqttPacketV3::Publish(qos_2_publish_packet);

        let pubrel_packet = MqttPacketV3::Pubrel(PubRelPacket::new(0x01));

        let tx = session.run_online_loop(keep_live_duration_secs,resend_duration_secs).await;

        tx.send(ReceiverMessage::Packet(packet)).await.unwrap();

        // assert forward publish packet to other client
        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::ForwardToRouter(teanant_identifier,packet) => {
                match packet {
                    MqttPacketV3::Publish(publish_packet) => {
                        assert_eq!(publish_packet.variable_header.packet_identifier.unwrap(), 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }
        //

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Pubrec(pubrec_packet) => {
                        assert_eq!(pubrec_packet.variable_header.packet_identifier, 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Pubrec(pubrec_packet) => {
                        assert_eq!(pubrec_packet.variable_header.packet_identifier, 0x01);
                        assert_eq!(pubrec_packet.fix_header.dup, Some(1));
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }

        tx.send(ReceiverMessage::Packet(pubrel_packet)).await.unwrap(); // send pubrel packet

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Pubcomp(pubcomp_packet) => {
                        assert_eq!(pubcomp_packet.variable_header.packet_identifier, 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }

    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_qos_tx_2_resend_publish_packet() {
        let keep_live_duration_secs = 10;

        let resend_duration_secs = 5;

        let (deliver_packet_tx,mut deliver_packet_rx) = tokio::sync::mpsc::channel(10);

        let mut session = Session{
            will_message: None,
            client_identifier: "clinet_a".to_string(),
            tenant_identifier: "tenant_a".to_string(),
            topic_tree: Arc::new(RwLock::new(TopicManager::new())),
            qos_context: QosContext::new(Duration::from_secs(2)),
            subscription_topics: RwLock::new(vec![]),
            deliver_packet_tx: Some(deliver_packet_tx)
        };

        let qos_2_publish_packet = PublishPacketBuilder::new("a/b".to_string(), vec![0x01])
            .qos(2)
            .packet_identifier(0x01)
            .build();

        let packet = MqttPacketV3::Publish(qos_2_publish_packet);

        let pubrec_packet = MqttPacketV3::Pubrec(PubRecPacket::new(0x01));
        let pubrel_packet = MqttPacketV3::Pubrel(PubRelPacket::new(0x01));
        let pubcomp_packet = MqttPacketV3::Pubcomp(PubCompPacket::new(0x01));

        let tx = session.run_online_loop(keep_live_duration_secs,resend_duration_secs).await;

        tx.send(ReceiverMessage::ForwardFromRouter(packet)).await.unwrap();

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Publish(publish_packet) => {
                        assert_eq!(publish_packet.variable_header.packet_identifier.unwrap(), 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Publish(publish_packet) => {
                        assert_eq!(publish_packet.variable_header.packet_identifier.unwrap(), 0x01);
                        assert_eq!(publish_packet.fix_header.dup, Some(1));
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }

        tx.send(ReceiverMessage::Packet(pubrec_packet)).await.unwrap();

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Pubrel(pubrel_packet) => {
                        assert_eq!(pubrel_packet.variable_header.packet_identifier, 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }
        tx.send(ReceiverMessage::Packet(pubcomp_packet)).await.unwrap();
    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_qos_tx_2_resend_pubrel_packet() {
        let keep_live_duration_secs = 10;

        let resend_duration_secs = 5;

        let (deliver_packet_tx,mut deliver_packet_rx) = tokio::sync::mpsc::channel(10);

        let mut session = Session{
            will_message: None,
            client_identifier: "clinet_a".to_string(),
            tenant_identifier: "tenant_a".to_string(),
            topic_tree: Arc::new(RwLock::new(TopicManager::new())),
            qos_context: QosContext::new(Duration::from_secs(2)),
            subscription_topics: RwLock::new(vec![]),
            deliver_packet_tx: Some(deliver_packet_tx)
        };

        let qos_2_publish_packet = PublishPacketBuilder::new("a/b".to_string(), vec![0x01])
            .qos(2)
            .packet_identifier(0x01)
            .build();

        let packet = MqttPacketV3::Publish(qos_2_publish_packet);

        let pubrec_packet = MqttPacketV3::Pubrec(PubRecPacket::new(0x01));
        let pubrel_packet = MqttPacketV3::Pubrel(PubRelPacket::new(0x01));
        let pubcomp_packet = MqttPacketV3::Pubcomp(PubCompPacket::new(0x01));

        let tx = session.run_online_loop(keep_live_duration_secs,resend_duration_secs).await;

        tx.send(ReceiverMessage::ForwardFromRouter(packet)).await.unwrap();

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Publish(publish_packet) => {
                        assert_eq!(publish_packet.variable_header.packet_identifier.unwrap(), 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }

        tx.send(ReceiverMessage::Packet(pubrec_packet)).await.unwrap();

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Pubrel(pubrel_packet) => {
                        assert_eq!(pubrel_packet.variable_header.packet_identifier, 0x01);
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }

        let receive_msg = deliver_packet_rx.recv().await.unwrap();

        match receive_msg {
            SenderMessage::WritePacket(packet) => {
                match packet {
                    MqttPacketV3::Pubrel(pubrel_packet) => {
                        assert_eq!(pubrel_packet.variable_header.packet_identifier, 0x01);
                        assert_eq!(pubrel_packet.fix_header.dup, Some(1));
                    }
                    _ => {
                        assert!(false);
                    }
                }
            }
            _ => {
                assert!(false);
            }
        }
        tx.send(ReceiverMessage::Packet(pubcomp_packet)).await.unwrap();
    }
}