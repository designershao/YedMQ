use std::{sync::Arc, time::Duration};

use tokio::sync::{RwLock, mpsc::Sender};

use crate::{protocol::{MqttPacketV3, v3::{pingresp::PingrespPacket, suback::SubackPacket}}, qos_context::QosContext, topic::TopicManager};


pub enum SenderMessage {
    WritePacket(MqttPacketV3),
    ForwardToPacket(String,MqttPacketV3),
    ShutdownConnection
}

pub enum ReceiverMessage {
    ForwardFromPacket(MqttPacketV3),
    Packet(MqttPacketV3)
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

    // The session subscribed topics
    subscription_topics: RwLock<Vec<String>>,

    // The QOS context, track all in flights qos packet.
    qos_context: QosContext,

    // Session cmd receiver
    delivier_packet_tx: Option<Sender<SenderMessage>>,

    // Topic tree
    topic_tree: Arc<RwLock<TopicManager>>,

}


impl Session
 {

    async fn run_online_loop(mut self, keep_alive:u64, resend_check:u64) -> Sender<ReceiverMessage> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ReceiverMessage>(1);

        tokio::spawn(async move {
            let mut resend_check_interval = tokio::time::interval(Duration::from_secs(resend_check));

            let mut keep_alive_interval = tokio::time::interval(Duration::from_secs(keep_alive));

            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                match msg {
                                    ReceiverMessage::ForwardFromPacket(packet) => {
                                        if let MqttPacketV3::Publish(publish_packet) = packet {
                                            let qos = publish_packet.fix_header.qos;
                                            let packet = MqttPacketV3::Publish(publish_packet);
                                            if qos > Some(0) {
                                                self.new_tx_qos_state_ctx(&packet).await;
                                            }
                                            self.delivier_packet_tx.as_mut().unwrap().send(SenderMessage::WritePacket(packet)).await;
                                        } 
                                    }
                                    ReceiverMessage::Packet(packet) => {
                                        self.do_process_rx_packet(&packet).await;
                                    }
                                }
                            }
                            None => {
                                break; // rx channel closed, exit the loop
                            }
                        }
                    }
                    _ = keep_alive_interval.tick() => {}
                    _ = resend_check_interval.tick() => {}
                }
            }
        });

        tx
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

    async fn do_process_rx_packet(&mut self, packet: &MqttPacketV3) {
        match packet {
            MqttPacketV3::Publish(publish_packet) => {
                let cmd = SenderMessage::ForwardToPacket(self.tenant_identifier.clone(), MqttPacketV3::Publish(publish_packet.clone()));
                self.delivier_packet_tx.as_mut().unwrap().send(cmd).await;
                self.new_rx_qos_state_ctx(packet).await;
                let packet = self.qos_context.get_current_packet(publish_packet.variable_header.packet_identifier.unwrap()).await.unwrap();
                self.delivier_packet_tx.as_mut().unwrap().send(SenderMessage::WritePacket(packet)).await;
            }
            MqttPacketV3::Disconnect(_) => {
                self.delivier_packet_tx.as_mut().unwrap().send(SenderMessage::ShutdownConnection).await;
            }
            MqttPacketV3::Pingreq(_) => {
                self.delivier_packet_tx.as_mut().unwrap().send(SenderMessage::WritePacket(MqttPacketV3::Pingresp(PingrespPacket::new()))).await;
            }
            MqttPacketV3::Puback(puback_packet) => {
                self.qos_context.next_state(puback_packet.variable_header.packet_identifier).await;
                if let Some(p) = self.qos_context.get_current_packet(puback_packet.variable_header.packet_identifier).await {
                    self.delivier_packet_tx.as_mut().unwrap().send(SenderMessage::WritePacket(p)).await;
                }
            }
            MqttPacketV3::Pubrec(pubrec_packet) => {
                self.qos_context.next_state(pubrec_packet.variable_header.packet_identifier).await;
                if let Some(p) = self.qos_context.get_current_packet(pubrec_packet.variable_header.packet_identifier).await {
                    self.delivier_packet_tx.as_mut().unwrap().send(SenderMessage::WritePacket(p)).await;
                }
            }
            MqttPacketV3::Pubrel(pubrel_packet) => {
                self.qos_context.next_state(pubrel_packet.variable_header.packet_identifier).await;
                if let Some(p) = self.qos_context.get_current_packet(pubrel_packet.variable_header.packet_identifier).await {
                    self.delivier_packet_tx.as_mut().unwrap().send(SenderMessage::WritePacket(p)).await;
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
                    self.delivier_packet_tx.as_mut().unwrap().send(SenderMessage::WritePacket(packet.as_ref().clone())).await;
                }
                self.delivier_packet_tx.as_mut().unwrap().send(
                    SenderMessage::WritePacket(
                    MqttPacketV3::Suback(
                    SubackPacket::new(*packet_identifier, return_code)))).await;
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
                self.delivier_packet_tx.as_mut().unwrap().send(SenderMessage::WritePacket(unsub_ack)).await;
            }
            MqttPacketV3::Pubcomp(pubcomp_packet) => {
                self.qos_context.next_state(pubcomp_packet.variable_header.packet_identifier).await;
                if let Some(p) = self.qos_context.get_current_packet(pubcomp_packet.variable_header.packet_identifier).await {
                    self.delivier_packet_tx.as_mut().unwrap().send(SenderMessage::WritePacket(p)).await;
                }
            }
            _ => {
                // Do nothing
            }
        }
        self.qos_context.clean_finished_items().await;
    }
}