use std::{collections::HashMap, time::Duration};

use tokio::sync::RwLock;

use crate::protocol::{MqttPacketV3, v3::{pubcomp::PubCompPacket, pubrel::PubRelPacket, puback::PubAckPacket, pubrec::PubRecPacket}};

pub struct QosContext {
    inner: RwLock<HashMap<u16, QosContextItem>>,
    expired_duration: Duration
}

impl QosContext {

    pub fn new(expired_duration: Duration) -> QosContext {
        QosContext {
            inner: RwLock::new(HashMap::new()),
            expired_duration
        }
    }

    pub async fn register_with_rx_packet(&mut self, packet: &MqttPacketV3) {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let packet_identifier = publish_packet.variable_header.packet_identifier;
            if let Some(packet_identifier) = packet_identifier {
                let qos = publish_packet.fix_header.qos.unwrap();
                if qos == 2 {
                    let item = QosPacketItemBuilder::new(
                        packet_identifier,
                        QosContextItemState::WaitPubrel
                    ).packet(&MqttPacketV3::Pubrec(PubRecPacket::new(packet_identifier))).build();
                    let mut inner = self.inner.write().await;
                    inner.insert(packet_identifier, item);
                } 
                if qos == 1 {
                    let item = QosPacketItemBuilder::new(
                        packet_identifier,
                        QosContextItemState::Finish
                    ).packet(&MqttPacketV3::Puback(PubAckPacket::new(packet_identifier))).build();
                    let mut inner = self.inner.write().await;
                    inner.insert(packet_identifier, item);
                }
            } 
        }
    }

    pub async fn register_with_tx_packet(&mut self, packet: &MqttPacketV3) {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let packet_identifier = publish_packet.variable_header.packet_identifier;
            if let Some(packet_identifier) = packet_identifier {
                let qos = publish_packet.fix_header.qos.unwrap();
                if qos == 2 {
                    let item = QosPacketItemBuilder::new(
                        packet_identifier,
                        QosContextItemState::WaitPubrec
                    ).packet(packet).build();
                    let mut inner = self.inner.write().await;
                    inner.insert(packet_identifier, item);
                } 
                if qos == 1 {
                    let item = QosPacketItemBuilder::new(
                        packet_identifier,
                        QosContextItemState::WaitPuback
                    ).packet(packet).build();
                    let mut inner = self.inner.write().await;
                    inner.insert(packet_identifier, item);
                }
            }
        }
    }

    // Remove qos context with packet identifier
    pub fn del(&mut self, packet_identifier: u16) {
        self.inner.blocking_write().remove(&packet_identifier);
    }

    // Check the packet identifier in used
    pub fn contains_packet_identifier(&self, packet_identifier:u16) -> bool {
        self.inner.blocking_read().contains_key(&packet_identifier)
    }

    // Get all packet which should be resend to the client
    pub async fn get_all_expired_packets(&self) -> Vec<MqttPacketV3> {
        let mut result_vec:Vec<MqttPacketV3> = vec![];
        let inner = self.inner.read().await;
        for item in inner.values() {
            if item.last_modified > std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs() + self.expired_duration.as_secs() {
                if let Some(packet) = item.current_packet() {
                    result_vec.push(packet.clone());
                }
            }
        }    
        result_vec
    }

    pub async fn get_current_packet(&self, packet_identifier: u16) -> Option<MqttPacketV3> {
        let inner = self.inner.read().await;
        let binding = inner;
        let ctx = binding.get(&packet_identifier);
        if let Some(ctx_item) = ctx {
            if let Some(packet) = ctx_item.current_packet() {
                Some(packet.clone())
            } else{
                None
            }
        } else {
            None
        }
    }

    pub async fn next_state(&mut self, packet_identifier: u16) {
        let mut binding = self.inner.write().await;
        let ctx = binding.get_mut(&packet_identifier);
        if let Some(ctx_item) = ctx {
            ctx_item.to_next();
        }
    }

    // Clean all finished qos packet identifier
    fn clean_finished_items(&mut self) {
        self.inner.blocking_write().retain(|_, v| v.last_modified < std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs());
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
enum QosContextItemState {
    WaitPubrel,
    WaitPubcomp,
    WaitPubrec,
    WaitPuback,
    Finish
}

struct QosContextItem {
    qos: u8,
    packet_identifier: u16,
    state: QosContextItemState,
    packet: Option<MqttPacketV3>,
    last_modified: u64
}

struct QosPacketItemBuilder {
    qos: u8,
    packet_identifier: u16,
    state: QosContextItemState,
    packet: Option<MqttPacketV3>,
}

impl QosPacketItemBuilder {

    fn new(packet_identifier: u16, state: QosContextItemState) -> QosPacketItemBuilder {

        let qos = match state {
            QosContextItemState::WaitPubrel => 2,
            QosContextItemState::WaitPubcomp => 2,
            QosContextItemState::WaitPubrec => 2,
            QosContextItemState::WaitPuback => 1,
            QosContextItemState::Finish => 0
        };

        QosPacketItemBuilder {
            qos,
            packet_identifier,
            state,
            packet: None
        }

    }

    fn packet(mut self, packet: &MqttPacketV3) -> QosPacketItemBuilder {
        self.packet = Some(packet.clone());
        if let MqttPacketV3::Publish(p) = packet {
            if let Some(qos) = p.fix_header.qos {
                self.qos = qos as u8;
            }
        }
        self
    }

    fn build(&self) -> QosContextItem {
        QosContextItem {
            qos: self.qos,
            packet_identifier: self.packet_identifier,
            state: self.state.clone(),
            packet: self.packet.clone(),
            last_modified: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
        }
    }

}

impl QosContextItem {

    // According current state , set the next state and set the next send packet
    pub fn to_next(&mut self) {
        let (next_state, next_packet) = match self.state {
            QosContextItemState::WaitPubrel => 
                (QosContextItemState::Finish, Some(MqttPacketV3::Pubcomp(PubCompPacket::new(self.packet_identifier)))),
            QosContextItemState::WaitPubcomp => 
                (QosContextItemState::Finish, None),
            QosContextItemState::WaitPubrec => 
                (QosContextItemState::WaitPubcomp, Some(MqttPacketV3::Pubrel(PubRelPacket::new(self.packet_identifier)))),
            QosContextItemState::WaitPuback => 
                (QosContextItemState::Finish, Some(MqttPacketV3::Puback(PubAckPacket::new(self.packet_identifier)))),
            QosContextItemState::Finish => (QosContextItemState::Finish, None)
        };
        self.state = next_state;
        self.packet = next_packet;
        self.last_modified = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    }

    // According current state , get the should send packet at current state
    pub fn current_packet(&self) -> Option<&MqttPacketV3> {
        match &self.packet {
            Some(packet) => Some(&packet),
            None => None
        }
    }

}


#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::protocol::{MqttPacketV3, v3::publish::PublishPacketBuilder};

    use super::{QosPacketItemBuilder, QosContextItemState, QosContext};


    #[test]
    fn test_qos_ctx_builder() {
        let builder = QosPacketItemBuilder::new(10, QosContextItemState::WaitPubrel);
        let item = builder.build();

        assert_eq!(2, item.qos);
        assert_eq!(10, item.packet_identifier);
    }

    #[test]
    fn test_qos_ctx_item_qos_1_next_state() {
        let builder = QosPacketItemBuilder::new(10, QosContextItemState::WaitPuback);
        let mut item = builder.build();
        item.to_next();

        assert_eq!(QosContextItemState::Finish, item.state);
        assert_eq!(true, item.current_packet().is_some());
    }

    #[test]
    fn test_qos_ctx_item_qos_2_next_state() {
        let builder = QosPacketItemBuilder::new(10, QosContextItemState::WaitPubrec);
        let mut item = builder.build();
        item.to_next();
        let packet = item.current_packet();
        assert_eq!(true, packet.is_some());
        if let MqttPacketV3::Pubrel(p) = packet.unwrap() {
            assert_eq!(10, p.variable_header.packet_identifier);
        } else {
            assert!(false);
        }

        item.to_next();
        let packet = item.current_packet();
        assert_eq!(true, packet.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_tx_qos_context_register() {
        let mut context = QosContext::new(Duration::from_secs(10));
        let packet_builder = PublishPacketBuilder::new("a/b".to_string(), vec![0x01]);
        let packet = packet_builder.qos(1).packet_identifier(10).build();
        
        context.register_with_tx_packet(&MqttPacketV3::Publish(packet)).await;

        let p = context.get_current_packet(10).await;

        assert_eq!(true, p.is_some());

        let packet = p.unwrap();

        if let MqttPacketV3::Publish(packet) = packet {
            assert_eq!(1, packet.fix_header.qos.unwrap());
            assert_eq!("a/b", packet.variable_header.topic_name);
            assert_eq!(vec![0x01], packet.payload.payload);
        } else {
            assert!(false);
        }

    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_tx_qos_context_qos1_next_state() {
        let mut context = QosContext::new(Duration::from_secs(10));
        let packet_builder = PublishPacketBuilder::new("a/b".to_string(), vec![0x01]);
        let packet = packet_builder.qos(1).packet_identifier(10).build();
        
        context.register_with_tx_packet(&MqttPacketV3::Publish(packet)).await;
        context.next_state(10).await;

        let p = context.get_current_packet(10).await;
        if let Some(MqttPacketV3::Puback(packet)) = p {
            assert_eq!(10, packet.variable_header.packet_identifier);
        } else {
            assert!(false);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_tx_qos_context_qos2_next_state() {
        let mut context = QosContext::new(Duration::from_secs(10));
        let packet_builder = PublishPacketBuilder::new("a/b".to_string(), vec![0x01]);
        let packet = packet_builder.qos(2).packet_identifier(10).build();
        
        context.register_with_tx_packet(&MqttPacketV3::Publish(packet)).await;
        context.next_state(10).await;

        let p = context.get_current_packet(10).await;

        if let Some(MqttPacketV3::Pubrel(packet)) = p {
            assert_eq!(10, packet.variable_header.packet_identifier);
        } else {
            assert!(false);
        }

        context.next_state(10).await;

        let p = context.get_current_packet(10).await;

        assert_eq!(true, p.is_none());
    }

}