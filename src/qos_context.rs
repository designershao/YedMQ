use std::collections::HashMap;

use crate::protocol::{MqttPacketV3, v3::{pubcomp::PubCompPacket, pubrel::PubRelPacket}};

struct QosContext {
    inner: HashMap<u16, QosContextItem>
}

impl QosContext {

    pub fn add_qos_ctx_item(&mut self, packet_identifier:u16, item: QosContextItem) {
        self.inner.insert(packet_identifier, item);
    }

    // Get qos context item with packet identifier
    pub fn get(&mut self, packet_identifier:u16) -> Option<&mut QosContextItem> {
        self.inner.get_mut(&packet_identifier)
    }

    // Check the packet identifier in used
    pub fn contains_packet_identifier(&self, packet_identifier:u16) -> bool {
        self.inner.contains_key(&packet_identifier)
    }

    // Get expired qos context item for resending the packet
    pub fn get_expired_qos_ctx_item(&mut self) -> Vec<&QosContextItem> {
        let mut result_vec:Vec<&QosContextItem> = vec![];
        for item in self.inner.values() {
            if item.last_modified < std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs() {
                result_vec.push(item);
            }
        }    
        result_vec
    }

    // Clean all finished qos packet identifier
    fn clean_finished_items(&mut self) {
        self.inner.retain(|_, v| v.last_modified < std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs());
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

    fn packet(mut self, packet: MqttPacketV3) -> QosPacketItemBuilder {
        self.packet = Some(packet);
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
            QosContextItemState::WaitPubcomp => (QosContextItemState::Finish, None),
            QosContextItemState::WaitPubrec => 
                (QosContextItemState::WaitPubcomp, Some(MqttPacketV3::Pubrel(PubRelPacket::new(self.packet_identifier)))),
            QosContextItemState::WaitPuback => (QosContextItemState::Finish, None),
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
    use crate::protocol::MqttPacketV3;

    use super::{QosPacketItemBuilder, QosContextItemState};


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
        assert_eq!(true, item.current_packet().is_none());
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
}