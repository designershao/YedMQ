use std::collections::HashMap;

use crate::protocol::{MqttPacketV3, v3::{pubcomp::PubCompPacket, pubrel::PubRelPacket}};

struct QosContext {
    inner: HashMap<u16, QosContextItem>
}

impl QosContext {

    pub fn add_qos_ctx_item(&mut self, packet_identifier:u16, item: QosContextItem) {
        self.inner.insert(packet_identifier, item);
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

    // According current state , get the current state packet
    pub fn current_packet(&self) -> Option<&MqttPacketV3> {
        match &self.packet {
            Some(packet) => Some(&packet),
            None => None
        }
    }

}