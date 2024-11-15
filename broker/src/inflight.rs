use std::{collections::HashMap, time::Duration};

use tokio::sync::RwLock;

use samoye_mqtt::{MqttPacketV3, v3::{pubcomp::PubCompPacket, pubrel::PubRelPacket, puback::PubAckPacket, pubrec::PubRecPacket}};

pub struct Inflight {
    inner: RwLock<HashMap<u16, InflightItem>>,
    expired_duration: Duration
}

impl Inflight {

    pub fn new(expired_duration: Duration) -> Inflight {
        Inflight {
            inner: RwLock::new(HashMap::new()),
            expired_duration
        }
    }

    pub async fn register_with_rx_packet(&self, packet: &MqttPacketV3) {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let packet_identifier = publish_packet.variable_header.packet_identifier;
            if let Some(packet_identifier) = packet_identifier {
                let qos = publish_packet.fix_header.qos.unwrap();
                if qos == 2 {
                    let item = InflightItemBuilder::new(
                        packet_identifier,
                        InflightState::WaitPubrel
                    ).packet(&MqttPacketV3::Pubrec(PubRecPacket::new(packet_identifier))).build();
                    let mut inner = self.inner.write().await;
                    inner.insert(packet_identifier, item);
                } 
                if qos == 1 {
                    let item = InflightItemBuilder::new(
                        packet_identifier,
                        InflightState::Finish
                    ).packet(&MqttPacketV3::Puback(PubAckPacket::new(packet_identifier))).build();
                    let mut inner = self.inner.write().await;
                    inner.insert(packet_identifier, item);
                }
            } 
        }
    }

    pub async fn register_with_tx_packet(&self, packet: &MqttPacketV3) {
        if let MqttPacketV3::Publish(publish_packet) = packet {
            let packet_identifier = publish_packet.variable_header.packet_identifier;
            if let Some(packet_identifier) = packet_identifier {
                let qos = publish_packet.fix_header.qos.unwrap();
                if qos == 2 {
                    let item = InflightItemBuilder::new(
                        packet_identifier,
                        InflightState::WaitPubrec
                    ).packet(packet).build();
                    let mut inner = self.inner.write().await;
                    inner.insert(packet_identifier, item);
                } 
                if qos == 1 {
                    let item = InflightItemBuilder::new(
                        packet_identifier,
                        InflightState::WaitPuback
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

    // Get all packet which should be resend to the client and refresh expired time
    pub async fn get_all_expired_packets_and_refresh_expired_time(&self) -> Vec<MqttPacketV3> {
        let mut result_vec:Vec<MqttPacketV3> = vec![];
        let mut inner = self.inner.write().await;
        for item in inner.values_mut() {
            if item.last_modified + self.expired_duration.as_secs() < std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs() {
                if let Some(packet) = item.current_packet() {
                    let mut packet = packet.clone();
                    packet.set_dup(1); // all expired packet should set dup to true
                    result_vec.push(packet.clone());
                }
                item.last_modified = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs();
            }
        }    
        result_vec
    }

    // Get all packet which should be resend to the client
    pub async fn get_all_expired_packets(&self) -> Vec<MqttPacketV3> {
        let mut result_vec:Vec<MqttPacketV3> = vec![];
        let inner = self.inner.read().await;
        for item in inner.values() {
            if item.last_modified + self.expired_duration.as_secs() < std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs() {
                if let Some(packet) = item.current_packet() {
                    let mut packet = packet.clone();
                    packet.set_dup(1); // all expired packet should set dup to true
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
    pub async fn clean_finished_items(&mut self) {
        let mut inner = self.inner.write().await;
        inner.retain(|_, v| v.state != InflightState::Finish);
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
enum InflightState {
    WaitPubrel,
    WaitPubcomp,
    WaitPubrec,
    WaitPuback,
    Finish
}

struct InflightItem {
    qos: u8,
    packet_identifier: u16,
    state: InflightState,
    packet: Option<MqttPacketV3>,
    last_modified: u64
}

struct InflightItemBuilder {
    qos: u8,
    packet_identifier: u16,
    state: InflightState,
    packet: Option<MqttPacketV3>,
}

impl InflightItemBuilder {

    fn new(packet_identifier: u16, state: InflightState) -> InflightItemBuilder {

        let qos = match state {
            InflightState::WaitPubrel => 2,
            InflightState::WaitPubcomp => 2,
            InflightState::WaitPubrec => 2,
            InflightState::WaitPuback => 1,
            InflightState::Finish => 0
        };

        InflightItemBuilder {
            qos,
            packet_identifier,
            state,
            packet: None
        }

    }

    fn packet(mut self, packet: &MqttPacketV3) -> InflightItemBuilder {
        self.packet = Some(packet.clone());
        if let MqttPacketV3::Publish(p) = packet {
            if let Some(qos) = p.fix_header.qos {
                self.qos = qos as u8;
            }
        }
        self
    }

    fn build(&self) -> InflightItem {
        InflightItem {
            qos: self.qos,
            packet_identifier: self.packet_identifier,
            state: self.state.clone(),
            packet: self.packet.clone(),
            last_modified: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
        }
    }

}

impl InflightItem {

    // According current state , set the next state and set the next send packet
    pub fn to_next(&mut self) {
        let (next_state, next_packet) = match self.state {
            InflightState::WaitPubrel => 
                (InflightState::Finish, Some(MqttPacketV3::Pubcomp(PubCompPacket::new(self.packet_identifier)))),
            InflightState::WaitPubcomp => 
                (InflightState::Finish, None),
            InflightState::WaitPubrec => 
                (InflightState::WaitPubcomp, Some(MqttPacketV3::Pubrel(PubRelPacket::new(self.packet_identifier)))),
            InflightState::WaitPuback => 
                (InflightState::Finish, Some(MqttPacketV3::Puback(PubAckPacket::new(self.packet_identifier)))),
            InflightState::Finish => (InflightState::Finish, None)
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