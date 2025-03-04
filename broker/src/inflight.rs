use std::{collections::HashMap, time::Duration};

use tokio::sync::RwLock;

use yedmq_mqtt::{MqttPacketV3, v3::{pubcomp::PubCompPacket, pubrel::PubRelPacket, puback::PubAckPacket, pubrec::PubRecPacket}};

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

    // Get all packet which should be resend to the client and refresh expired time
    pub async fn get_all_expired_packets_and_refresh_expired_time(&self) -> Vec<(u16,MqttPacketV3)> {
        let mut result_vec:Vec<(u16,MqttPacketV3)> = vec![];
        let mut inner = self.inner.write().await;
        for item in inner.values_mut() {
            if item.last_modified + self.expired_duration.as_secs() < std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs() {
                if let Some(packet) = item.current_packet() {
                    let mut packet = packet.clone();
                    packet.set_dup(1); // all expired packet should set dup to true
                    result_vec.push((item.packet_identifier, packet.clone()));
                }
                item.last_modified = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs();
            }
        }    
        result_vec
    }

    pub async fn get_next_state_packet(&self, packet_identifier: u16) -> Option<MqttPacketV3> {
        let inner = self.inner.read().await;
        let binding = inner;
        let ctx = binding.get(&packet_identifier);
        if let Some(ctx_item) = ctx {
            ctx_item.next_packet()
        } else {
            None
        }
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
            packet_identifier: self.packet_identifier,
            state: self.state.clone(),
            packet: self.packet.clone(),
            last_modified: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
        }
    }

}

impl InflightItem {

    pub fn next_packet(&self) -> Option<MqttPacketV3> {
        match self.state {
            InflightState::WaitPubrel => 
                Some(MqttPacketV3::Pubcomp(PubCompPacket::new(self.packet_identifier))),
            InflightState::WaitPubcomp => 
                None,
            InflightState::WaitPubrec => 
                Some(MqttPacketV3::Pubrel(PubRelPacket::new(self.packet_identifier))),
            InflightState::WaitPuback => 
                None,
            InflightState::Finish => None
        }       
    }

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
                (InflightState::Finish, None),
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

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use super::{Inflight, InflightState};

    async fn get_state(inflight: &Inflight, packet_identifier: u16) -> Option<InflightState> {
        let inner = inflight.inner.read().await;
        inner.get(&packet_identifier).map(|item| item.state.clone())
        
    }

    #[tokio::test()]
    async fn when_get_next_state_after_register_tx_qos_1_publish_packet_inflight_should_return_correct_state() {
        let mut inflight = Inflight::new(Duration::from_secs(10));
        let publish_packet = yedmq_mqtt::v3::publish::PublishPacketBuilder::new("a/b/c".to_string(),vec![0x01]).qos(1).build();

        let packet_identifier = publish_packet.variable_header.packet_identifier.unwrap();

        let packet = yedmq_mqtt::MqttPacketV3::Publish(publish_packet);
        inflight.register_with_tx_packet(&packet).await;

        let state = get_state(&inflight, packet_identifier).await;
        assert!(state.is_some());

        let state = state.unwrap();

        assert_eq!(state, super::InflightState::WaitPuback);

        inflight.next_state(packet_identifier).await;


        let packet = inflight.get_current_packet(packet_identifier).await;

        assert!(packet.is_none());

        let state = get_state(&inflight, packet_identifier).await;
        assert!(state.is_some());

        let state = state.unwrap();

        assert_eq!(state, super::InflightState::Finish);

    }


    #[tokio::test()]
    async fn when_get_next_state_after_register_rx_qos_1_publish_packet_inflight_should_return_correct_state() {

        let inflight = Inflight::new(Duration::from_secs(10));
        let publish_packet = yedmq_mqtt::v3::publish::PublishPacketBuilder::new("a/b/c".to_string(),vec![0x01]).qos(1).build();

        let packet_identifier = publish_packet.variable_header.packet_identifier.unwrap();

        let packet = yedmq_mqtt::MqttPacketV3::Publish(publish_packet);
        inflight.register_with_rx_packet(&packet).await;

        let state = get_state(&inflight, packet_identifier).await;

        assert!(state.is_some());

        let state = state.unwrap();

        assert_eq!(state, super::InflightState::Finish);
        let packet = inflight.get_current_packet(packet_identifier).await;

        assert!(packet.is_some());
        match packet.unwrap() {
            yedmq_mqtt::MqttPacketV3::Puback(p) => assert_eq!(p.variable_header.packet_identifier, packet_identifier),
            _ => assert!(false)
        }

    }

    #[tokio::test()]
    async fn when_get_next_state_after_register_rx_qos_2_publish_packet_inflight_should_return_correct_state() {
        let mut inflight = Inflight::new(Duration::from_secs(10));
        let publish_packet = yedmq_mqtt::v3::publish::PublishPacketBuilder::new("a/b/c".to_string(),vec![0x01]).qos(2).build();

        let packet_identifier = publish_packet.variable_header.packet_identifier.unwrap();

        let packet = yedmq_mqtt::MqttPacketV3::Publish(publish_packet);
        inflight.register_with_rx_packet(&packet).await;

        let state = get_state(&inflight, packet_identifier).await;

        assert!(state.is_some());

        let state = state.unwrap();

        assert_eq!(state, super::InflightState::WaitPubrel);

        let packet = inflight.get_current_packet(packet_identifier).await;

        assert!(packet.is_some());
        match packet.unwrap() {
            yedmq_mqtt::MqttPacketV3::Pubrec(p) => assert_eq!(p.variable_header.packet_identifier, packet_identifier),
            _ => assert!(false)
        }

        inflight.next_state(packet_identifier).await;

        let state = get_state(&inflight, packet_identifier).await;

        assert!(state.is_some());

        let state = state.unwrap();

        assert_eq!(state, super::InflightState::Finish);

        let packet = inflight.get_current_packet(packet_identifier).await;

        assert!(packet.is_some());
        match packet.unwrap() {
            yedmq_mqtt::MqttPacketV3::Pubcomp(p) => assert_eq!(p.variable_header.packet_identifier, packet_identifier),
            _ => assert!(false)
        }

        inflight.next_state(packet_identifier).await;

        let state = get_state(&inflight, packet_identifier).await;

        assert!(state.is_some());

        let state = state.unwrap();

        assert_eq!(state, super::InflightState::Finish);

        let packet = inflight.get_current_packet(packet_identifier).await;

        assert!(packet.is_none());


    }

    #[tokio::test()]
    async fn when_get_next_state_after_register_tx_qos_2_publish_packet_inflight_should_return_correct_state() {
        let mut inflight = Inflight::new(Duration::from_secs(10));
        let publish_packet = yedmq_mqtt::v3::publish::PublishPacketBuilder::new("a/b/c".to_string(),vec![0x01]).qos(2).build();

        let packet_identifier = publish_packet.variable_header.packet_identifier.unwrap();

        let packet = yedmq_mqtt::MqttPacketV3::Publish(publish_packet);
        inflight.register_with_tx_packet(&packet).await;

        let state = get_state(&inflight, packet_identifier).await;

        assert!(state.is_some());

        let state = state.unwrap();

        assert_eq!(state, super::InflightState::WaitPubrec);

        let packet = inflight.get_current_packet(packet_identifier).await;

        assert!(packet.is_some());
        match packet.unwrap() {
            yedmq_mqtt::MqttPacketV3::Publish(p) => assert_eq!(p.variable_header.packet_identifier.unwrap(), packet_identifier),
            _ => assert!(false)
        }

        inflight.next_state(packet_identifier).await;        

        let state = get_state(&inflight, packet_identifier).await;

        assert!(state.is_some());

        let state = state.unwrap();

        assert_eq!(state, super::InflightState::WaitPubcomp);

        let packet = inflight.get_current_packet(packet_identifier).await;

        assert!(packet.is_some());
        match packet.unwrap() {
            yedmq_mqtt::MqttPacketV3::Pubrel(p) => assert_eq!(p.variable_header.packet_identifier, packet_identifier),
            _ => assert!(false)
        }

        inflight.next_state(packet_identifier).await;        


        let state = get_state(&inflight, packet_identifier).await;

        assert!(state.is_some());

        let state = state.unwrap();

        assert_eq!(state, super::InflightState::Finish);

    }
}