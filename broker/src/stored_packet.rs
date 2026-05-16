use std::sync::Arc;

use yedmq_mqtt::{packet::Packet, MqttPacketV3};

pub fn serialize_stored_packet(packet: &Packet) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(packet)
}

pub fn serialize_stored_packet_to_string(packet: &Packet) -> Result<String, serde_json::Error> {
    serde_json::to_string(packet)
}

pub fn deserialize_stored_packet(bytes: &[u8]) -> Result<Packet, serde_json::Error> {
    match serde_json::from_slice::<Packet>(bytes) {
        Ok(packet) => Ok(packet),
        Err(packet_error) => match serde_json::from_slice::<MqttPacketV3>(bytes) {
            Ok(packet) => Ok(Packet::from(packet)),
            Err(_) => Err(packet_error),
        },
    }
}

pub fn deserialize_stored_packet_from_str(payload: &str) -> Result<Packet, serde_json::Error> {
    match serde_json::from_str::<Packet>(payload) {
        Ok(packet) => Ok(packet),
        Err(packet_error) => match serde_json::from_str::<MqttPacketV3>(payload) {
            Ok(packet) => Ok(Packet::from(packet)),
            Err(_) => Err(packet_error),
        },
    }
}

pub fn deserialize_stored_packet_list_from_str(
    payload: &str,
) -> Result<Vec<Arc<Packet>>, serde_json::Error> {
    match serde_json::from_str::<Vec<Arc<Packet>>>(payload) {
        Ok(packets) => Ok(packets),
        Err(packet_error) => match serde_json::from_str::<Vec<Arc<MqttPacketV3>>>(payload) {
            Ok(packets) => Ok(packets
                .into_iter()
                .map(|packet| Arc::new(Packet::from((*packet).clone())))
                .collect()),
            Err(_) => Err(packet_error),
        },
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use yedmq_mqtt::{v3::publish::PublishPacketBuilder, MqttPacketV3};

    use super::*;

    #[test]
    fn stored_packet_round_trips_neutral_packet() {
        let packet = Packet::from(MqttPacketV3::Publish(
            PublishPacketBuilder::new("a/b".to_string(), Bytes::from_static(b"hello"))
                .qos(1)
                .packet_identifier(7)
                .retain(true)
                .build(),
        ));

        let bytes = serialize_stored_packet(&packet).expect("serialize neutral packet");
        let decoded = deserialize_stored_packet(&bytes).expect("deserialize neutral packet");

        assert_eq!(decoded, packet);
    }

    #[test]
    fn old_mqtt3_publish_payload_deserializes_to_neutral_packet() {
        let old_packet = MqttPacketV3::Publish(
            PublishPacketBuilder::new("legacy/topic".to_string(), Bytes::from_static(b"legacy"))
                .qos(1)
                .packet_identifier(42)
                .build(),
        );
        let bytes = serde_json::to_vec(&old_packet).expect("serialize old mqtt3 packet");

        let decoded = deserialize_stored_packet(&bytes).expect("deserialize old mqtt3 packet");

        match decoded {
            Packet::Publish(publish) => {
                assert_eq!(publish.topic_name, "legacy/topic");
                assert_eq!(publish.payload, Bytes::from_static(b"legacy"));
                assert_eq!(publish.qos, 1);
                assert_eq!(publish.packet_identifier, Some(42));
            }
            other => panic!("expected publish packet, got {other:?}"),
        }
    }

    #[test]
    fn old_mqtt3_publish_list_payload_deserializes_to_neutral_packets() {
        let old_packet = Arc::new(MqttPacketV3::Publish(
            PublishPacketBuilder::new("legacy/list".to_string(), Bytes::from_static(b"legacy"))
                .qos(0)
                .build(),
        ));
        let payload = serde_json::to_string(&vec![old_packet]).expect("serialize old packet list");

        let decoded =
            deserialize_stored_packet_list_from_str(&payload).expect("deserialize old packet list");

        assert_eq!(decoded.len(), 1);
        match decoded[0].as_ref() {
            Packet::Publish(publish) => {
                assert_eq!(publish.topic_name, "legacy/list");
                assert_eq!(publish.payload, Bytes::from_static(b"legacy"));
            }
            other => panic!("expected publish packet, got {other:?}"),
        }
    }
}
