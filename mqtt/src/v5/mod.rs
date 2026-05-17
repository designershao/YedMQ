mod ack;
pub mod auth;
pub mod common;
pub mod connack;
pub mod connect;
pub mod disconnect;
pub mod fixed_header;
pub mod pingreq;
pub mod pingresp;
pub mod properties;
pub mod puback;
pub mod pubcomp;
pub mod publish;
pub mod pubrec;
pub mod pubrel;
pub mod reason_code;
pub mod suback;
pub mod subscribe;
pub mod unsuback;
pub mod unsubscribe;

use bytes::BytesMut;

use crate::packet::Packet;

use self::{common::Mqtt5ParseError, fixed_header::ControlPacketType};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (_, fixed_header) = fixed_header::parse(input, max_message_size)?;
    match fixed_header.packet_type {
        ControlPacketType::Connect => connect::parse(input, max_message_size),
        ControlPacketType::Connack => connack::parse(input, max_message_size),
        ControlPacketType::Publish => publish::parse(input, max_message_size),
        ControlPacketType::Puback => puback::parse(input, max_message_size),
        ControlPacketType::Pubrec => pubrec::parse(input, max_message_size),
        ControlPacketType::Pubrel => pubrel::parse(input, max_message_size),
        ControlPacketType::Pubcomp => pubcomp::parse(input, max_message_size),
        ControlPacketType::Subscribe => subscribe::parse(input, max_message_size),
        ControlPacketType::Suback => suback::parse(input, max_message_size),
        ControlPacketType::Unsubscribe => unsubscribe::parse(input, max_message_size),
        ControlPacketType::Unsuback => unsuback::parse(input, max_message_size),
        ControlPacketType::Pingreq => pingreq::parse(input, max_message_size),
        ControlPacketType::Pingresp => pingresp::parse(input, max_message_size),
        ControlPacketType::Disconnect => disconnect::parse(input, max_message_size),
        ControlPacketType::Auth => auth::parse(input, max_message_size),
    }
}

pub fn encode(packet: &Packet, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    match packet {
        Packet::Connect(packet) => connect::encode(packet, buffer),
        Packet::Connack(packet) => connack::encode(packet, buffer),
        Packet::Publish(packet) => publish::encode(packet, buffer),
        Packet::Puback(packet) => puback::encode(packet, buffer),
        Packet::Pubrec(packet) => pubrec::encode(packet, buffer),
        Packet::Pubrel(packet) => pubrel::encode(packet, buffer),
        Packet::Pubcomp(packet) => pubcomp::encode(packet, buffer),
        Packet::Subscribe(packet) => subscribe::encode(packet, buffer),
        Packet::Suback(packet) => suback::encode(packet, buffer),
        Packet::Unsubscribe(packet) => unsubscribe::encode(packet, buffer),
        Packet::Unsuback(packet) => unsuback::encode(packet, buffer),
        Packet::Pingreq => pingreq::encode(buffer),
        Packet::Pingresp => pingresp::encode(buffer),
        Packet::Disconnect(packet) => disconnect::encode(packet, buffer),
        Packet::Auth(packet) => auth::encode(packet, buffer),
    }
}

pub fn to_bytes(packet: &Packet) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::packet::{Properties, ProtocolVersion, Publish};

    use super::*;

    #[test]
    fn top_level_parse_dispatches_publish() {
        let packet = Packet::Publish(Publish {
            protocol_version: ProtocolVersion::V5_0,
            topic_name: "a/b".to_string(),
            payload: Bytes::from_static(b"hi"),
            qos: 0,
            retain: false,
            dup: false,
            packet_identifier: None,
            properties: Properties::default(),
            expires_at_unix_secs: None,
        });

        let encoded = to_bytes(&packet).expect("encode PUBLISH");
        let (_, decoded) = parse(&encoded, 1024).expect("parse PUBLISH");

        assert_eq!(decoded, packet);
    }
}
