use bytes::{BufMut, BytesMut};

use crate::packet::{Packet, ProtocolVersion, Unsubscribe};

use super::{
    common::{encode_utf8_string, parse_u16, parse_utf8_string, Mqtt5ParseError},
    fixed_header::{self, ControlPacketType, FixedHeader},
    properties::{self, PropertyScope},
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != ControlPacketType::Unsubscribe {
        return Err(Mqtt5ParseError::MalformedPacket(
            "expected UNSUBSCRIBE packet",
        ));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if input.len() < remaining_length {
        return Err(Mqtt5ParseError::Incomplete("UNSUBSCRIBE body"));
    }
    let (body, input) = input.split_at(remaining_length);
    let (_, unsubscribe) = parse_body(body)?;

    Ok((input, Packet::Unsubscribe(unsubscribe)))
}

fn parse_body(input: &[u8]) -> Result<(&[u8], Unsubscribe), Mqtt5ParseError> {
    let (input, packet_identifier) = parse_u16(input, "UNSUBSCRIBE packet identifier")?;
    if packet_identifier == 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "UNSUBSCRIBE packet identifier must not be zero",
        ));
    }
    let (mut input, properties) = properties::parse(input, PropertyScope::Unsubscribe)?;
    let mut topics = Vec::new();
    while !input.is_empty() {
        let (remaining, topic) = parse_utf8_string(input)?;
        if topic.is_empty() {
            return Err(Mqtt5ParseError::MalformedPacket(
                "UNSUBSCRIBE topic filter must not be empty",
            ));
        }
        topics.push(topic);
        input = remaining;
    }
    if topics.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "UNSUBSCRIBE must contain at least one topic filter",
        ));
    }

    Ok((
        input,
        Unsubscribe {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier,
            topics,
            properties,
        },
    ))
}

pub fn encode(packet: &Unsubscribe, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    if packet.protocol_version != ProtocolVersion::V5_0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "UNSUBSCRIBE packet is not MQTT 5.0",
        ));
    }
    if packet.packet_identifier == 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "UNSUBSCRIBE packet identifier must not be zero",
        ));
    }
    if packet.topics.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "UNSUBSCRIBE must contain at least one topic filter",
        ));
    }

    let mut body = BytesMut::new();
    body.put_u16(packet.packet_identifier);
    properties::encode(&packet.properties, PropertyScope::Unsubscribe, &mut body)?;
    for topic in &packet.topics {
        if topic.is_empty() {
            return Err(Mqtt5ParseError::MalformedPacket(
                "UNSUBSCRIBE topic filter must not be empty",
            ));
        }
        encode_utf8_string(topic, &mut body)?;
    }

    let fixed_header = FixedHeader::new(
        ControlPacketType::Unsubscribe,
        0b0010,
        body.len()
            .try_into()
            .map_err(|_| Mqtt5ParseError::VariableByteIntegerTooLarge(body.len() as u32))?,
    )?;
    fixed_header.encode(buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

pub fn to_bytes(packet: &Unsubscribe) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::Properties;

    #[test]
    fn parses_unsubscribe_fixture() {
        let input = [0xa2, 0x08, 0x00, 0x10, 0x00, 0x00, 0x03, b'a', b'/', b'b'];

        let (_, packet) = parse(&input, 1024).expect("parse UNSUBSCRIBE");

        let Packet::Unsubscribe(unsubscribe) = packet else {
            panic!("expected UNSUBSCRIBE packet");
        };
        assert_eq!(unsubscribe.packet_identifier, 16);
        assert_eq!(unsubscribe.topics, vec!["a/b".to_string()]);
    }

    #[test]
    fn unsubscribe_round_trips_with_properties() {
        let packet = Unsubscribe {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier: 2,
            topics: vec!["a/b".to_string(), "c/d".to_string()],
            properties: Properties {
                user_properties: vec![("k".to_string(), "v".to_string())],
                ..Properties::default()
            },
        };

        let encoded = to_bytes(&packet).expect("encode UNSUBSCRIBE");
        let (_, decoded) = parse(&encoded, 1024).expect("parse UNSUBSCRIBE");

        assert_eq!(decoded, Packet::Unsubscribe(packet));
    }
}
