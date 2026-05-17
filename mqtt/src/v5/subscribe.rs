use bytes::{BufMut, BytesMut};

use crate::packet::{Packet, ProtocolVersion, RetainHandling, Subscribe, SubscribeTopic};

use super::{
    common::{encode_utf8_string, parse_u16, parse_u8, parse_utf8_string, Mqtt5ParseError},
    fixed_header::{self, ControlPacketType, FixedHeader},
    properties::{self, PropertyScope},
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != ControlPacketType::Subscribe {
        return Err(Mqtt5ParseError::MalformedPacket(
            "expected SUBSCRIBE packet",
        ));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if input.len() < remaining_length {
        return Err(Mqtt5ParseError::Incomplete("SUBSCRIBE body"));
    }
    let (body, input) = input.split_at(remaining_length);
    let (_, subscribe) = parse_body(body)?;

    Ok((input, Packet::Subscribe(subscribe)))
}

fn parse_body(input: &[u8]) -> Result<(&[u8], Subscribe), Mqtt5ParseError> {
    let (input, packet_identifier) = parse_u16(input, "SUBSCRIBE packet identifier")?;
    if packet_identifier == 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBSCRIBE packet identifier must not be zero",
        ));
    }
    let (mut input, properties) = properties::parse(input, PropertyScope::Subscribe)?;

    let mut topics = Vec::new();
    while !input.is_empty() {
        let (remaining, topic_filter) = parse_utf8_string(input)?;
        if topic_filter.is_empty() {
            return Err(Mqtt5ParseError::MalformedPacket(
                "SUBSCRIBE topic filter must not be empty",
            ));
        }
        let (remaining, options) = parse_u8(remaining, "SUBSCRIBE options")?;
        topics.push(parse_topic(topic_filter, options)?);
        input = remaining;
    }
    if topics.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBSCRIBE must contain at least one topic filter",
        ));
    }

    Ok((
        input,
        Subscribe {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier,
            topics,
            properties,
        },
    ))
}

fn parse_topic(topic_filter: String, options: u8) -> Result<SubscribeTopic, Mqtt5ParseError> {
    if options & 0b1100_0000 != 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBSCRIBE option reserved bits must be zero",
        ));
    }
    let qos = options & 0b0000_0011;
    if qos == 3 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBSCRIBE requested QoS must not be 3",
        ));
    }
    let retain_handling = match (options & 0b0011_0000) >> 4 {
        0 => RetainHandling::SendAtSubscribe,
        1 => RetainHandling::SendAtSubscribeIfNew,
        2 => RetainHandling::DoNotSend,
        _ => {
            return Err(Mqtt5ParseError::MalformedPacket(
                "SUBSCRIBE retain handling must not be 3",
            ));
        }
    };

    Ok(SubscribeTopic {
        topic_filter,
        qos,
        no_local: options & 0b0000_0100 != 0,
        retain_as_published: options & 0b0000_1000 != 0,
        retain_handling,
    })
}

pub fn encode(packet: &Subscribe, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    if packet.protocol_version != ProtocolVersion::V5_0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBSCRIBE packet is not MQTT 5.0",
        ));
    }
    if packet.packet_identifier == 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBSCRIBE packet identifier must not be zero",
        ));
    }
    if packet.topics.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBSCRIBE must contain at least one topic filter",
        ));
    }

    let mut body = BytesMut::new();
    body.put_u16(packet.packet_identifier);
    properties::encode(&packet.properties, PropertyScope::Subscribe, &mut body)?;
    for topic in &packet.topics {
        if topic.topic_filter.is_empty() {
            return Err(Mqtt5ParseError::MalformedPacket(
                "SUBSCRIBE topic filter must not be empty",
            ));
        }
        if topic.qos > 2 {
            return Err(Mqtt5ParseError::MalformedPacket(
                "SUBSCRIBE requested QoS must be 0, 1, or 2",
            ));
        }
        encode_utf8_string(&topic.topic_filter, &mut body)?;
        body.put_u8(encode_options(topic));
    }

    let fixed_header = FixedHeader::new(
        ControlPacketType::Subscribe,
        0b0010,
        body.len()
            .try_into()
            .map_err(|_| Mqtt5ParseError::VariableByteIntegerTooLarge(body.len() as u32))?,
    )?;
    fixed_header.encode(buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

pub fn to_bytes(packet: &Subscribe) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

fn encode_options(topic: &SubscribeTopic) -> u8 {
    let retain_handling = match topic.retain_handling {
        RetainHandling::SendAtSubscribe => 0,
        RetainHandling::SendAtSubscribeIfNew => 1,
        RetainHandling::DoNotSend => 2,
    };
    let mut options = topic.qos & 0b11;
    if topic.no_local {
        options |= 0b0000_0100;
    }
    if topic.retain_as_published {
        options |= 0b0000_1000;
    }
    options | (retain_handling << 4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::Properties;

    #[test]
    fn parses_subscribe_fixture() {
        let input = [
            0x82, 0x09, 0x00, 0x10, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x02,
        ];

        let (_, packet) = parse(&input, 1024).expect("parse SUBSCRIBE");

        let Packet::Subscribe(subscribe) = packet else {
            panic!("expected SUBSCRIBE packet");
        };
        assert_eq!(subscribe.packet_identifier, 16);
        assert_eq!(subscribe.topics[0].topic_filter, "a/b");
        assert_eq!(subscribe.topics[0].qos, 2);
        assert_eq!(
            subscribe.topics[0].retain_handling,
            RetainHandling::SendAtSubscribe
        );
    }

    #[test]
    fn subscribe_round_trips_with_options_and_properties() {
        let packet = Subscribe {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier: 7,
            topics: vec![SubscribeTopic {
                topic_filter: "sensors/#".to_string(),
                qos: 1,
                no_local: true,
                retain_as_published: true,
                retain_handling: RetainHandling::DoNotSend,
            }],
            properties: Properties {
                user_properties: vec![("tenant".to_string(), "alpha".to_string())],
                ..Properties::default()
            },
        };

        let encoded = to_bytes(&packet).expect("encode SUBSCRIBE");
        let (_, decoded) = parse(&encoded, 1024).expect("parse SUBSCRIBE");

        assert_eq!(decoded, Packet::Subscribe(packet));
    }

    #[test]
    fn rejects_invalid_subscribe_options() {
        let input = [
            0x82, 0x09, 0x00, 0x10, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x03,
        ];

        let err = parse(&input, 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::MalformedPacket("SUBSCRIBE requested QoS must not be 3")
        );
    }
}
