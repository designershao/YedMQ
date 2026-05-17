use bytes::{BufMut, Bytes, BytesMut};

use crate::packet::{Packet, ProtocolVersion, Publish};

use super::{
    common::{encode_utf8_string, parse_u16, parse_utf8_string, Mqtt5ParseError},
    fixed_header::{self, ControlPacketType, FixedHeader},
    properties::{self, PropertyScope},
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != ControlPacketType::Publish {
        return Err(Mqtt5ParseError::MalformedPacket("expected PUBLISH packet"));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if input.len() < remaining_length {
        return Err(Mqtt5ParseError::Incomplete("PUBLISH body"));
    }
    let (body, input) = input.split_at(remaining_length);
    let (_, publish) = parse_body(body, fixed_header)?;

    Ok((input, Packet::Publish(publish)))
}

fn parse_body(
    input: &[u8],
    fixed_header: FixedHeader,
) -> Result<(&[u8], Publish), Mqtt5ParseError> {
    let (input, topic_name) = parse_utf8_string(input)?;
    if topic_name.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "PUBLISH topic name must not be empty",
        ));
    }

    let (input, packet_identifier) = if fixed_header.qos > 0 {
        let (input, packet_identifier) = parse_u16(input, "PUBLISH packet identifier")?;
        if packet_identifier == 0 {
            return Err(Mqtt5ParseError::MalformedPacket(
                "PUBLISH packet identifier must not be zero",
            ));
        }
        (input, Some(packet_identifier))
    } else {
        (input, None)
    };

    let (input, properties) = properties::parse(input, PropertyScope::Publish)?;
    if properties.topic_alias.is_some() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "PUBLISH topic alias is not supported",
        ));
    }

    Ok((
        &[][..],
        Publish {
            protocol_version: ProtocolVersion::V5_0,
            topic_name,
            payload: Bytes::copy_from_slice(input),
            qos: fixed_header.qos,
            retain: fixed_header.retain,
            dup: fixed_header.dup,
            packet_identifier,
            properties,
            expires_at_unix_secs: None,
        },
    ))
}

pub fn encode(packet: &Publish, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    if packet.protocol_version != ProtocolVersion::V5_0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "PUBLISH packet is not MQTT 5.0",
        ));
    }
    if packet.qos > 2 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "PUBLISH QoS must be 0, 1, or 2",
        ));
    }
    if packet.topic_name.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "PUBLISH topic name must not be empty",
        ));
    }
    if packet.properties.topic_alias.is_some() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "PUBLISH topic alias is not supported",
        ));
    }
    match (packet.qos, packet.packet_identifier) {
        (0, Some(_)) => {
            return Err(Mqtt5ParseError::MalformedPacket(
                "QoS 0 PUBLISH must not include packet identifier",
            ));
        }
        (1 | 2, Some(0)) => {
            return Err(Mqtt5ParseError::MalformedPacket(
                "PUBLISH packet identifier must not be zero",
            ));
        }
        (1 | 2, None) => {
            return Err(Mqtt5ParseError::MalformedPacket(
                "QoS 1 or 2 PUBLISH requires packet identifier",
            ));
        }
        _ => {}
    }

    let mut body = BytesMut::new();
    encode_utf8_string(&packet.topic_name, &mut body)?;
    if let Some(packet_identifier) = packet.packet_identifier {
        body.put_u16(packet_identifier);
    }
    properties::encode(&packet.properties, PropertyScope::Publish, &mut body)?;
    body.extend_from_slice(&packet.payload);

    let mut flags = 0u8;
    if packet.dup {
        flags |= 0b1000;
    }
    flags |= (packet.qos & 0b11) << 1;
    if packet.retain {
        flags |= 0b0001;
    }

    let fixed_header = FixedHeader::new(
        ControlPacketType::Publish,
        flags,
        body.len()
            .try_into()
            .map_err(|_| Mqtt5ParseError::VariableByteIntegerTooLarge(body.len() as u32))?,
    )?;
    fixed_header.encode(buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

pub fn to_bytes(packet: &Publish) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::{Properties, Publish};

    #[test]
    fn parses_qos0_publish_fixture() {
        let input = [0x30, 0x08, 0x00, 0x03, b'a', b'/', b'b', 0x00, b'h', b'i'];

        let (remaining, packet) = parse(&input, 1024).expect("parse PUBLISH");

        assert!(remaining.is_empty());
        let Packet::Publish(publish) = packet else {
            panic!("expected PUBLISH packet");
        };
        assert_eq!(publish.protocol_version, ProtocolVersion::V5_0);
        assert_eq!(publish.topic_name, "a/b");
        assert_eq!(publish.payload, Bytes::from_static(b"hi"));
        assert_eq!(publish.qos, 0);
        assert_eq!(publish.packet_identifier, None);
    }

    #[test]
    fn qos1_publish_round_trips_with_properties() {
        let packet = Publish {
            protocol_version: ProtocolVersion::V5_0,
            topic_name: "sensors/temperature".to_string(),
            payload: Bytes::from_static(b"22.5"),
            qos: 1,
            retain: true,
            dup: true,
            packet_identifier: Some(42),
            properties: Properties {
                payload_format_indicator: Some(1),
                content_type: Some("text/plain".to_string()),
                user_properties: vec![("unit".to_string(), "celsius".to_string())],
                ..Properties::default()
            },
            expires_at_unix_secs: None,
        };

        let encoded = to_bytes(&packet).expect("encode PUBLISH");
        let (remaining, decoded) = parse(&encoded, 1024).expect("parse encoded PUBLISH");

        assert!(remaining.is_empty());
        assert_eq!(decoded, Packet::Publish(packet));
    }

    #[test]
    fn parses_qos2_publish_fixture() {
        let input = [
            0x34, 0x09, 0x00, 0x03, b'a', b'/', b'b', 0x00, 0x0a, 0x00, b'x',
        ];

        let (_, packet) = parse(&input, 1024).expect("parse QoS 2 PUBLISH");

        let Packet::Publish(publish) = packet else {
            panic!("expected PUBLISH packet");
        };
        assert_eq!(publish.qos, 2);
        assert_eq!(publish.packet_identifier, Some(10));
        assert_eq!(publish.payload, Bytes::from_static(b"x"));
    }

    #[test]
    fn rejects_empty_topic_name() {
        let err = parse(&[0x30, 0x03, 0x00, 0x00, 0x00], 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::MalformedPacket("PUBLISH topic name must not be empty")
        );
    }

    #[test]
    fn rejects_topic_alias_property() {
        let input = [0x30, 0x07, 0x00, 0x01, b'a', 0x03, 0x23, 0x00, 0x01];

        let err = parse(&input, 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::MalformedPacket("PUBLISH topic alias is not supported")
        );
    }

    #[test]
    fn rejects_qos_publish_with_zero_packet_identifier() {
        let input = [0x32, 0x06, 0x00, 0x01, b'a', 0x00, 0x00, 0x00];

        let err = parse(&input, 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::MalformedPacket("PUBLISH packet identifier must not be zero")
        );
    }

    #[test]
    fn rejects_qos_publish_without_packet_identifier() {
        let input = [0x32, 0x05, 0x00, 0x03, b'a', b'/', b'b'];

        let err = parse(&input, 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::Incomplete("PUBLISH packet identifier")
        );
    }

    #[test]
    fn rejects_qos0_encode_with_packet_identifier() {
        let packet = Publish {
            protocol_version: ProtocolVersion::V5_0,
            topic_name: "a".to_string(),
            payload: Bytes::new(),
            qos: 0,
            retain: false,
            dup: false,
            packet_identifier: Some(1),
            properties: Properties::default(),
            expires_at_unix_secs: None,
        };

        let err = to_bytes(&packet).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::MalformedPacket("QoS 0 PUBLISH must not include packet identifier")
        );
    }
}
