use bytes::{BufMut, BytesMut};

use crate::packet::{Connack, Packet, ProtocolVersion};

use super::{
    common::{parse_u8, Mqtt5ParseError},
    fixed_header::{self, ControlPacketType, FixedHeader},
    properties::{self, PropertyScope},
    reason_code,
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != ControlPacketType::Connack {
        return Err(Mqtt5ParseError::MalformedPacket("expected CONNACK packet"));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if input.len() < remaining_length {
        return Err(Mqtt5ParseError::Incomplete("CONNACK body"));
    }
    let (body, input) = input.split_at(remaining_length);
    let (_, connack) = parse_body(body)?;

    Ok((input, Packet::Connack(connack)))
}

fn parse_body(input: &[u8]) -> Result<(&[u8], Connack), Mqtt5ParseError> {
    let (input, acknowledge_flags) = parse_u8(input, "CONNACK acknowledge flags")?;
    if acknowledge_flags & 0xfe != 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "CONNACK acknowledge flags reserved bits must be zero",
        ));
    }
    let session_present = acknowledge_flags & 0x01 != 0;
    let (input, reason_code) = parse_u8(input, "CONNACK reason code")?;
    let reason_code = reason_code::decode_for_packet(reason_code, ControlPacketType::Connack)?;
    if reason_code != crate::packet::ReasonCode::Success && session_present {
        return Err(Mqtt5ParseError::MalformedPacket(
            "CONNACK session present requires success reason code",
        ));
    }
    let (input, properties) = properties::parse(input, PropertyScope::Connack)?;
    if !input.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "CONNACK payload has trailing bytes",
        ));
    }

    Ok((
        input,
        Connack {
            protocol_version: ProtocolVersion::V5_0,
            session_present,
            reason_code,
            properties,
        },
    ))
}

pub fn encode(packet: &Connack, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    if packet.protocol_version != ProtocolVersion::V5_0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "CONNACK packet is not MQTT 5.0",
        ));
    }
    if packet.reason_code != crate::packet::ReasonCode::Success && packet.session_present {
        return Err(Mqtt5ParseError::MalformedPacket(
            "CONNACK session present requires success reason code",
        ));
    }
    reason_code::validate_for_packet(packet.reason_code, ControlPacketType::Connack)?;

    let mut body = BytesMut::new();
    body.put_u8(u8::from(packet.session_present));
    body.put_u8(reason_code::encode(packet.reason_code));
    properties::encode(&packet.properties, PropertyScope::Connack, &mut body)?;

    let fixed_header = FixedHeader::new(
        ControlPacketType::Connack,
        0,
        body.len()
            .try_into()
            .map_err(|_| Mqtt5ParseError::VariableByteIntegerTooLarge(body.len() as u32))?,
    )?;
    fixed_header.encode(buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

pub fn to_bytes(packet: &Connack) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::{Properties, ReasonCode};

    #[test]
    fn encodes_success_connack_without_properties() {
        let packet = Connack {
            protocol_version: ProtocolVersion::V5_0,
            session_present: false,
            reason_code: ReasonCode::Success,
            properties: Properties::default(),
        };

        let encoded = to_bytes(&packet).expect("encode CONNACK");

        assert_eq!(encoded.to_vec(), vec![0x20, 0x03, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn parses_success_connack_with_limit_properties() {
        let packet = Connack {
            protocol_version: ProtocolVersion::V5_0,
            session_present: true,
            reason_code: ReasonCode::Success,
            properties: Properties {
                topic_alias_maximum: Some(0),
                maximum_packet_size: Some(1024),
                subscription_identifier_available: Some(false),
                shared_subscription_available: Some(false),
                ..Properties::default()
            },
        };

        let encoded = to_bytes(&packet).expect("encode CONNACK");
        let (remaining, decoded) = parse(&encoded, 1024).expect("parse CONNACK");

        assert!(remaining.is_empty());
        assert_eq!(decoded, Packet::Connack(packet));
    }

    #[test]
    fn rejects_invalid_acknowledge_flags() {
        let err = parse(&[0x20, 0x03, 0x02, 0x00, 0x00], 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::MalformedPacket(
                "CONNACK acknowledge flags reserved bits must be zero"
            )
        );
    }

    #[test]
    fn rejects_session_present_on_error_connack() {
        let err = parse(&[0x20, 0x03, 0x01, 0x84, 0x00], 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::MalformedPacket(
                "CONNACK session present requires success reason code"
            )
        );
    }
}
