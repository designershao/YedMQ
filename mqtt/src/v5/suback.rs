use bytes::{BufMut, BytesMut};

use crate::packet::{Packet, ProtocolVersion, ReasonCode, Suback};

use super::{
    common::{parse_u16, Mqtt5ParseError},
    fixed_header::{self, ControlPacketType, FixedHeader},
    properties::{self, PropertyScope},
    reason_code,
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != ControlPacketType::Suback {
        return Err(Mqtt5ParseError::MalformedPacket("expected SUBACK packet"));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if input.len() < remaining_length {
        return Err(Mqtt5ParseError::Incomplete("SUBACK body"));
    }
    let (body, input) = input.split_at(remaining_length);
    let (_, suback) = parse_body(body)?;

    Ok((input, Packet::Suback(suback)))
}

fn parse_body(input: &[u8]) -> Result<(&[u8], Suback), Mqtt5ParseError> {
    let (input, packet_identifier) = parse_u16(input, "SUBACK packet identifier")?;
    if packet_identifier == 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBACK packet identifier must not be zero",
        ));
    }
    let (input, properties) = properties::parse(input, PropertyScope::Suback)?;
    if input.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBACK must contain at least one reason code",
        ));
    }

    let reason_codes = input
        .iter()
        .copied()
        .map(decode_suback_reason_code)
        .collect::<Result<Vec<_>, _>>()?;

    Ok((
        &[][..],
        Suback {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier,
            reason_codes,
            properties,
        },
    ))
}

pub fn encode(packet: &Suback, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    if packet.protocol_version != ProtocolVersion::V5_0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBACK packet is not MQTT 5.0",
        ));
    }
    if packet.packet_identifier == 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBACK packet identifier must not be zero",
        ));
    }
    if packet.reason_codes.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "SUBACK must contain at least one reason code",
        ));
    }

    let mut body = BytesMut::new();
    body.put_u16(packet.packet_identifier);
    properties::encode(&packet.properties, PropertyScope::Suback, &mut body)?;
    for reason_code in &packet.reason_codes {
        body.put_u8(reason_code::encode(*reason_code));
    }

    let fixed_header = FixedHeader::new(
        ControlPacketType::Suback,
        0,
        body.len()
            .try_into()
            .map_err(|_| Mqtt5ParseError::VariableByteIntegerTooLarge(body.len() as u32))?,
    )?;
    fixed_header.encode(buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

pub fn to_bytes(packet: &Suback) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

fn decode_suback_reason_code(value: u8) -> Result<ReasonCode, Mqtt5ParseError> {
    match value {
        0x00 => Ok(ReasonCode::GrantedQos0),
        0x01 => Ok(ReasonCode::GrantedQos1),
        0x02 => Ok(ReasonCode::GrantedQos2),
        other => reason_code::decode(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::Properties;

    #[test]
    fn parses_suback_fixture() {
        let input = [0x90, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 0x02, 0x80];

        let (_, packet) = parse(&input, 1024).expect("parse SUBACK");

        assert_eq!(
            packet,
            Packet::Suback(Suback {
                protocol_version: ProtocolVersion::V5_0,
                packet_identifier: 1,
                reason_codes: vec![
                    ReasonCode::GrantedQos0,
                    ReasonCode::GrantedQos1,
                    ReasonCode::GrantedQos2,
                    ReasonCode::UnspecifiedError,
                ],
                properties: Properties::default(),
            })
        );
    }

    #[test]
    fn suback_round_trips_with_properties() {
        let packet = Suback {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier: 1,
            reason_codes: vec![ReasonCode::GrantedQos1],
            properties: Properties {
                reason_string: Some("ok".to_string()),
                ..Properties::default()
            },
        };

        let encoded = to_bytes(&packet).expect("encode SUBACK");
        let (_, decoded) = parse(&encoded, 1024).expect("parse SUBACK");

        assert_eq!(decoded, Packet::Suback(packet));
    }
}
