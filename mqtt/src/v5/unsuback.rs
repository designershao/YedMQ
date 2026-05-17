use bytes::{BufMut, BytesMut};

use crate::packet::{Packet, ProtocolVersion, Unsuback};

use super::{
    common::{parse_u16, Mqtt5ParseError},
    fixed_header::{self, ControlPacketType, FixedHeader},
    properties::{self, PropertyScope},
    reason_code,
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != ControlPacketType::Unsuback {
        return Err(Mqtt5ParseError::MalformedPacket("expected UNSUBACK packet"));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if input.len() < remaining_length {
        return Err(Mqtt5ParseError::Incomplete("UNSUBACK body"));
    }
    let (body, input) = input.split_at(remaining_length);
    let (_, unsuback) = parse_body(body)?;

    Ok((input, Packet::Unsuback(unsuback)))
}

fn parse_body(input: &[u8]) -> Result<(&[u8], Unsuback), Mqtt5ParseError> {
    let (input, packet_identifier) = parse_u16(input, "UNSUBACK packet identifier")?;
    if packet_identifier == 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "UNSUBACK packet identifier must not be zero",
        ));
    }
    let (input, properties) = properties::parse(input, PropertyScope::Unsuback)?;
    if input.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "UNSUBACK must contain at least one reason code",
        ));
    }
    let reason_codes = input
        .iter()
        .copied()
        .map(reason_code::decode)
        .collect::<Result<Vec<_>, _>>()?;

    Ok((
        &[][..],
        Unsuback {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier,
            reason_codes,
            properties,
        },
    ))
}

pub fn encode(packet: &Unsuback, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    if packet.protocol_version != ProtocolVersion::V5_0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "UNSUBACK packet is not MQTT 5.0",
        ));
    }
    if packet.packet_identifier == 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "UNSUBACK packet identifier must not be zero",
        ));
    }
    if packet.reason_codes.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "UNSUBACK must contain at least one reason code",
        ));
    }

    let mut body = BytesMut::new();
    body.put_u16(packet.packet_identifier);
    properties::encode(&packet.properties, PropertyScope::Unsuback, &mut body)?;
    for reason_code in &packet.reason_codes {
        body.put_u8(reason_code::encode(*reason_code));
    }

    let fixed_header = FixedHeader::new(
        ControlPacketType::Unsuback,
        0,
        body.len()
            .try_into()
            .map_err(|_| Mqtt5ParseError::VariableByteIntegerTooLarge(body.len() as u32))?,
    )?;
    fixed_header.encode(buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

pub fn to_bytes(packet: &Unsuback) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::Properties;
    use crate::packet::ReasonCode;

    #[test]
    fn parses_unsuback_fixture() {
        let input = [0xb0, 0x04, 0x00, 0x01, 0x00, 0x00];

        let (_, packet) = parse(&input, 1024).expect("parse UNSUBACK");

        assert_eq!(
            packet,
            Packet::Unsuback(Unsuback {
                protocol_version: ProtocolVersion::V5_0,
                packet_identifier: 1,
                reason_codes: vec![ReasonCode::Success],
                properties: Properties::default(),
            })
        );
    }

    #[test]
    fn unsuback_round_trips_with_properties() {
        let packet = Unsuback {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier: 1,
            reason_codes: vec![ReasonCode::Success],
            properties: Properties {
                reason_string: Some("ok".to_string()),
                ..Properties::default()
            },
        };

        let encoded = to_bytes(&packet).expect("encode UNSUBACK");
        let (_, decoded) = parse(&encoded, 1024).expect("parse UNSUBACK");

        assert_eq!(decoded, Packet::Unsuback(packet));
    }
}
