use bytes::{BufMut, BytesMut};

use crate::packet::{Disconnect, Packet, Properties, ProtocolVersion, ReasonCode};

use super::{
    common::{parse_u8, Mqtt5ParseError},
    fixed_header::{self, ControlPacketType, FixedHeader},
    properties::{self, PropertyScope},
    reason_code,
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != ControlPacketType::Disconnect {
        return Err(Mqtt5ParseError::MalformedPacket(
            "expected DISCONNECT packet",
        ));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if input.len() < remaining_length {
        return Err(Mqtt5ParseError::Incomplete("DISCONNECT body"));
    }
    let (body, input) = input.split_at(remaining_length);
    let (_, disconnect) = parse_body(body)?;

    Ok((input, Packet::Disconnect(disconnect)))
}

fn parse_body(input: &[u8]) -> Result<(&[u8], Disconnect), Mqtt5ParseError> {
    let (reason_code, properties) = if input.is_empty() {
        (ReasonCode::NormalDisconnection, Properties::default())
    } else {
        let (input, reason_code) = parse_u8(input, "DISCONNECT reason code")?;
        let reason_code = decode_disconnect_reason_code(reason_code)?;
        if input.is_empty() {
            (reason_code, Properties::default())
        } else {
            let (input, properties) = properties::parse(input, PropertyScope::Disconnect)?;
            if !input.is_empty() {
                return Err(Mqtt5ParseError::MalformedPacket(
                    "DISCONNECT payload has trailing bytes",
                ));
            }
            (reason_code, properties)
        }
    };

    Ok((
        &[][..],
        Disconnect {
            protocol_version: ProtocolVersion::V5_0,
            reason_code,
            session_expiry_interval: properties.session_expiry_interval,
            properties,
        },
    ))
}

pub fn encode(packet: &Disconnect, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    if packet.protocol_version != ProtocolVersion::V5_0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "DISCONNECT packet is not MQTT 5.0",
        ));
    }
    reason_code::validate_for_packet(packet.reason_code, ControlPacketType::Disconnect)?;

    let properties = normalized_disconnect_properties(packet)?;

    let mut body = BytesMut::new();
    if packet.reason_code != ReasonCode::NormalDisconnection || properties != Properties::default()
    {
        body.put_u8(reason_code::encode(packet.reason_code));
        if properties != Properties::default() {
            properties::encode(&properties, PropertyScope::Disconnect, &mut body)?;
        }
    }

    let fixed_header = FixedHeader::new(
        ControlPacketType::Disconnect,
        0,
        body.len()
            .try_into()
            .map_err(|_| Mqtt5ParseError::VariableByteIntegerTooLarge(body.len() as u32))?,
    )?;
    fixed_header.encode(buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

pub fn to_bytes(packet: &Disconnect) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

fn decode_disconnect_reason_code(value: u8) -> Result<ReasonCode, Mqtt5ParseError> {
    let reason_code = if value == 0 {
        ReasonCode::NormalDisconnection
    } else {
        reason_code::decode(value)?
    };
    reason_code::validate_for_packet(reason_code, ControlPacketType::Disconnect)?;
    Ok(reason_code)
}

fn normalized_disconnect_properties(packet: &Disconnect) -> Result<Properties, Mqtt5ParseError> {
    let mut properties = packet.properties.clone();
    match (
        packet.session_expiry_interval,
        properties.session_expiry_interval,
    ) {
        (Some(packet_value), Some(property_value)) if packet_value != property_value => {
            return Err(Mqtt5ParseError::MalformedPacket(
                "DISCONNECT session expiry interval disagrees with properties",
            ));
        }
        (Some(packet_value), None) => {
            properties.session_expiry_interval = Some(packet_value);
        }
        _ => {}
    }
    Ok(properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_shortest_normal_disconnect() {
        let (_, packet) = parse(&[0xe0, 0x00], 1024).expect("parse DISCONNECT");

        assert_eq!(
            packet,
            Packet::Disconnect(Disconnect {
                protocol_version: ProtocolVersion::V5_0,
                reason_code: ReasonCode::NormalDisconnection,
                session_expiry_interval: None,
                properties: Properties::default(),
            })
        );
    }

    #[test]
    fn disconnect_round_trips_with_session_expiry() {
        let packet = Disconnect {
            protocol_version: ProtocolVersion::V5_0,
            reason_code: ReasonCode::NormalDisconnection,
            session_expiry_interval: Some(0),
            properties: Properties {
                session_expiry_interval: Some(0),
                ..Properties::default()
            },
        };

        let encoded = to_bytes(&packet).expect("encode DISCONNECT");
        let (_, decoded) = parse(&encoded, 1024).expect("parse DISCONNECT");

        assert_eq!(decoded, Packet::Disconnect(packet));
    }

    #[test]
    fn parses_disconnect_reason_code_without_properties() {
        let (_, packet) = parse(&[0xe0, 0x01, 0x80], 1024).expect("parse DISCONNECT");

        let Packet::Disconnect(disconnect) = packet else {
            panic!("expected DISCONNECT packet");
        };
        assert_eq!(disconnect.reason_code, ReasonCode::UnspecifiedError);
        assert_eq!(disconnect.properties, Properties::default());
    }
}
