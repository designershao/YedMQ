use bytes::{BufMut, BytesMut};

use crate::packet::{Ack, Packet, Properties, ProtocolVersion, ReasonCode};

use super::{
    common::{parse_u16, parse_u8, Mqtt5ParseError},
    fixed_header::{self, ControlPacketType, FixedHeader},
    properties::{self, PropertyScope},
    reason_code,
};

pub(crate) fn parse(
    input: &[u8],
    max_message_size: u32,
    expected_packet_type: ControlPacketType,
    property_scope: PropertyScope,
    packet_builder: fn(Ack) -> Packet,
) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != expected_packet_type {
        return Err(Mqtt5ParseError::MalformedPacket(
            "unexpected publish acknowledgement packet type",
        ));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if remaining_length < 2 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "publish acknowledgement remaining length must be at least 2",
        ));
    }
    if input.len() < remaining_length {
        return Err(Mqtt5ParseError::Incomplete("publish acknowledgement body"));
    }
    let (body, input) = input.split_at(remaining_length);
    let (_, ack) = parse_body(body, expected_packet_type, property_scope)?;

    Ok((input, packet_builder(ack)))
}

pub(crate) fn encode(
    packet: &Ack,
    packet_type: ControlPacketType,
    property_scope: PropertyScope,
    buffer: &mut BytesMut,
) -> Result<(), Mqtt5ParseError> {
    if packet.protocol_version != ProtocolVersion::V5_0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "publish acknowledgement packet is not MQTT 5.0",
        ));
    }
    if packet.packet_identifier == 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "publish acknowledgement packet identifier must not be zero",
        ));
    }
    reason_code::validate_for_packet(packet.reason_code, packet_type)?;

    let mut body = BytesMut::new();
    body.put_u16(packet.packet_identifier);
    if packet.reason_code != ReasonCode::Success || packet.properties != Properties::default() {
        body.put_u8(reason_code::encode(packet.reason_code));
        if packet.properties != Properties::default() {
            properties::encode(&packet.properties, property_scope, &mut body)?;
        }
    }

    let fixed_header = FixedHeader::new(
        packet_type,
        fixed_header_flags(packet_type),
        body.len()
            .try_into()
            .map_err(|_| Mqtt5ParseError::VariableByteIntegerTooLarge(body.len() as u32))?,
    )?;
    fixed_header.encode(buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

fn parse_body(
    input: &[u8],
    packet_type: ControlPacketType,
    property_scope: PropertyScope,
) -> Result<(&[u8], Ack), Mqtt5ParseError> {
    let (input, packet_identifier) = parse_u16(input, "publish acknowledgement packet identifier")?;
    if packet_identifier == 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "publish acknowledgement packet identifier must not be zero",
        ));
    }

    if input.is_empty() {
        return Ok((
            input,
            Ack {
                protocol_version: ProtocolVersion::V5_0,
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Properties::default(),
            },
        ));
    }

    let (input, reason_code) = parse_u8(input, "publish acknowledgement reason code")?;
    let reason_code = reason_code::decode_for_packet(reason_code, packet_type)?;
    let (input, properties) = if input.is_empty() {
        (input, Properties::default())
    } else {
        properties::parse(input, property_scope)?
    };
    if !input.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "publish acknowledgement payload has trailing bytes",
        ));
    }

    Ok((
        input,
        Ack {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier,
            reason_code,
            properties,
        },
    ))
}

fn fixed_header_flags(packet_type: ControlPacketType) -> u8 {
    match packet_type {
        ControlPacketType::Pubrel => 0b0010,
        _ => 0,
    }
}
