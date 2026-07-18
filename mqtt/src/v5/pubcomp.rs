use bytes::BytesMut;

use crate::packet::{Ack, Packet};

use super::{
    ack, common::Mqtt5ParseError, fixed_header::ControlPacketType, properties::PropertyScope,
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    ack::parse(
        input,
        max_message_size,
        ControlPacketType::Pubcomp,
        PropertyScope::Pubcomp,
        Packet::Pubcomp,
    )
}

pub fn encode(packet: &Ack, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    ack::encode(
        packet,
        ControlPacketType::Pubcomp,
        PropertyScope::Pubcomp,
        buffer,
    )
}

pub fn to_bytes(packet: &Ack) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::{Ack, Properties, ProtocolVersion, ReasonCode};

    #[test]
    fn pubcomp_round_trips_with_reason_string_property() {
        let packet = Ack {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier: 10,
            reason_code: ReasonCode::PacketIdentifierNotFound,
            properties: Properties {
                reason_string: Some("failed".to_string()),
                ..Properties::default()
            },
        };

        let encoded = to_bytes(&packet).expect("encode PUBCOMP");
        let (_, decoded) = parse(&encoded, 1024).expect("parse PUBCOMP");

        assert_eq!(decoded, Packet::Pubcomp(packet));
    }
}
