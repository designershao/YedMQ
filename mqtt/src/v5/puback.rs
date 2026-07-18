use bytes::BytesMut;

use crate::packet::{Ack, Packet};

use super::{
    ack, common::Mqtt5ParseError, fixed_header::ControlPacketType, properties::PropertyScope,
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    ack::parse(
        input,
        max_message_size,
        ControlPacketType::Puback,
        PropertyScope::Puback,
        Packet::Puback,
    )
}

pub fn encode(packet: &Ack, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    ack::encode(
        packet,
        ControlPacketType::Puback,
        PropertyScope::Puback,
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
    fn encodes_success_puback_with_shortest_form() {
        let packet = Ack {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier: 10,
            reason_code: ReasonCode::Success,
            properties: Properties::default(),
        };

        let encoded = to_bytes(&packet).expect("encode PUBACK");

        assert_eq!(encoded.to_vec(), vec![0x40, 0x02, 0x00, 0x0a]);
    }

    #[test]
    fn parses_success_puback_shortest_form() {
        let (_, packet) = parse(&[0x40, 0x02, 0x00, 0x0a], 1024).expect("parse PUBACK");

        assert_eq!(
            packet,
            Packet::Puback(Ack {
                protocol_version: ProtocolVersion::V5_0,
                packet_identifier: 10,
                reason_code: ReasonCode::Success,
                properties: Properties::default(),
            })
        );
    }

    #[test]
    fn rejects_reason_code_not_allowed_on_puback() {
        let err = parse(&[0x40, 0x03, 0x00, 0x0a, 0x84], 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::ReasonCodeNotAllowed {
                reason_code: 0x84,
                packet_type: "PUBACK",
            }
        );
    }
}
