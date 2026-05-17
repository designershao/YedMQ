use bytes::BytesMut;

use crate::packet::{Ack, Packet};

use super::{
    ack, common::Mqtt5ParseError, fixed_header::ControlPacketType, properties::PropertyScope,
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    ack::parse(
        input,
        max_message_size,
        ControlPacketType::Pubrec,
        PropertyScope::Pubrec,
        Packet::Pubrec,
    )
}

pub fn encode(packet: &Ack, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    ack::encode(
        packet,
        ControlPacketType::Pubrec,
        PropertyScope::Pubrec,
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
    fn parses_pubrec_with_reason_code() {
        let (_, packet) = parse(&[0x50, 0x03, 0x00, 0x0a, 0x80], 1024).expect("parse PUBREC");

        assert_eq!(
            packet,
            Packet::Pubrec(Ack {
                protocol_version: ProtocolVersion::V5_0,
                packet_identifier: 10,
                reason_code: ReasonCode::UnspecifiedError,
                properties: Properties::default(),
            })
        );
    }
}
