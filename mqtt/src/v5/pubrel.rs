use bytes::BytesMut;

use crate::packet::{Ack, Packet};

use super::{
    ack, common::Mqtt5ParseError, fixed_header::ControlPacketType, properties::PropertyScope,
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    ack::parse(
        input,
        max_message_size,
        ControlPacketType::Pubrel,
        PropertyScope::Pubrel,
        Packet::Pubrel,
    )
}

pub fn encode(packet: &Ack, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    ack::encode(
        packet,
        ControlPacketType::Pubrel,
        PropertyScope::Pubrel,
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
    fn encodes_pubrel_with_required_fixed_header_flags() {
        let packet = Ack {
            protocol_version: ProtocolVersion::V5_0,
            packet_identifier: 10,
            reason_code: ReasonCode::Success,
            properties: Properties::default(),
        };

        let encoded = to_bytes(&packet).expect("encode PUBREL");

        assert_eq!(encoded.to_vec(), vec![0x62, 0x02, 0x00, 0x0a]);
    }
}
