use bytes::BytesMut;

use crate::packet::Packet;

use super::{
    common::Mqtt5ParseError,
    fixed_header::{self, ControlPacketType, FixedHeader},
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != ControlPacketType::Pingresp {
        return Err(Mqtt5ParseError::MalformedPacket("expected PINGRESP packet"));
    }
    if fixed_header.remaining_length != 0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "PINGRESP remaining length must be zero",
        ));
    }
    Ok((input, Packet::Pingresp))
}

pub fn encode(buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    FixedHeader::new(ControlPacketType::Pingresp, 0, 0)?.encode(buffer)
}

pub fn to_bytes() -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(&mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pingresp_fixture() {
        assert_eq!(
            parse(&[0xd0, 0x00], 1024).unwrap(),
            (&[][..], Packet::Pingresp)
        );
    }

    #[test]
    fn encodes_pingresp_fixture() {
        assert_eq!(to_bytes().unwrap().to_vec(), vec![0xd0, 0x00]);
    }
}
