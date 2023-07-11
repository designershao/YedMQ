use bytes::BytesMut;
use nom::{IResult, combinator::map};

use super::fixed_header::{FixHeader, self};


pub struct PingreqPacket {
    fix_header: FixHeader,
}

pub fn parse(input: &[u8]) -> IResult<&[u8], PingreqPacket> {
    map(
    fixed_header::parse,
    |fixed_header| {
        PingreqPacket {
            fix_header: fixed_header,
        } 
    })(input)
}

impl PingreqPacket {
    pub fn to_bytes(&self) -> BytesMut {
        self.fix_header.to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;

    use crate::protocol::PacketType;

    use super::*;

    #[test]
    fn test_parse() {
        let input = &[0xC0,0x00];
        let out = parse(input).unwrap();
        assert!(out.1.fix_header.packet_type == PacketType::PINGREQ);
    }

    #[test]
    fn test_to_bytes() {
        let fix_header = FixHeader {
            packet_type: PacketType::PINGREQ,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 0,
        };

        let pingreq_packet = PingreqPacket {
            fix_header
        };

        assert_eq!(pingreq_packet.to_bytes().as_bytes(), &[0xC0, 0x00]);
    }
}