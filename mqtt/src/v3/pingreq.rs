use bytes::BytesMut;
use nom::{IResult, combinator::map};
use serde::{Deserialize, Serialize};

use crate::MqttPacket;

use super::fixed_header::{FixHeader, self};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingreqPacket {
    pub fix_header: FixHeader,
}

#[derive(Default)]
pub struct  PingreqPacketBuilder;

impl PingreqPacketBuilder {

    pub fn new () -> PingreqPacketBuilder {
        PingreqPacketBuilder
    }

    pub fn build(self) -> PingreqPacket {
        PingreqPacket {
            fix_header: FixHeader {
                packet_type: crate::PacketType::PINGREQ,
                qos: None,
                retain: None,
                dup: None,
                remaining_length: 0,
            }
        }
    }
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

impl MqttPacket for PingreqPacket {
    fn to_bytes(&self) -> BytesMut {
        self.fix_header.to_bytes()
    }

    fn get_packet_type(&self) -> crate::PacketType {
        crate::PacketType::PINGREQ
    }
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;

    use crate::PacketType;

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