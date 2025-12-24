use bytes::BytesMut;
use nom::{IResult, combinator::map};
use serde::{Deserialize, Serialize};

use crate::{MqttPacket, PacketType};

use super::fixed_header::{FixHeader, self};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingrespPacket {
    pub fix_header: FixHeader,
}

impl Default for PingrespPacket {
    fn default() -> Self {
        Self::new()
    }
}

impl PingrespPacket {

    pub fn new() -> PingrespPacket {
        PingrespPacket {
            fix_header: FixHeader {
                packet_type: PacketType::PINGRESP,
                qos: None,
                retain: None,
                dup: None,
                remaining_length: 0,
            }
        }
    }

}

pub fn parse(input: &[u8]) -> IResult<&[u8], PingrespPacket> {
    map(
    fixed_header::parse,
    |fixed_header| {
        PingrespPacket {
            fix_header: fixed_header,
        } 
    })(input)
}

impl MqttPacket for PingrespPacket {
    fn to_bytes(&self) -> BytesMut {
        self.fix_header.to_bytes()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.fix_header.ecnode(buf);
    }

    fn get_packet_type(&self) -> crate::PacketType {
        crate::PacketType::PINGRESP
    }
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;

    use crate::PacketType;

    use super::*;

    #[test]
    fn test_parse() {
        let input = &[0xD0,0x00];
        let out = parse(input).unwrap();
        assert!(out.1.fix_header.packet_type == PacketType::PINGRESP);
    }

    #[test]
    fn test_to_bytes() {
        let fix_header = FixHeader {
            packet_type: PacketType::PINGRESP,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 0,
        };

        let pingresp_packet = PingrespPacket{ fix_header };

        let pingresp_packet_bytes = pingresp_packet.to_bytes();

        assert_eq!(pingresp_packet_bytes.as_bytes(), &[0xD0, 0x00]);
    }
}