use bytes::BytesMut;
use nom::{combinator::map, IResult};
use serde::{Deserialize, Serialize};

use crate::MqttPacket;

use super::fixed_header::{self, FixHeader};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisconnectPacket {
    pub fix_header: FixHeader,
}

pub fn parse(input: &[u8]) -> IResult<&[u8], DisconnectPacket> {
    map(fixed_header::parse, |fixed_header| DisconnectPacket {
        fix_header: fixed_header,
    })(input)
}

impl MqttPacket for DisconnectPacket {
    fn to_bytes(&self) -> BytesMut {
        self.fix_header.to_bytes()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.fix_header.ecnode(buf);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use nom::AsBytes;

    use crate::PacketType;

    use super::*;

    #[test]
    fn test_parse() {
        let input = &[0xE0, 0x00];
        let out = parse(input).unwrap();
        assert!(out.1.fix_header.packet_type == PacketType::DISCONNECT);
    }

    #[test]
    fn test_to_bytes() {
        let fix_header = FixHeader {
            packet_type: PacketType::DISCONNECT,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 0,
        };
        let disconnect_packet = DisconnectPacket { fix_header };
        let disconnect_packet_bytes = disconnect_packet.to_bytes();
        assert_eq!(disconnect_packet_bytes.as_bytes(), &[0xE0, 0x00]);
    }
}
