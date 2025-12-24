use bytes::{BytesMut, BufMut};
use nom::{IResult, number::streaming::be_u16, combinator::{map_res, flat_map, map}, error::Error};
use serde::{Deserialize, Serialize};
use crate::{MqttPacket, PacketType};

use super::fixed_header::{FixHeader, self};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubAckPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader
}

impl PubAckPacket {
    pub fn new(packet_identifier: u16) -> PubAckPacket {
        let fix_header = FixHeader {
            packet_type: PacketType::PUBACK, 
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 2,
        };

        let variable_header = VariableHeader{
            packet_identifier
        };

        PubAckPacket {
            fix_header,
            variable_header
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableHeader {
    pub packet_identifier: u16,
}

impl VariableHeader {
    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_u16(self.packet_identifier);
        buf
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16(self.packet_identifier);
    }
}

pub fn parse(input: &[u8]) -> IResult<&[u8], PubAckPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
            nom::bytes::streaming::take(fixed_header.remaining_length),
            be_u16::<&[u8], Error<&[u8]>>
            ), move |packet_identifier| {
                let cloned_fixed_header = fixed_header.clone();
                PubAckPacket {
                    fix_header: cloned_fixed_header,
                    variable_header: VariableHeader {
                        packet_identifier: packet_identifier.1
                    }
                }
            })
    })(input)
}


impl MqttPacket for PubAckPacket {
    fn to_bytes(&self) -> BytesMut {
        let fix_header_bytes = self.fix_header.to_bytes();
        let variable_header_bytes = self.variable_header.to_bytes();

        let mut buf: BytesMut = BytesMut::with_capacity(fix_header_bytes.len() + variable_header_bytes.len());
        buf.put(fix_header_bytes);
        buf.put(variable_header_bytes);
        buf
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.fix_header.ecnode(buf);
        self.variable_header.encode(buf);
    }

    fn get_packet_type(&self) -> crate::PacketType {
        crate::PacketType::PUBACK
    }
}

#[cfg(test)]
mod tests{
    use nom::AsBytes;

    use crate::PacketType;

    use super::*;

    #[test]
    fn test_parse() {
        let input = &[0x40, 0x02, 0x00, 0x0A];
        let out = parse(input).unwrap();
        assert_eq!(out.1.variable_header.packet_identifier, 10);
        assert_eq!(out.1.fix_header.packet_type, PacketType::PUBACK);
    }

    #[test]
    fn test_to_bytes() {
        let fix_header = FixHeader {
            packet_type: PacketType::PUBACK, 
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 2,
        };

        let variable_header = VariableHeader{
            packet_identifier: 10
        };

        let puback_packet = PubAckPacket {
            fix_header,
            variable_header
        };

        assert_eq!(puback_packet.to_bytes().as_bytes(), &[0x40, 0x02, 0x00, 0x0A]);

    }

}

