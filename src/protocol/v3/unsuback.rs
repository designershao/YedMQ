use byteorder::{BigEndian, ByteOrder};
use bytes::{BytesMut, BufMut};
use nom::{IResult, Parser, number::streaming::{be_u16, be_u8}, combinator::{map_res, flat_map, map, rest}, sequence::tuple, bits, error::Error};
use nom::bytes::{streaming::take};
use crate::protocol::{MqttPacket, PacketType};

use super::{fixed_header::{FixHeader, self}, common::parse_utf8};

#[derive(Debug, Clone)]
pub struct UnSubackPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
}

impl UnSubackPacket {
    pub fn new(packet_identifier: u16) -> UnSubackPacket {
        UnSubackPacket {
            fix_header: FixHeader {
                packet_type: PacketType::UNSUBACK,
                qos: None,
                retain: None,
                dup: None,
                remaining_length: 2,
            },
            variable_header: VariableHeader {
                packet_identifier,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct VariableHeader {
    pub packet_identifier: u16,
}

impl VariableHeader {
    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_u16(self.packet_identifier);
        buf
    }
}

impl MqttPacket for UnSubackPacket {
    fn to_bytes(&self) -> BytesMut {
        let fix_header_bytes = self.fix_header.to_bytes();
        let variable_header_bytes = self.variable_header.to_bytes();

        let mut buf: BytesMut = BytesMut::with_capacity(fix_header_bytes.len() + variable_header_bytes.len());
        buf.put(fix_header_bytes);
        buf.put(variable_header_bytes);

        buf
    }

    fn get_packet_type(&self) -> crate::protocol::PacketType {
        crate::protocol::PacketType::UNSUBACK
    }
}

pub fn parse(input: &[u8]) -> IResult<&[u8], UnSubackPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
            nom::bytes::streaming::take(fixed_header.remaining_length),
            be_u16::<&[u8], Error<&[u8]>>),
            move |(_, packet_identifier)| {
                let cloned_fixed_header = fixed_header.clone();
                UnSubackPacket {
                    fix_header: cloned_fixed_header,
                    variable_header: VariableHeader {
                        packet_identifier: packet_identifier
                    },
                }
            })
        })(input)
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;

    use crate::protocol::PacketType;

    use super::*;

    #[test]
    fn test_parse() {
        let input = &[0xB0, 0x02, 0x00,0x01];
        let out = parse(input).unwrap();
        assert_eq!(out.1.variable_header.packet_identifier, 1);
    }

    #[test]
    fn test_to_bytes() {
        let fix_header = FixHeader {
            packet_type: PacketType::UNSUBACK,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 2,
        };

        let variable_header = VariableHeader{
            packet_identifier: 1
        };

        let suback_packet = UnSubackPacket {
            fix_header,
            variable_header,
        };

        assert_eq!(suback_packet.to_bytes().as_bytes(), &[0xB0, 0x02, 0x00, 0x01]);
    }
}