use byteorder::{BigEndian, ByteOrder};
use bytes::{BytesMut, BufMut};
use nom::{IResult, Parser, number::streaming::{be_u16, be_u8,}, combinator::{map_res, flat_map, map, rest}, sequence::tuple, bits, error::Error, multi::many0};
use nom::multi::many1;
use nom::bytes::{streaming::take};
use crate::protocol::{MqttPacket, PacketType};

use super::{fixed_header::{FixHeader, self}, common::parse_utf8};

#[derive(Debug)]
pub struct SubackPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
    pub payload: Payload
}

impl SubackPacket {
    pub fn new(packet_identifier: u16, return_code: Vec<u8>) -> SubackPacket {
        SubackPacket {
            fix_header: FixHeader {
                packet_type: PacketType::SUBACK,
                qos: None,
                retain: None,
                dup: None,
                remaining_length: 2 + return_code.len(),
            },
            variable_header: VariableHeader {
                packet_identifier,
            },
            payload: Payload {
                return_code
            }
        } 
    }
}

#[derive(Debug)]
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


#[derive(Debug)]
pub struct Payload {
    pub return_code: Vec<u8>,
}

impl Payload {

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(1);
        for byte in self.return_code.iter() {
            buf.put_u8(*byte);
        }
        buf
    }

}

impl MqttPacket for SubackPacket {
    fn to_bytes(&self) -> BytesMut {
        let fix_header_bytes = self.fix_header.to_bytes();
        let variable_header_bytes = self.variable_header.to_bytes();
        let payload_bytes = self.payload.to_bytes();

        let mut buf: BytesMut = BytesMut::with_capacity(fix_header_bytes.len() + variable_header_bytes.len() + payload_bytes.len());
        buf.put(fix_header_bytes);
        buf.put(variable_header_bytes);
        buf.put(payload_bytes);

        buf
    }

    fn get_packet_type(&self) -> crate::protocol::PacketType {
        crate::protocol::PacketType::SUBACK
    }
}

pub fn parse(input: &[u8]) -> IResult<&[u8], SubackPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
            nom::bytes::streaming::take(fixed_header.remaining_length),
            tuple((be_u16::<&[u8], Error<&[u8]>>,many0(be_u8)))),
            move |(_, (packet_identifier, return_code))| {
                let cloned_fixed_header = fixed_header.clone();
                SubackPacket {
                    fix_header: cloned_fixed_header,
                    variable_header: VariableHeader {
                        packet_identifier: packet_identifier
                    },
                    payload: Payload {
                        return_code: return_code
                    }
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
        let input = &[0x90, 0x03, 0x00,0x01, 0x02];
        let out = parse(input).unwrap();
        assert_eq!(out.1.variable_header.packet_identifier, 1);
        assert_eq!(out.1.payload.return_code, vec![2]);
    }

    #[test]
    fn test_to_bytes() {
        let fix_header = FixHeader {
            packet_type: PacketType::SUBACK,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 3,
        };

        let variable_header = VariableHeader{
            packet_identifier: 1
        };

        let payload = Payload{
            return_code: vec![2]
        };

        let suback_packet = SubackPacket {
            fix_header,
            variable_header,
            payload
        };

        assert_eq!(suback_packet.to_bytes().as_bytes(), &[0x90, 0x03, 0x00, 0x01, 0x02]);
    }
}