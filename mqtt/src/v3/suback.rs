use bytes::{BytesMut, BufMut};
use nom::{IResult,  number::streaming::be_u16, combinator::{map_res, flat_map, map}, sequence::tuple, multi::many0};
use serde::{Deserialize, Serialize};
use crate::{MqttPacket, PacketType};

use super::fixed_header::{FixHeader, self};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubackPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
    pub payload: Payload
}

impl SubackPacket {
    pub fn new(packet_identifier: u16, return_code: Vec<ReturnCode>) -> SubackPacket {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableHeader {
    pub packet_identifier: u16,
}

impl VariableHeader {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16(self.packet_identifier);
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_u16(self.packet_identifier);
        buf
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    pub return_code: Vec<ReturnCode>,
}

impl Payload {
    pub fn encode(&self, buf: &mut BytesMut) {
        for code in self.return_code.iter() {
            match code {
                ReturnCode::MaxQos0 => buf.put_u8(0x00),
                ReturnCode::MaxQos1 => buf.put_u8(0x01),
                ReturnCode::MaxQos2 => buf.put_u8(0x02),
                ReturnCode::Failure => buf.put_u8(0x80),
                ReturnCode::Invalid => (),
            }
        }
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(1);
        for code in self.return_code.iter() {
            match code {
                ReturnCode::MaxQos0 => buf.put_u8(0x00),
                ReturnCode::MaxQos1 => buf.put_u8(0x01),
                ReturnCode::MaxQos2 => buf.put_u8(0x02),
                ReturnCode::Failure => buf.put_u8(0x80),
                ReturnCode::Invalid => (),
            }
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

    fn encode(&self, buf: &mut BytesMut) {
        self.fix_header.ecnode(buf);
        self.variable_header.encode(buf);
        self.payload.encode(buf);
    }

    fn get_packet_type(&self) -> crate::PacketType {
        crate::PacketType::SUBACK
    }
}

fn variable_header(input: &[u8]) -> IResult<&[u8], VariableHeader> {
    map(be_u16, |packet_identifier| {
        VariableHeader{
            packet_identifier
        }
    })(input)
}

#[derive(Debug,PartialEq, Clone, Serialize, Deserialize)]
pub enum ReturnCode {
    MaxQos0,
    MaxQos1,
    MaxQos2,
    Failure,
    Invalid
}

fn payload(input: &[u8]) -> IResult<&[u8], Payload> {
    map(many0(nom::number::complete::be_u8), |return_code| {
        let r:Vec<ReturnCode> = return_code.iter().map(|byte| {
            match byte {
                0x00 => ReturnCode::MaxQos0,
                0x01 => ReturnCode::MaxQos1,
                0x02 => ReturnCode::MaxQos2,
                0x80 => ReturnCode::Failure,
                _ => ReturnCode::Invalid
            } 
        }).to_owned().collect();
        Payload{
            return_code: r
        }
    })(input)
}

pub fn parse(input: &[u8]) -> IResult<&[u8], SubackPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
                nom::bytes::streaming::take(fixed_header.remaining_length),
                tuple((variable_header,payload))
            ),
            move |(_, (variable_header, payload))| {
                let cloned_fixed_header = fixed_header.clone();
                SubackPacket {
                    fix_header: cloned_fixed_header,
                    variable_header,
                    payload
                }
            })
        })(input)
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;

    use crate::PacketType;

    use super::*;

    #[test]
    fn test_parse() {
        let input = &[0x90, 0x04, 0x00, 0x01, 0x00, 0x01];
        let out = parse(input).unwrap();
        assert_eq!(out.1.variable_header.packet_identifier, 1);
        assert_eq!(out.1.payload.return_code, vec![ReturnCode::MaxQos0, ReturnCode::MaxQos1]);
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
            return_code: vec![ReturnCode::MaxQos2]
        };

        let suback_packet = SubackPacket {
            fix_header,
            variable_header,
            payload
        };

        assert_eq!(suback_packet.to_bytes().as_bytes(), &[0x90, 0x03, 0x00, 0x01, 0x02]);
    }
}