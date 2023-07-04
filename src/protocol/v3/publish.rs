use byteorder::{BigEndian, ByteOrder};
use bytes::{BytesMut, BufMut};
use nom::{IResult, Parser, number::streaming::{be_u16, be_u8}, combinator::{map_res, flat_map, map, rest}, sequence::tuple, bits, error::Error};
use nom::bytes::{streaming::take};
use super::{fixed_header::{FixHeader, self}, common::{parse_utf8, parse_utf8_complete}};

pub struct PublishPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
    pub payload: Payload,
}

pub struct VariableHeader {
    pub topic_name: String,
    pub packet_identifier: Option<u16>,
}

impl VariableHeader {

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.get_length());
        buf.put_u16(self.topic_name.len().try_into().unwrap());
        buf.put(self.topic_name.as_bytes());

        if self.packet_identifier.is_some() {
            buf.put_u16(self.packet_identifier.unwrap());
        }
        buf
    }

    fn get_length(&self) -> usize {
        if self.packet_identifier.is_some() {
            self.topic_name.len() + 2 + 2
        } else {
            self.topic_name.len() + 2
        }
    }
}

pub struct Payload {
    pub payload: Vec<u8>,
}

impl Payload {

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.payload.len());
        buf.put_slice(&self.payload);
        buf
    }

}

fn variable_header(qos_1_or_2:bool) -> impl Fn(&[u8]) -> IResult<&[u8], VariableHeader> {
    move |i| {
        if qos_1_or_2 {
            map(
            tuple((parse_utf8_complete, nom::number::complete::be_u16)),
            |(topic_name, packet_identifier)| {
                VariableHeader {
                    topic_name,
                    packet_identifier: Some(packet_identifier),
                }
            })(i)
        } else {
            map(
            parse_utf8_complete,
            |topic_name| {
                VariableHeader {
                    topic_name,
                    packet_identifier: None,
                }
            })(i)
        }
    }
}

pub fn parse(input: &[u8]) -> IResult<&[u8], PublishPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
                nom::bytes::streaming::take(fixed_header.remaining_length),
                tuple((variable_header(fixed_header.qos == Some(1) || fixed_header.qos == Some(2)),rest))
            ),
            move |(_, (variable_header, payload_bytes))| {
                let cloned_fixed_header = fixed_header.clone();
                let payload = Payload {
                    payload: payload_bytes.to_vec()
                };
                PublishPacket {
                    fix_header: cloned_fixed_header,
                    variable_header,
                    payload
                }
            })
    })(input)
}

impl PublishPacket {
    pub fn to_bytes(&self) -> BytesMut {
        let fix_header_bytes = self.fix_header.to_bytes();
        let variable_header_bytes = self.variable_header.to_bytes();
        let payload_bytes = self.payload.to_bytes();

        let mut buf: BytesMut = BytesMut::with_capacity(fix_header_bytes.len() + variable_header_bytes.len() + payload_bytes.len());
        buf.put(fix_header_bytes);
        buf.put(variable_header_bytes);
        buf.put(payload_bytes);
        buf
    }
}

#[cfg(test)]
mod tests {
    
    use nom::AsBytes;

    use crate::protocol::v3::fixed_header::PacketType;

    use super::*;

    #[test] 
    fn test_variable_header() {
        let input = &[0x00,0x03,0x61,0x2F,0x62];
        let output = variable_header(false)(input).unwrap();
        assert_eq!(output.1.topic_name, "a/b".to_string());
        assert_eq!(output.1.packet_identifier, None);
    }

    #[test]
    fn test_parse() {
        let input = &[0x3B,0x08,0x00,0x03,0x61,0x2F,0x62,0x00,0x10,0x01];
        let out = parse(input).unwrap();
        assert_eq!(out.1.payload.payload, vec!(0x01));
        assert_eq!(out.1.variable_header.topic_name, "a/b".to_string());
        assert_eq!(out.1.fix_header.qos, Some(1));

    }

    #[test]
    fn test_to_bytes() {
        let fix_header = FixHeader {
            packet_type: PacketType::PUBLISH,
            qos: Some(1),
            retain: Some(true),
            dup: Some(1),
            remaining_length: 8,
        };
        let variable_header = VariableHeader {
            topic_name: "a/b".to_string(),
            packet_identifier: Some(0x10),
        };

        let payload = Payload{
            payload: vec!(0x01)
        };

        let publish_packet = PublishPacket {
            fix_header,
            variable_header,
            payload
        };

        assert_eq!(publish_packet.to_bytes().as_bytes(), &[0x3B,0x08,0x00,0x03,0x61,0x2F,0x62,0x00,0x10,0x01]);
    }

}