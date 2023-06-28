use byteorder::{BigEndian, ByteOrder};
use nom::{IResult, Parser, number::streaming::{be_u16, be_u8}, combinator::{map_res, flat_map, map, rest}, sequence::tuple, bits, error::Error};
use nom::bytes::{streaming::take};
use super::{fixed_header::{FixHeader, self}, common::parse_utf8};

pub struct PublishPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
    pub payload: Payload,
}

pub struct VariableHeader {
    pub topic_name: String,
    pub packet_identifier: Option<u16>,
}

pub struct Payload {
    pub payload: Vec<u8>,
}

fn variable_header(qos_1_or_2:bool) -> impl Fn(&[u8]) -> IResult<&[u8], VariableHeader> {
    move |i| {
        if qos_1_or_2 {
            map(
            tuple((parse_utf8, be_u16)),
            |(topic_name, packet_identifier)| {
                VariableHeader {
                    topic_name,
                    packet_identifier: Some(packet_identifier),
                }
            })(i)
        } else {
            map(
            parse_utf8,
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

#[cfg(test)]
mod tests {
    
    use super::*;

    #[test] 
    fn test_variable_header() {
        let input = &[0x00,0x03,0x61,0x2F,0x62];
        let output = variable_header(false)(input).unwrap();
        assert_eq!(output.1.topic_name, "a/b".to_string());
        assert_eq!(output.1.packet_identifier, None);
    }

}