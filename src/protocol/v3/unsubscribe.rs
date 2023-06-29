use byteorder::{BigEndian, ByteOrder};
use nom::{IResult, Parser, number::streaming::{be_u16, be_u8}, combinator::{map_res, flat_map, map, cut, eof}, sequence::tuple, bits, error::Error, multi::many0};
use crate::protocol::v3::common::parse_utf8;
use nom::bits::{streaming::take};
use super::{fixed_header::{FixHeader, self}, common::parse_utf8_complete};

pub struct UnsubscribePacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
    pub payload: Payload
}

pub struct VariableHeader {
    pub packet_identifier: u16,
}

pub struct TopicFilter {
    topic_name: String,
}

pub struct Payload {
    pub topic_filters: Vec<TopicFilter>,
}

// MQTT Subscribe Topic Filter
// +---------------+--------------+---+---+---+---+---+---+---+
// | Description   | 7            | 6 | 5 | 4 | 3 | 2 | 1 | 0 |
// +---------------+--------------+---+---+---+---+---+---+---+
// | TopicFilter                                              |
// +---------------+--------------+---+---+---+---+---+---+---+
// | byte1         | MSB          |   |   |   |   |   |   |   |
// | byte2         | LSB          |   |   |   |   |   |   |   |
// | byte 3..N     | Topic Filter |   |   |   |   |   |   |   |
// +---------------+--------------+---+---+---+---+---+---+---+
// | Requested Qos                                            |
// +---------------+--------------+---+---+---+---+---+---+---+
// | byte N+1      | 0            | 0 | 0 | 0 | 0 | 0 | X | X |
// +---------------+--------------+---+---+---+---+---+---+---+

fn topic_filter(input: &[u8]) -> IResult<&[u8], TopicFilter> {
    let i = map(parse_utf8_complete, |topic_name| {
        TopicFilter{
            topic_name,
        }
    })(input);
    return i;
}

fn variable_header(input: &[u8]) -> IResult<&[u8], VariableHeader> {
    map(be_u16, |packet_identifier| {
        VariableHeader{
            packet_identifier
        }
    })(input)
}

pub fn parse(input: &[u8]) -> IResult<&[u8], UnsubscribePacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
                nom::bytes::streaming::take(fixed_header.remaining_length),
                tuple((variable_header,many0(topic_filter)))
            ),
            move |v| {
                let cloned_fixed_header = fixed_header.clone();
                UnsubscribePacket { 
                    fix_header: cloned_fixed_header,
                    variable_header: v.1.0,
                    payload: Payload { topic_filters: v.1.1 }
                }
            }
        )
    })(input)
}

#[cfg(test)]
mod tests{
    use crate::protocol::v3::fixed_header::PacketType;

    use super::*;

    #[test]
    fn test_payload() {
        let input = &[0x00,0x03,0x61,0x2F,0x62];
        let out = topic_filter(input).unwrap();
        assert_eq!(out.1.topic_name, "a/b".to_string());
    }

    #[test]
    fn test_parse() {
        let input = &[0xA2,0x07,0x00,0x10,0x00,0x03,0x61,0x2F,0x62];
        let fixed_header = fixed_header::parse(input).unwrap();
        assert_eq!(fixed_header.1.remaining_length, 7);
        assert_eq!(fixed_header.1.packet_type, PacketType::UNSUBSCRIBE);
        let out = parse(input).unwrap();
        assert_eq!(out.1.payload.topic_filters[0].topic_name, "a/b".to_string());
    }
}