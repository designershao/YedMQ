use super::{
    common::parse_utf8_complete,
    fixed_header::{self, FixHeader},
};
use crate::{MqttPacket, PacketType};
use bytes::{BufMut, BytesMut};
use nom::{
    combinator::{flat_map, map, map_res},
    multi::many0,
    number::streaming::be_u16,
    sequence::tuple,
    IResult,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribePacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
    pub payload: Payload,
}

#[derive(Default)]
pub struct SubscribePacketBuilder {
    topic_filters: Vec<TopicFilter>,
    packet_identifier: u16,
}

impl SubscribePacketBuilder {
    pub fn new(packet_identifier: u16) -> Self {
        SubscribePacketBuilder {
            topic_filters: Vec::new(),
            packet_identifier,
        }
    }

    pub fn add_topic_filter(mut self, topic_filter: TopicFilter) -> Self {
        self.topic_filters.push(topic_filter);
        self
    }

    pub fn build(self) -> SubscribePacket {
        let variable_header = VariableHeader {
            packet_identifier: self.packet_identifier,
        };

        let payload = Payload {
            topic_filters: self.topic_filters,
        };

        let fix_header = FixHeader {
            packet_type: PacketType::SUBSCRIBE,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 2 + payload.get_length(),
        };

        SubscribePacket {
            fix_header,
            variable_header,
            payload,
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
pub struct TopicFilter {
    pub topic_name: String,
    pub qos: u8,
}

impl TopicFilter {
    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.get_length());
        buf.put_u16(
            u16::try_from(self.topic_name.len()).expect("topic name length fits into u16"),
        );
        buf.put(self.topic_name.as_bytes());
        buf.put_u8(self.qos);
        buf
    }

    pub fn get_length(&self) -> usize {
        self.topic_name.len() + 2 + 1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    pub topic_filters: Vec<TopicFilter>,
}

impl Payload {
    pub fn encode(&self, buf: &mut BytesMut) {
        for topic_filter in self.topic_filters.iter() {
            buf.put(topic_filter.to_bytes());
        }
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.get_length());
        for topic_filter in self.topic_filters.iter() {
            buf.put(topic_filter.to_bytes());
        }
        buf
    }

    pub fn get_length(&self) -> usize {
        let mut total_length = 0;
        for topic_filter in self.topic_filters.iter() {
            total_length += topic_filter.get_length();
        }
        total_length
    }
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
    map(
        tuple((parse_utf8_complete, nom::number::complete::be_u8)),
        |(topic_name, qos)| TopicFilter { topic_name, qos },
    )(input)
}

fn variable_header(input: &[u8]) -> IResult<&[u8], VariableHeader> {
    map(be_u16, |packet_identifier| VariableHeader {
        packet_identifier,
    })(input)
}

pub fn parse(input: &[u8]) -> IResult<&[u8], SubscribePacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
                nom::bytes::streaming::take(fixed_header.remaining_length),
                tuple((variable_header, many0(topic_filter))),
            ),
            move |v| {
                let cloned_fixed_header = fixed_header.clone();
                SubscribePacket {
                    fix_header: cloned_fixed_header,
                    variable_header: v.1 .0,
                    payload: Payload {
                        topic_filters: v.1 .1,
                    },
                }
            },
        )
    })(input)
}

impl SubscribePacket {
    fn get_fix_header_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(2);
        buf.put_u8((1 << 7) + (1 << 1));
        buf.put_u8(
            u8::try_from(self.fix_header.remaining_length)
                .expect("remaining length fits into u8"),
        );
        buf
    }
}

impl MqttPacket for SubscribePacket {
    fn to_bytes(&self) -> BytesMut {
        let fix_header_bytes = self.get_fix_header_bytes();
        let variable_header_bytes = self.variable_header.to_bytes();
        let payload_bytes = self.payload.to_bytes();

        let mut buf: BytesMut = BytesMut::with_capacity(
            fix_header_bytes.len() + variable_header_bytes.len() + payload_bytes.len(),
        );
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
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;

    use crate::PacketType;

    use super::*;

    #[test]
    fn test_payload() {
        let input = &[0x00, 0x03, 0x61, 0x2F, 0x62, 0x02];
        let out = topic_filter(input).expect("parse topic filter");
        assert_eq!(out.1.topic_name, "a/b".to_string());
        assert_eq!(out.1.qos, 2);
    }

    #[test]
    fn test_parse() {
        let input = &[0x82, 0x08, 0x00, 0x10, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x02];
        let fixed_header = fixed_header::parse(input).expect("parse fixed header");
        assert_eq!(fixed_header.1.remaining_length, 8);
        assert_eq!(fixed_header.1.packet_type, PacketType::SUBSCRIBE);
        let out = parse(input).expect("parse subscribe packet");
        assert_eq!(out.1.payload.topic_filters[0].topic_name, "a/b".to_string());
    }

    #[test]
    fn test_subscribe_pakcet_builder() {
        let builder = SubscribePacketBuilder::new(0x10);
        let packet = builder
            .add_topic_filter(TopicFilter {
                topic_name: "a/b".to_string(),
                qos: 2,
            })
            .build();

        assert_eq!(
            packet.to_bytes().as_bytes(),
            &[0x82, 0x08, 0x00, 0x10, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x02]
        );
    }

    #[test]
    fn test_to_bytes() {
        let fix_header = FixHeader {
            packet_type: PacketType::SUBSCRIBE,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 8,
        };

        let variable_header = VariableHeader {
            packet_identifier: 0x10,
        };

        let payload = Payload {
            topic_filters: vec![TopicFilter {
                topic_name: "a/b".to_string(),
                qos: 2,
            }],
        };

        let subscribe_packet = SubscribePacket {
            fix_header,
            variable_header,
            payload,
        };

        assert_eq!(
            subscribe_packet.to_bytes().as_bytes(),
            &[0x82, 0x08, 0x00, 0x10, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x02]
        );
    }
}
