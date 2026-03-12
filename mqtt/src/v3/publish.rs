use crate::{MqttPacket, PacketType};
use bytes::{BufMut, Bytes, BytesMut};
use nom::{
    combinator::{map, map_res, rest, verify},
    sequence::tuple,
    IResult,
};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

use super::{
    common::parse_utf8_complete,
    fixed_header::{self, FixHeader},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
    pub payload: Payload,
}

impl PublishPacket {
    pub fn builder() -> PublishPacketBuilder {
        PublishPacketBuilder::default()
    }
}

#[derive(Default)]
pub struct PublishPacketBuilder {
    dup: bool,
    retain: bool,
    qos: u8,
    topic_name: String,
    payload: Bytes,
    packet_identifier: Option<u16>,
}

impl PublishPacketBuilder {
    pub fn new(topic_name: String, payload: Bytes) -> PublishPacketBuilder {
        PublishPacketBuilder {
            dup: false,
            retain: false,
            qos: 0,
            topic_name,
            payload,
            packet_identifier: None,
        }
    }

    pub fn dup(mut self, dup: bool) -> Self {
        self.dup = dup;
        self
    }

    pub fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    pub fn qos(mut self, qos: u8) -> Self {
        self.qos = qos;
        self
    }

    pub fn topic_name(mut self, topic_name: String) -> Self {
        self.topic_name = topic_name;
        self
    }

    pub fn payload(mut self, payload: Bytes) -> Self {
        self.payload = payload;
        self
    }

    pub fn packet_identifier(mut self, packet_identifier: u16) -> Self {
        self.packet_identifier = Some(packet_identifier);
        self
    }

    fn generate_random_u16() -> u16 {
        let mut rng = thread_rng();
        rng.gen()
    }

    pub fn build(self) -> PublishPacket {
        let mut fix_header = FixHeader {
            packet_type: PacketType::PUBLISH,
            qos: Some(self.qos.into()),
            retain: Some(self.retain),
            dup: None,
            remaining_length: 0,
        };
        if self.dup {
            fix_header.dup = Some(1);
        }

        let mut variable_header = VariableHeader {
            topic_name: self.topic_name,
            packet_identifier: None,
        };

        if self.qos > 0 {
            if self.packet_identifier.is_none() {
                variable_header.packet_identifier = Some(Self::generate_random_u16());
            } else {
                variable_header.packet_identifier = self.packet_identifier
            }
        }

        let payload = Payload {
            payload: self.payload,
        };

        fix_header.remaining_length = variable_header.to_bytes().len() + payload.to_bytes().len();

        PublishPacket {
            fix_header,
            variable_header,
            payload,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableHeader {
    pub topic_name: String,
    pub packet_identifier: Option<u16>,
}

impl VariableHeader {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16(u16::try_from(self.topic_name.len()).expect("topic name length fits into u16"));
        buf.put(self.topic_name.as_bytes());

        if let Some(packet_identifier) = self.packet_identifier {
            buf.put_u16(packet_identifier);
        }
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.get_length());
        buf.put_u16(u16::try_from(self.topic_name.len()).expect("topic name length fits into u16"));
        buf.put(self.topic_name.as_bytes());

        if let Some(packet_identifier) = self.packet_identifier {
            buf.put_u16(packet_identifier);
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    pub payload: Bytes,
}

impl Payload {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.payload);
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.payload.len());
        buf.put_slice(&self.payload);
        buf
    }
}

fn variable_header(qos_1_or_2: bool) -> impl Fn(&[u8]) -> IResult<&[u8], VariableHeader> {
    move |i| {
        if qos_1_or_2 {
            map(
                tuple((parse_utf8_complete, nom::number::complete::be_u16)),
                |(topic_name, packet_identifier)| VariableHeader {
                    topic_name,
                    packet_identifier: Some(packet_identifier),
                },
            )(i)
        } else {
            map(parse_utf8_complete, |topic_name| VariableHeader {
                topic_name,
                packet_identifier: None,
            })(i)
        }
    }
}

pub fn parse_with_max_message_size_limit(
    input: &[u8],
    max_message_size: usize,
) -> IResult<&[u8], PublishPacket> {
    verify(fixed_header::parse, |fixed_header| {
        fixed_header.remaining_length <= max_message_size
    })(input)
    .and_then(|(input, fixed_header)| {
        map(
            map_res(
                nom::bytes::streaming::take(fixed_header.remaining_length),
                tuple((
                    variable_header(fixed_header.qos == Some(1) || fixed_header.qos == Some(2)),
                    rest,
                )),
            ),
            move |(_, (variable_header, payload_bytes))| {
                let cloned_fixed_header = fixed_header.clone();
                let payload = Payload {
                    payload: Bytes::copy_from_slice(payload_bytes),
                };
                PublishPacket {
                    fix_header: cloned_fixed_header,
                    variable_header,
                    payload,
                }
            },
        )(input)
    })
}

pub fn parse(input: &[u8]) -> IResult<&[u8], PublishPacket> {
    verify(fixed_header::parse, |fixed_header| {
        fixed_header.remaining_length <= 1000
    })(input)
    .and_then(|(input, fixed_header)| {
        map(
            map_res(
                nom::bytes::streaming::take(fixed_header.remaining_length),
                tuple((
                    variable_header(fixed_header.qos == Some(1) || fixed_header.qos == Some(2)),
                    rest,
                )),
            ),
            move |(_, (variable_header, payload_bytes))| {
                let cloned_fixed_header = fixed_header.clone();
                let payload = Payload {
                    payload: Bytes::copy_from_slice(payload_bytes),
                };
                PublishPacket {
                    fix_header: cloned_fixed_header,
                    variable_header,
                    payload,
                }
            },
        )(input)
    })
    /*
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
    */
}

impl MqttPacket for PublishPacket {
    fn to_bytes(&self) -> BytesMut {
        let fix_header_bytes = self.fix_header.to_bytes();
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

    fn encode(&self, buffer: &mut BytesMut) {
        self.fix_header.ecnode(buffer);
        self.variable_header.encode(buffer);
        self.payload.encode(buffer);
    }
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;
    use Bytes;

    use crate::PacketType;

    use super::*;

    #[test]
    fn test_variable_header() {
        let input = &[0x00, 0x03, 0x61, 0x2F, 0x62];
        let output = variable_header(false)(input).expect("parse variable header");
        assert_eq!(output.1.topic_name, "a/b".to_string());
        assert_eq!(output.1.packet_identifier, None);
    }

    #[test]
    fn test_publish_packet_builder() {
        let publish_packet_builder = PublishPacketBuilder::new(
            "a/b".to_string(),
            Bytes::copy_from_slice(vec![0x01].as_slice()),
        );
        let publish_packet = publish_packet_builder
            .packet_identifier(0x10)
            .dup(true)
            .qos(1)
            .build();
        assert_eq!(
            publish_packet.to_bytes().as_bytes(),
            &[0x3B, 0x08, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x10, 0x01]
        );
    }

    #[test]
    fn test_parse() {
        let input = &[0x3B, 0x08, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x10, 0x01];
        let out = parse(input).expect("parse publish packet");
        assert_eq!(out.1.payload.payload, vec!(0x01));
        assert_eq!(out.1.variable_header.topic_name, "a/b".to_string());
        assert_eq!(out.1.fix_header.qos, Some(1));
    }
    #[test]
    fn test_parse_with_max_message_size_limit() {
        let input = &[0x3B, 0x08, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x10, 0x01];
        let out = parse_with_max_message_size_limit(input, 2);
        assert!(out.is_err());
    }

    #[test]
    fn test_to_bytes_with_retain_true() {
        let fix_header = FixHeader {
            packet_type: PacketType::PUBLISH,
            qos: Some(0),
            retain: Some(true),
            dup: Some(0),
            remaining_length: 8,
        };
        let variable_header = VariableHeader {
            topic_name: "a/b".to_string(),
            packet_identifier: Some(0x10),
        };

        let payload = Payload {
            payload: Bytes::copy_from_slice(vec![0x01].as_slice()),
        };

        let publish_packet = PublishPacket {
            fix_header,
            variable_header,
            payload,
        };

        assert_eq!(
            publish_packet.to_bytes().as_bytes(),
            &[0x31, 0x08, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x10, 0x01]
        );
    }

    #[test]
    fn test_to_bytes_with_retain_false() {
        let fix_header = FixHeader {
            packet_type: PacketType::PUBLISH,
            qos: Some(0),
            retain: Some(false),
            dup: Some(0),
            remaining_length: 8,
        };
        let variable_header = VariableHeader {
            topic_name: "a/b".to_string(),
            packet_identifier: Some(0x10),
        };

        let payload = Payload {
            payload: Bytes::copy_from_slice(vec![0x01].as_slice()),
        };

        let publish_packet = PublishPacket {
            fix_header,
            variable_header,
            payload,
        };

        assert_eq!(
            publish_packet.to_bytes().as_bytes(),
            &[0x30, 0x08, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x10, 0x01]
        );
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

        let payload = Payload {
            payload: Bytes::copy_from_slice(vec![0x01].as_slice()),
        };

        let publish_packet = PublishPacket {
            fix_header,
            variable_header,
            payload,
        };

        assert_eq!(
            publish_packet.to_bytes().as_bytes(),
            &[0x3B, 0x08, 0x00, 0x03, 0x61, 0x2F, 0x62, 0x00, 0x10, 0x01]
        );
    }
}
