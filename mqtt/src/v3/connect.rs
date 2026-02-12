use super::fixed_header::{self, FixHeader};
use crate::{v3::common::parse_utf8, MqttPacket, PacketType};
use ::bytes::{BufMut, BytesMut};
use nom::bits::streaming::take;
use nom::{
    bits,
    combinator::{flat_map, map, map_res},
    error::Error,
    number::streaming::{be_u16, be_u8},
    sequence::tuple,
    IResult, Parser,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectFlags {
    pub username_flag: bool,
    pub password_flag: bool,
    pub will_retain: bool,
    pub will_qos: u8,
    pub will_flag: bool,
    pub clean_session: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
    pub payload: Payload,
}

#[derive(Default)]
pub struct ConnectPacketBuilder {
    client_identifier: String,
    username: Option<String>,
    password: Option<String>,
    will_retain: bool,
    will_qos: u8,
    will_topic: Option<String>,
    will_message: Option<String>,
    clean_session: bool,
    keep_alive: u16,
}

impl ConnectPacketBuilder {
    pub fn new(client_identifier: String) -> ConnectPacketBuilder {
        ConnectPacketBuilder {
            client_identifier,
            username: None,
            password: None,
            will_retain: false,
            will_qos: 0,
            clean_session: false,
            keep_alive: 2,
            will_topic: None,
            will_message: None,
        }
    }

    pub fn will_msg(
        mut self,
        will_topic: String,
        will_message: String,
        will_qos: u8,
        will_retain: bool,
    ) -> ConnectPacketBuilder {
        self.will_topic = Some(will_topic);
        self.will_message = Some(will_message);
        self.will_qos = will_qos;
        self.will_retain = will_retain;
        self
    }

    pub fn username(mut self, username: String) -> ConnectPacketBuilder {
        self.username = Some(username);
        self
    }

    pub fn password(mut self, password: String) -> ConnectPacketBuilder {
        self.password = Some(password);
        self
    }

    pub fn keep_alive(mut self, keep_alive: u16) -> ConnectPacketBuilder {
        self.keep_alive = keep_alive;
        self
    }

    pub fn clean_session(mut self, clean_session: bool) -> ConnectPacketBuilder {
        self.clean_session = clean_session;
        self
    }

    pub fn build(self) -> ConnectPacket {
        let variable_header = VariableHeader {
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x04,
            username_flag: self.username.is_some(),
            password_flag: self.password.is_some(),
            will_retain: self.will_retain,
            will_qos: self.will_qos,
            will_flag: self.will_message.is_some(),
            clean_session: self.clean_session,
            keep_alive: self.keep_alive,
        };

        let payload = Payload {
            client_identifier: self.client_identifier,
            will_topic: self.will_topic,
            will_message: self.will_message,
            username: self.username,
            password: self.password,
        };

        let fix_header = FixHeader {
            packet_type: PacketType::CONNECT,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: variable_header.get_length() + payload.get_length(),
        };

        ConnectPacket {
            fix_header,
            variable_header,
            payload,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableHeader {
    pub protocol_name: String,
    pub protocol_level: u8,
    pub username_flag: bool,
    pub password_flag: bool,
    pub will_retain: bool,
    pub will_qos: u8,
    pub will_flag: bool,
    pub clean_session: bool,
    pub keep_alive: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    pub client_identifier: String,
    pub will_topic: Option<String>,
    pub will_message: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

// MQTT Protocol Name
//
// +----------+-------------+---+---+---+---+---+---+---+---+
// |          | Description | 7 | 6 | 5 | 4 | 3 | 2 | 1 | 0 |
// +----------+-------------+---+---+---+---+---+---+---+---+
// | Protocol |             |   |   |   |   |   |   |   |   |
// +----------+-------------+---+---+---+---+---+---+---+---+
// | byte 1   | MSB (0)     | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
// | byte 2   | LSB (4)     | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 |
// | byte 3   | 'M'         | 0 | 1 | 0 | 0 | 1 | 1 | 0 | 1 |
// | byte 4   | 'Q'         | 0 | 1 | 0 | 1 | 0 | 0 | 0 | 1 |
// | byte 5   | 'T'         | 0 | 1 | 0 | 1 | 0 | 1 | 0 | 0 |
// | byte 6   | 'T'         | 0 | 1 | 0 | 1 | 0 | 1 | 0 | 0 |
// +----------+-------------+---+---+---+---+---+---+---+---+
fn protocol_name(input: &[u8]) -> IResult<&[u8], String> {
    flat_map(be_u16, |protocol_length| {
        nom::bytes::streaming::take(protocol_length).map(|w| String::from_utf8_lossy(w).into())
    })(input)
}

// MQTT Protocol Level
//
// +----------+-------------+---+---+---+---+---+---+---+---+
// |          | Description | 7 | 6 | 5 | 4 | 3 | 2 | 1 | 0 |
// +----------+-------------+---+---+---+---+---+---+---+---+
// | Protocol |             |   |   |   |   |   |   |   |   |
// +----------+-------------+---+---+---+---+---+---+---+---+
// | byte 7   | LEVEL (4)   | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
// +----------+-------------+---+---+---+---+---+---+---+---+
fn protocol_level(input: &[u8]) -> IResult<&[u8], u8> {
    be_u8(input)
}

// MQTT Connect Flags
//
// +-------+----------------+---------------+-------------+----------+---+-----------+---------------+----------+--+
// | Bit   | 7              | 6             | 5           | 4        | 3 | 2         | 1             | 0        |  |
// +-------+----------------+---------------+-------------+----------+---+-----------+---------------+----------+--+
// |       | User Name Flag | Password Flag | Will Retain | Will QoS     | Will Flag | Clean Session | Reserved |  |
// +-------+----------------+---------------+-------------+----------+---+-----------+---------------+----------+--+
// | byte8 | X              | X             | X           | X        | X | X         | X             | 0        |  |
// +-------+----------------+---------------+-------------+----------+---+-----------+---------------+----------+--+
fn connect_flags(input: &[u8]) -> IResult<&[u8], ConnectFlags> {
    map(
        bits::<&[u8], (u8, u8, u8, u8, u8, u8, u8), Error<(&[u8], usize)>, _, _>(tuple((
            take(1usize),
            take(1usize),
            take(1usize),
            take(2usize),
            take(1usize),
            take(1usize),
            take(1usize),
        ))),
        |flags| ConnectFlags {
            username_flag: flags.0 == 1,
            password_flag: flags.1 == 1,
            will_retain: flags.2 == 1,
            will_qos: flags.3,
            will_flag: flags.4 == 1,
            clean_session: flags.5 == 1,
        },
    )(input)
}

// MQTT Keep Alive
//
// +----------+-------------+---+---+---+---+---+---+---+---+
// |          | Description | 7 | 6 | 5 | 4 | 3 | 2 | 1 | 0 |
// +----------+-------------+---+---+---+---+---+---+---+---+
// | Protocol |             |   |   |   |   |   |   |   |   |
// +----------+-------------+---+---+---+---+---+---+---+---+
// | byte 9   | MSB (0)     | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
// | byte 10  | MSB (0)     | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
// +----------+-------------+---+---+---+---+---+---+---+---+
fn keep_alive(input: &[u8]) -> IResult<&[u8], u16> {
    be_u16(input)
}

// MQTT Variable Header
fn variable_header(input: &[u8]) -> IResult<&[u8], VariableHeader> {
    map(
        tuple((protocol_name, protocol_level, connect_flags, keep_alive)),
        |r| {
            let (protocol_name, protocol_level, connect_flags, keep_alive) = r;
            VariableHeader {
                protocol_name,
                protocol_level,
                username_flag: connect_flags.username_flag,
                password_flag: connect_flags.password_flag,
                will_retain: connect_flags.will_retain,
                will_qos: connect_flags.will_qos,
                will_flag: connect_flags.will_flag,
                clean_session: connect_flags.clean_session,
                keep_alive,
            }
        },
    )(input)
}

// MQTT Connect Payload Client Identifier
fn client_identifier(input: &[u8]) -> IResult<&[u8], String> {
    parse_utf8(input)
}

// MQTT Connect Payload Username
fn username(input: &[u8]) -> IResult<&[u8], String> {
    parse_utf8(input)
}

// MQTT Connect Payload Password
fn password(input: &[u8]) -> IResult<&[u8], String> {
    parse_utf8(input)
}

// MQTT Connect Payload Will Topic
fn will_topic(input: &[u8]) -> IResult<&[u8], String> {
    parse_utf8(input)
}

// MQTT Connect Payload Will Message
fn will_message(input: &[u8]) -> IResult<&[u8], String> {
    parse_utf8(input)
}

// MQTT Connect Payload
fn payload(
    username_flag: bool,
    password_flag: bool,
    will_flag: bool,
) -> impl Fn(&[u8]) -> IResult<&[u8], Payload> {
    move |v: &[u8]| {
        if will_flag {
            if username_flag {
                if password_flag {
                    map(
                        tuple((
                            client_identifier,
                            will_topic,
                            will_message,
                            username,
                            password,
                        )),
                        |r| Payload {
                            client_identifier: r.0,
                            will_topic: Some(r.1),
                            will_message: Some(r.2),
                            username: Some(r.3),
                            password: Some(r.4),
                        },
                    )(v)
                } else {
                    map(
                        tuple((client_identifier, will_topic, will_message, username)),
                        |r| Payload {
                            client_identifier: r.0,
                            will_topic: Some(r.1),
                            will_message: Some(r.2),
                            username: Some(r.3),
                            password: None,
                        },
                    )(v)
                }
            } else if password_flag {
                map(
                    tuple((client_identifier, will_topic, will_message, password)),
                    |r| Payload {
                        client_identifier: r.0,
                        will_topic: Some(r.1),
                        will_message: Some(r.2),
                        username: None,
                        password: Some(r.3),
                    },
                )(v)
            } else {
                map(tuple((client_identifier, will_topic, will_message)), |r| {
                    Payload {
                        client_identifier: r.0,
                        will_topic: Some(r.1),
                        will_message: Some(r.2),
                        username: None,
                        password: None,
                    }
                })(v)
            }
        } else if username_flag {
            if password_flag {
                map(tuple((client_identifier, username, password)), |r| {
                    Payload {
                        client_identifier: r.0,
                        will_topic: None,
                        will_message: None,
                        username: Some(r.1),
                        password: Some(r.2),
                    }
                })(v)
            } else {
                map(tuple((client_identifier, username)), |r| Payload {
                    client_identifier: r.0,
                    will_topic: None,
                    will_message: None,
                    username: Some(r.1),
                    password: None,
                })(v)
            }
        } else if password_flag {
            map(tuple((client_identifier, password)), |r| Payload {
                client_identifier: r.0,
                will_topic: None,
                will_message: None,
                username: None,
                password: Some(r.1),
            })(v)
        } else {
            map(client_identifier, |r| Payload {
                client_identifier: r,
                will_topic: None,
                will_message: None,
                username: None,
                password: None,
            })(v)
        }
    }
}

pub fn variable_header_and_payload(input: &[u8]) -> IResult<&[u8], (VariableHeader, Payload)> {
    flat_map(variable_header, |variable_header| {
        map(
            payload(
                variable_header.username_flag,
                variable_header.password_flag,
                variable_header.will_flag,
            ),
            move |payload| {
                (
                    VariableHeader {
                        protocol_name: variable_header.protocol_name.clone(),
                        protocol_level: variable_header.protocol_level,
                        username_flag: variable_header.username_flag,
                        password_flag: variable_header.password_flag,
                        will_retain: variable_header.will_retain,
                        will_qos: variable_header.will_qos,
                        will_flag: variable_header.will_flag,
                        clean_session: variable_header.clean_session,
                        keep_alive: variable_header.keep_alive,
                    },
                    payload,
                )
            },
        )
    })(input)
}

pub fn parse(input: &[u8]) -> IResult<&[u8], ConnectPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
                nom::bytes::streaming::take(fixed_header.remaining_length),
                variable_header_and_payload,
            ),
            move |(_, (variable_header, payload))| {
                let cloned_fixed_header = fixed_header.clone();
                ConnectPacket {
                    fix_header: cloned_fixed_header,
                    variable_header,
                    payload,
                }
            },
        )
    })(input)
}

impl VariableHeader {
    pub fn get_length(&self) -> usize {
        2 + self.protocol_name.len() + 2 + 2
    }

    pub(crate) fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16(self.protocol_name.len() as u16);

        for byte in self.protocol_name.as_bytes() {
            buf.put_u8(*byte);
        }
        buf.put_u8(0x04);

        let mut connect_flags: u8 = 0;

        if self.username_flag {
            connect_flags += 1 << 7;
        }
        if self.password_flag {
            connect_flags += 1 << 6;
        }

        if self.will_retain {
            connect_flags += 1 << 5;
        }

        connect_flags += self.will_qos << 3;

        if self.will_flag {
            connect_flags += 1 << 2;
        }

        if self.clean_session {
            connect_flags += 1 << 1;
        }

        buf.put_u8(connect_flags);

        buf.put_u16(self.keep_alive);
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(10);

        buf.put_u16(self.protocol_name.len() as u16);

        for byte in self.protocol_name.as_bytes() {
            buf.put_u8(*byte);
        }
        buf.put_u8(0x04);

        let mut connect_flags: u8 = 0;

        if self.username_flag {
            connect_flags += 1 << 7;
        }
        if self.password_flag {
            connect_flags += 1 << 6;
        }

        if self.will_retain {
            connect_flags += 1 << 5;
        }

        connect_flags += self.will_qos << 3;

        if self.will_flag {
            connect_flags += 1 << 2;
        }

        if self.clean_session {
            connect_flags += 1 << 1;
        }

        buf.put_u8(connect_flags);

        buf.put_u16(self.keep_alive);

        buf
    }
}

impl Payload {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16(self.client_identifier.len() as u16);
        buf.put(self.client_identifier.as_bytes());

        if self.will_topic.is_some() {
            buf.put_u16(self.will_topic.as_ref().unwrap().len() as u16);
            buf.put(self.will_topic.as_ref().unwrap().as_bytes());
        }

        if self.will_message.is_some() {
            buf.put_u16(self.will_message.as_ref().unwrap().len() as u16);
            buf.put(self.will_message.as_ref().unwrap().as_bytes());
        }

        if self.username.is_some() {
            buf.put_u16(self.username.as_ref().unwrap().len() as u16);
            buf.put(self.username.as_ref().unwrap().as_bytes());
        }

        if self.password.is_some() {
            buf.put_u16(self.password.as_ref().unwrap().len() as u16);
            buf.put(self.password.as_ref().unwrap().as_bytes());
        }
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.get_length());
        buf.put_u16(self.client_identifier.len() as u16);
        buf.put(self.client_identifier.as_bytes());

        if self.will_topic.is_some() {
            buf.put_u16(self.will_topic.as_ref().unwrap().len() as u16);
            buf.put(self.will_topic.as_ref().unwrap().as_bytes());
        }

        if self.will_message.is_some() {
            buf.put_u16(self.will_message.as_ref().unwrap().len() as u16);
            buf.put(self.will_message.as_ref().unwrap().as_bytes());
        }

        if self.username.is_some() {
            buf.put_u16(self.username.as_ref().unwrap().len() as u16);
            buf.put(self.username.as_ref().unwrap().as_bytes());
        }

        if self.password.is_some() {
            buf.put_u16(self.password.as_ref().unwrap().len() as u16);
            buf.put(self.password.as_ref().unwrap().as_bytes());
        }

        buf
    }

    pub fn get_length(&self) -> usize {
        let mut len = 0;

        len = len + self.client_identifier.len() + 2;

        if self.will_topic.is_some() {
            len = len + self.will_topic.as_ref().unwrap().len() + 2;
        }

        if self.will_message.is_some() {
            len = len + self.will_message.as_ref().unwrap().len() + 2;
        }

        if self.username.is_some() {
            len = len + self.username.as_ref().unwrap().len() + 2;
        }

        if self.password.is_some() {
            len = len + self.password.as_ref().unwrap().len() + 2;
        }

        len
    }
}

impl MqttPacket for ConnectPacket {
    fn to_bytes(&self) -> BytesMut {
        let fix_header_bytes = self.fix_header.to_bytes();
        let variable_bytes = self.variable_header.to_bytes();
        let payload_bytes = self.payload.to_bytes();

        let mut buf: BytesMut = BytesMut::with_capacity(
            fix_header_bytes.len() + variable_bytes.len() + payload_bytes.len(),
        );
        buf.put(fix_header_bytes);
        buf.put(variable_bytes);
        buf.put(payload_bytes);
        buf
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.fix_header.ecnode(buf);
        self.variable_header.encode(buf);
        self.payload.encode(buf);
    }

    fn get_packet_type(&self) -> crate::PacketType {
        crate::PacketType::CONNECT
    }
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;

    use crate::{
        v3::{
            connect::{protocol_level, ConnectPacketBuilder},
            fixed_header::FixHeader,
        },
        MqttPacket, PacketType,
    };

    use super::{connect_flags, parse, payload, protocol_name, ConnectPacket};

    #[test]
    fn test_connect_flag() {
        let input = &[0xF6];
        let (_, flags) = connect_flags(input).unwrap();
        assert_eq!(flags.username_flag, true);
        assert_eq!(flags.password_flag, true);
        assert_eq!(flags.will_retain, true);
        assert_eq!(flags.will_qos, 2);
        assert_eq!(flags.will_flag, true);
        assert_eq!(flags.clean_session, true);
    }

    #[test]
    fn test_protocol_name() {
        let input = &[0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x4];
        let protocol_name = protocol_name(input).unwrap();
        assert_eq!(protocol_name.1, "MQTT".to_string());
        let protocol_level = protocol_level(protocol_name.0).unwrap();
        assert_eq!(protocol_level.1, 0x4);
    }

    #[test]
    fn test_payload() {
        let input = &[
            0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04,
            0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51,
            0x54, 0x54,
        ];
        let payload = payload(true, true, true)(input).unwrap();
        assert_eq!(payload.1.client_identifier, "MQTT".to_string());
        assert_eq!(payload.1.will_topic.unwrap(), "MQTT".to_string());
        assert_eq!(payload.1.will_message.unwrap(), "MQTT".to_string());
        assert_eq!(payload.1.username.unwrap(), "MQTT".to_string());
        assert_eq!(payload.1.password.unwrap(), "MQTT".to_string());
    }

    #[test]
    fn test_parse() {
        let input = &[
            0x10, 0x28, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0xEE, 0x00, 0x00, 0x00, 0x04,
            0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51,
            0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54,
        ];
        let out = parse(input).unwrap();
        assert_eq!(out.1.variable_header.clean_session, true);
        assert_eq!(out.1.variable_header.password_flag, true);
        assert_eq!(out.1.variable_header.username_flag, true);
        assert_eq!(out.1.variable_header.will_qos, 1);
        assert_eq!(out.1.variable_header.protocol_level, 0x04);
        assert_eq!(out.1.variable_header.keep_alive, 0x00);
        assert_eq!(out.1.payload.client_identifier, "MQTT".to_string());
        assert_eq!(out.1.payload.will_topic.unwrap(), "MQTT".to_string());
        assert_eq!(out.1.payload.will_message.unwrap(), "MQTT".to_string());
        assert_eq!(out.1.payload.username.unwrap(), "MQTT".to_string());
        assert_eq!(out.1.payload.password.unwrap(), "MQTT".to_string());
    }

    #[test]
    fn test_to_bytes() {
        let variable_header = super::VariableHeader {
            protocol_name: "MQTT".to_string(),
            protocol_level: 0x04,
            username_flag: true,
            password_flag: true,
            will_retain: true,
            will_qos: 1,
            will_flag: true,
            clean_session: true,
            keep_alive: 0,
        };
        let payload = super::Payload {
            client_identifier: "MQTT".to_string(),
            will_topic: Some("MQTT".to_string()),
            will_message: Some("MQTT".to_string()),
            username: Some("MQTT".to_string()),
            password: Some("MQTT".to_string()),
        };

        let fix_header = FixHeader {
            packet_type: PacketType::CONNECT,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: variable_header.get_length() + payload.get_length(),
        };

        let connect_packet = ConnectPacket {
            fix_header,
            variable_header,
            payload,
        };

        let connect_packet_bytes = connect_packet.to_bytes();

        let input = &[
            0x10, 0x28, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0xEE, 0x00, 0x00, 0x00, 0x04,
            0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51,
            0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54,
        ];

        assert_eq!(connect_packet_bytes.as_bytes(), input);
    }

    #[test]
    fn test_connect_packet_builder() {
        let connect_packet_builder = ConnectPacketBuilder::new("MQTT".to_string());
        let packet = connect_packet_builder
            .will_msg("MQTT".to_string(), "MQTT".to_string(), 1, true)
            .username("MQTT".to_string())
            .password("MQTT".to_string())
            .clean_session(true)
            .keep_alive(0)
            .build();
        let input = &[
            0x10, 0x28, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0xEE, 0x00, 0x00, 0x00, 0x04,
            0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51,
            0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x00, 0x04, 0x4D, 0x51, 0x54, 0x54,
        ];
        assert_eq!(packet.to_bytes().as_bytes(), input);
    }
}
