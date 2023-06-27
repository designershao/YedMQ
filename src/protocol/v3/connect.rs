use byteorder::{BigEndian, ByteOrder};
use nom::{IResult, Parser, number::streaming::{be_u16, be_u8}, combinator::{map_res, flat_map, map}, sequence::tuple, bits, error::Error};
use crate::protocol::v3::fixed_header;
use crate::protocol::v3::common::parse_utf8;
use nom::bits::{streaming::take};
use super::fixed_header::FixHeader;

pub struct ConnectPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
    pub payload: Payload
}

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
    flat_map(be_u16, |protocol_length|{
        nom::bytes::streaming::take(protocol_length).map(|w|{String::from_utf8_lossy(w).into()})
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
fn protocol_level(input:&[u8]) -> IResult<&[u8], u8> {
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
fn connect_flags(input: &[u8]) -> IResult<&[u8], (bool, bool, bool, u8, bool, bool)> {
    map(
        bits::<&[u8], (u8, u8, u8, u8, u8, u8, u8), Error<(&[u8], usize)>,_, _>(tuple((take(1usize), take(1usize), take(1usize), take(2usize), take(1usize), take(1usize),take(1usize)))),
        |flags| {
            (flags.0 == 1, flags.1 == 1, flags.2 == 1, flags.3, flags.4 == 1, flags.5== 1)
        })(input)
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
    map(tuple((protocol_name, protocol_level, connect_flags, keep_alive)), |r| {
        let (protocol_name, protocol_level, connect_flags, keep_alive) = r;
        VariableHeader {
            protocol_name,
            protocol_level,
            username_flag: connect_flags.0,
            password_flag: connect_flags.1,
            will_retain: connect_flags.2,
            will_qos: connect_flags.3,
            will_flag: connect_flags.4,
            clean_session: connect_flags.5,
            keep_alive
        }
    })(input)
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
fn payload(username_flag: bool, password_flag: bool, will_flag: bool) -> impl Fn(&[u8]) -> IResult<&[u8], Payload> {
    move |v:&[u8]| {
        if will_flag {
            if username_flag {
                if password_flag {
                    map(tuple((client_identifier, will_topic, will_message,username, password)),|r|{
                        Payload{
                            client_identifier: r.0,
                            will_topic: Some(r.1),
                            will_message: Some(r.2),
                            username: Some(r.3),
                            password: Some(r.4),
                        }
                    })(v)
                } else {
                    map(tuple((client_identifier, will_topic, will_message,username)),|r|{
                        Payload{
                            client_identifier: r.0,
                            will_topic: Some(r.1),
                            will_message: Some(r.2),
                            username: Some(r.3),
                            password: None,
                        }
                    })(v)
                }
            } else {
                if password_flag {
                    map(tuple((client_identifier, will_topic, will_message,password)),|r|{
                        Payload{
                            client_identifier: r.0,
                            will_topic: Some(r.1),
                            will_message: Some(r.2),
                            username: None,
                            password: Some(r.3),
                        }
                    })(v)

                } else {
                    map(tuple((client_identifier, will_topic, will_message)),|r|{
                        Payload{
                            client_identifier: r.0,
                            will_topic: Some(r.1),
                            will_message: Some(r.2),
                            username: None,
                            password: None,
                        }
                    })(v)

                }
            }
        } else {
            if username_flag {
                if password_flag {
                    map(tuple((client_identifier, username, password)), |r|{
                        Payload{
                            client_identifier: r.0,
                            will_topic: None,
                            will_message: None,
                            username: Some(r.1),
                            password: Some(r.2),
                        }
                    })(v)

                } else {
                    map(tuple((client_identifier, username)), |r|{
                        Payload{
                            client_identifier: r.0,
                            will_topic: None,
                            will_message: None,
                            username: Some(r.1),
                            password: None,
                        }
                    })(v)

                }
            } else {
                if password_flag {
                    map(tuple((client_identifier, password)), |r|{
                        Payload{
                            client_identifier: r.0,
                            will_topic: None,
                            will_message: None,
                            username: None,
                            password: Some(r.1),
                        }
                    })(v)

                } else {
                    map(client_identifier, |r|{
                        Payload{
                            client_identifier: r,
                            will_topic: None,
                            will_message: None,
                            username: None,
                            password: None,
                        }
                    })(v)

                }
            }
        }
    }
}

pub fn variable_header_and_payload(input: &[u8]) -> IResult<&[u8], (VariableHeader, Payload)> {
    flat_map(variable_header, |variable_header|{
        map(
            payload(variable_header.username_flag, variable_header.password_flag, variable_header.will_flag),
            move |payload| {
                (VariableHeader{
                    protocol_name: variable_header.protocol_name.clone(),
                    protocol_level: variable_header.protocol_level,
                    username_flag: variable_header.username_flag,
                    password_flag: variable_header.password_flag,
                    will_retain: variable_header.will_retain,
                    will_qos: variable_header.will_qos,
                    will_flag: variable_header.will_flag,
                    clean_session: variable_header.clean_session,
                    keep_alive: variable_header.keep_alive,
                }, payload)
            })
    })(input)
}

pub fn parse(input: &[u8]) -> IResult<&[u8], ConnectPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
            nom::bytes::streaming::take(fixed_header.remaining_length),
            variable_header_and_payload
            ),
            move |(_, (variable_header,payload))| {
                let cloned_fixed_header = fixed_header.clone();
                ConnectPacket {
                    fix_header: cloned_fixed_header,
                    variable_header,
                    payload,
                }
            })
        })(input)
}

#[cfg(test)]
mod tests {
    use crate::protocol::v3::connect::protocol_level;

    use super::{connect_flags, protocol_name, payload};


    #[test]
    fn test_connect_flag() {
        let input = &[0xF6];
        let (_, flags) =connect_flags(input).unwrap();
        assert_eq!(flags.0, true);
        assert_eq!(flags.1, true);
        assert_eq!(flags.2,true);
        assert_eq!(flags.3, 2);
        assert_eq!(flags.4,true);
        assert_eq!(flags.5, true);
    }

    #[test]
    fn test_protocl_name() {
        let input = &[0x00,0x04,0x4D,0x51,0x54, 0x54,0x4];
        let protocol_name = protocol_name(input).unwrap();
        assert_eq!(protocol_name.1, "MQTT".to_string());
        let protocol_level = protocol_level(protocol_name.0).unwrap();
        assert_eq!(protocol_level.1, 0x4);
    }

    #[test]
    fn test_payload() {
        let input = &[0x00,0x04,0x4D,0x51,0x54,0x54,0x00,0x04,0x4D,0x51,0x54,0x54,0x00,0x04,0x4D,0x51,0x54,0x54,0x00,0x04,0x4D,0x51,0x54,0x54,0x00,0x04,0x4D,0x51,0x54,0x54];
        let payload = payload(true, true, true)(input).unwrap();
        assert_eq!(payload.1.client_identifier, "MQTT".to_string());
        assert_eq!(payload.1.will_topic.unwrap(), "MQTT".to_string());
        assert_eq!(payload.1.will_message.unwrap(), "MQTT".to_string());
        assert_eq!(payload.1.username.unwrap(), "MQTT".to_string());
        assert_eq!(payload.1.password.unwrap(), "MQTT".to_string());
    }

}