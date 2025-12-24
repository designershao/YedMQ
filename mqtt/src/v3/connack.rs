use bytes::BufMut;
use nom::{IResult, combinator::{map_res, flat_map, map}, sequence::tuple, error::Error};
use nom::bytes::streaming::take;
use ::bytes::BytesMut;
use serde::{Deserialize, Serialize};

use crate::{MqttPacket, PacketType};

use super::fixed_header::{FixHeader, self};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnAckPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
}

pub enum ConnackReturnCode {
    Accept = 0x00,
    UnsupportedProtocolVersion = 0x01,
    InvalidClientIdentifier = 0x02,
    ServerUnavailable = 0x03,
    InvalidUsernameOrPassword = 0x04,
    UnAuthorized = 0x05,
}

pub struct ConnAckPacketBuilder {
    return_code: ConnackReturnCode,
    session_present: bool
}

impl Default for ConnAckPacketBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnAckPacketBuilder {
    pub fn new() -> ConnAckPacketBuilder {
        ConnAckPacketBuilder { return_code: ConnackReturnCode::Accept, session_present: false }
    }

    pub fn set_session_present(mut self, session_present: bool) -> Self {
        self.session_present = session_present;
        self
    }

    pub fn set_return_code(mut self, return_code: ConnackReturnCode) -> Self {
        self.return_code = return_code;
        self
    }

    pub fn build(self) -> ConnAckPacket {
        let fix_header = FixHeader {
            packet_type: PacketType::CONNACK,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 2,
        };
        let return_code = match self.return_code {
            ConnackReturnCode::Accept => 0x00,
            ConnackReturnCode::UnsupportedProtocolVersion => 0x01,
            ConnackReturnCode::InvalidClientIdentifier => 0x02,
            ConnackReturnCode::ServerUnavailable => 0x03,
            ConnackReturnCode::InvalidUsernameOrPassword => 0x04,
            ConnackReturnCode::UnAuthorized => 0x05,
        };
        let variable_header = VariableHeader {
            session_present: self.session_present,
            connect_return_code: return_code,
        };
        
        ConnAckPacket{
            fix_header,
            variable_header,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableHeader {
    pub session_present: bool,
    pub connect_return_code: u8,
}


// MQTT Connect ACK Variable Header
// +---------------------+----------+---+---+---+---+---+---+---+----+
// |                     | Desc     | 7 | 6 | 5 | 4 | 3 | 2 | 1 | 0  |
// +---------------------+----------+---+---+---+---+---+---+---+----+
// |                     | Reserved |   |   |   |   |   |   |   | SP |
// | byte1               |          | 0 | 0 | 0 | 0 | 0 | 0 | 0 | x  |
// | Connect Return Code                                             |
// | byte2               |          | x | x | x | x | x | x | x | x  |
// +---------------------+----------+---+---+---+---+---+---+---+----+
fn variable_header(input:&[u8]) -> IResult<&[u8], VariableHeader> {
    map(
        tuple((take::<u8, &[u8], Error<&[u8]>>(1u8),take(1u8))),
        |v| {
            let session_present = v.0[0] & 0x1 == 0x1;
            VariableHeader{
                session_present,
                connect_return_code: v.1[0]
            }
        }
    )(input)
}

pub fn parse(input: &[u8]) -> IResult<&[u8], ConnAckPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
            nom::bytes::streaming::take(fixed_header.remaining_length),
            variable_header
            ), move |(_,variable_header)| {
                let cloned_fixed_header = fixed_header.clone();
                ConnAckPacket { 
                    fix_header:cloned_fixed_header , 
                    variable_header 
                }
            }
        )
    })(input)
}

impl VariableHeader {
    pub fn encode(&self, buf: &mut BytesMut) {
        if self.session_present {
            buf.put_u8(1);
        } else {
            buf.put_u8(0)
        }
        buf.put_u8(self.connect_return_code);
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(2);
        if self.session_present {
            buf.put_u8(1);
        } else {
            buf.put_u8(0)
        }
        buf.put_u8(self.connect_return_code);
        buf
    }
}

impl MqttPacket for ConnAckPacket {
    fn to_bytes(&self) -> BytesMut {
        let fix_header_bytes = self.fix_header.to_bytes();
        let variable_header_bytes = self.variable_header.to_bytes();

        let mut buf = BytesMut::with_capacity(fix_header_bytes.len() + variable_header_bytes.len());
        buf.put(fix_header_bytes);
        buf.put(variable_header_bytes);
        buf
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.fix_header.ecnode(buf);
        self.variable_header.encode(buf);
    }

    fn get_packet_type(&self) -> crate::PacketType {
        crate::PacketType::CONNACK
    }
}

#[cfg(test)]
mod tests {
    use nom::AsBytes;

    use crate::PacketType;

    use super::*;

    #[test]
    fn test_variable_header() {
        let input = &[0x01, 0x01];
        let output = variable_header(input).unwrap();
        assert_eq!(output.1.session_present, true);
        assert_eq!(output.1.connect_return_code, 1);
    }

    #[test]
    fn test_parse() {
        let input = &[0x20,0x02,0x01,0x01];
        let out = parse(input).unwrap();
        assert_eq!(out.1.variable_header.connect_return_code, 0x01);
        assert_eq!(out.1.variable_header.session_present, true);
    }

    #[test]
    fn test_to_bytes() {
        let fix_header = FixHeader {
            packet_type: PacketType::CONNACK,
            qos: None,
            retain: None,
            dup: None,
            remaining_length: 2,
        };
        let variable_header = VariableHeader {
            session_present: true,
            connect_return_code: 1,
        };
        let connack_packet = ConnAckPacket{
            fix_header,
            variable_header,
        };
        let connack_packet_bytes = connack_packet.to_bytes();

        let input = &[0x20,0x02,0x01,0x01];

        assert_eq!(connack_packet_bytes.as_bytes(), input);
    }
}