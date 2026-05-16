use bytes::{BufMut, BytesMut};

use super::common::{encode_variable_byte_integer, parse_variable_byte_integer, Mqtt5ParseError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPacketType {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
    Auth = 15,
}

impl ControlPacketType {
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for ControlPacketType {
    type Error = Mqtt5ParseError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ControlPacketType::Connect),
            2 => Ok(ControlPacketType::Connack),
            3 => Ok(ControlPacketType::Publish),
            4 => Ok(ControlPacketType::Puback),
            5 => Ok(ControlPacketType::Pubrec),
            6 => Ok(ControlPacketType::Pubrel),
            7 => Ok(ControlPacketType::Pubcomp),
            8 => Ok(ControlPacketType::Subscribe),
            9 => Ok(ControlPacketType::Suback),
            10 => Ok(ControlPacketType::Unsubscribe),
            11 => Ok(ControlPacketType::Unsuback),
            12 => Ok(ControlPacketType::Pingreq),
            13 => Ok(ControlPacketType::Pingresp),
            14 => Ok(ControlPacketType::Disconnect),
            15 => Ok(ControlPacketType::Auth),
            _ => Err(Mqtt5ParseError::UnsupportedPacketType(value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedHeader {
    pub packet_type: ControlPacketType,
    pub flags: u8,
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    pub remaining_length: u32,
}

impl FixedHeader {
    pub fn new(
        packet_type: ControlPacketType,
        flags: u8,
        remaining_length: u32,
    ) -> Result<Self, Mqtt5ParseError> {
        validate_flags(packet_type, flags)?;
        Ok(Self {
            packet_type,
            flags,
            dup: flags & 0b1000 != 0,
            qos: (flags & 0b0110) >> 1,
            retain: flags & 0b0001 != 0,
            remaining_length,
        })
    }

    pub fn encode(&self, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
        validate_flags(self.packet_type, self.flags)?;
        buffer.put_u8((self.packet_type.as_u8() << 4) | (self.flags & 0x0f));
        encode_variable_byte_integer(self.remaining_length, buffer)
    }
}

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], FixedHeader), Mqtt5ParseError> {
    let Some(first_byte) = input.first().copied() else {
        return Err(Mqtt5ParseError::Incomplete("fixed header"));
    };

    let packet_type = ControlPacketType::try_from(first_byte >> 4)?;
    let flags = first_byte & 0x0f;
    validate_flags(packet_type, flags)?;

    let (input, remaining_length) = parse_variable_byte_integer(&input[1..])?;
    if remaining_length > max_message_size {
        return Err(Mqtt5ParseError::RemainingLengthExceeded {
            remaining_length,
            max_message_size,
        });
    }

    Ok((
        input,
        FixedHeader {
            packet_type,
            flags,
            dup: flags & 0b1000 != 0,
            qos: (flags & 0b0110) >> 1,
            retain: flags & 0b0001 != 0,
            remaining_length,
        },
    ))
}

fn validate_flags(packet_type: ControlPacketType, flags: u8) -> Result<(), Mqtt5ParseError> {
    let valid = match packet_type {
        ControlPacketType::Publish => ((flags & 0b0110) >> 1) != 0b11,
        ControlPacketType::Pubrel
        | ControlPacketType::Subscribe
        | ControlPacketType::Unsubscribe => flags == 0b0010,
        _ => flags == 0,
    };

    if valid {
        Ok(())
    } else {
        Err(Mqtt5ParseError::InvalidFixedHeaderFlags {
            packet_type: packet_type.as_u8(),
            flags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_publish_fixed_header_flags() {
        let (_, header) = parse(&[0b0011_1011, 0x05], 1024).unwrap();

        assert_eq!(header.packet_type, ControlPacketType::Publish);
        assert_eq!(header.flags, 0b1011);
        assert!(header.dup);
        assert_eq!(header.qos, 1);
        assert!(header.retain);
        assert_eq!(header.remaining_length, 5);
    }

    #[test]
    fn parses_auth_packet_type() {
        let (_, header) = parse(&[0b1111_0000, 0x00], 1024).unwrap();

        assert_eq!(header.packet_type, ControlPacketType::Auth);
        assert_eq!(header.remaining_length, 0);
    }

    #[test]
    fn rejects_invalid_subscribe_flags() {
        let err = parse(&[0b1000_0000, 0x00], 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::InvalidFixedHeaderFlags {
                packet_type: ControlPacketType::Subscribe.as_u8(),
                flags: 0
            }
        );
    }

    #[test]
    fn rejects_publish_qos_three() {
        let err = parse(&[0b0011_0110, 0x00], 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::InvalidFixedHeaderFlags {
                packet_type: ControlPacketType::Publish.as_u8(),
                flags: 0b0110
            }
        );
    }

    #[test]
    fn rejects_remaining_length_above_limit() {
        let err = parse(&[0b0011_0000, 0x80, 0x01], 127).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::RemainingLengthExceeded {
                remaining_length: 128,
                max_message_size: 127
            }
        );
    }

    #[test]
    fn encodes_fixed_header() {
        let header = FixedHeader::new(ControlPacketType::Subscribe, 0b0010, 128).unwrap();
        let mut buffer = BytesMut::new();

        header.encode(&mut buffer).unwrap();

        assert_eq!(buffer.to_vec(), vec![0x82, 0x80, 0x01]);
    }
}
