use std::{error::Error, fmt, str};

use bytes::{BufMut, Bytes, BytesMut};

pub const MAX_VARIABLE_BYTE_INTEGER: u32 = 268_435_455;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mqtt5ParseError {
    Incomplete(&'static str),
    MalformedVariableByteInteger,
    VariableByteIntegerTooLarge(u32),
    StringTooLong(usize),
    BinaryTooLong(usize),
    InvalidUtf8,
    MalformedPacket(&'static str),
    UnsupportedPacketType(u8),
    InvalidFixedHeaderFlags {
        packet_type: u8,
        flags: u8,
    },
    RemainingLengthExceeded {
        remaining_length: u32,
        max_message_size: u32,
    },
    UnknownProperty(u32),
    DuplicateProperty(&'static str),
    PropertyNotAllowed {
        property: &'static str,
        scope: &'static str,
    },
    InvalidPropertyValue(&'static str),
    UnknownReasonCode(u8),
    ReasonCodeNotAllowed {
        reason_code: u8,
        packet_type: &'static str,
    },
}

impl fmt::Display for Mqtt5ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Mqtt5ParseError::Incomplete(context) => write!(f, "incomplete MQTT 5 {context}"),
            Mqtt5ParseError::MalformedVariableByteInteger => {
                write!(f, "malformed MQTT 5 variable byte integer")
            }
            Mqtt5ParseError::VariableByteIntegerTooLarge(value) => {
                write!(f, "MQTT 5 variable byte integer too large: {value}")
            }
            Mqtt5ParseError::StringTooLong(length) => {
                write!(f, "MQTT 5 UTF-8 string too long: {length}")
            }
            Mqtt5ParseError::BinaryTooLong(length) => {
                write!(f, "MQTT 5 binary data too long: {length}")
            }
            Mqtt5ParseError::InvalidUtf8 => write!(f, "invalid MQTT 5 UTF-8 string"),
            Mqtt5ParseError::MalformedPacket(reason) => {
                write!(f, "malformed MQTT 5 packet: {reason}")
            }
            Mqtt5ParseError::UnsupportedPacketType(packet_type) => {
                write!(f, "unsupported MQTT 5 packet type: {packet_type}")
            }
            Mqtt5ParseError::InvalidFixedHeaderFlags { packet_type, flags } => write!(
                f,
                "invalid MQTT 5 fixed header flags {flags:#06b} for packet type {packet_type}"
            ),
            Mqtt5ParseError::RemainingLengthExceeded {
                remaining_length,
                max_message_size,
            } => write!(
                f,
                "MQTT 5 remaining length {remaining_length} exceeds max message size {max_message_size}"
            ),
            Mqtt5ParseError::UnknownProperty(identifier) => {
                write!(f, "unknown MQTT 5 property identifier: {identifier:#x}")
            }
            Mqtt5ParseError::DuplicateProperty(property) => {
                write!(f, "duplicate MQTT 5 property: {property}")
            }
            Mqtt5ParseError::PropertyNotAllowed { property, scope } => {
                write!(f, "MQTT 5 property {property} is not allowed on {scope}")
            }
            Mqtt5ParseError::InvalidPropertyValue(property) => {
                write!(f, "invalid MQTT 5 property value: {property}")
            }
            Mqtt5ParseError::UnknownReasonCode(reason_code) => {
                write!(f, "unknown MQTT 5 reason code: {reason_code:#x}")
            }
            Mqtt5ParseError::ReasonCodeNotAllowed {
                reason_code,
                packet_type,
            } => write!(
                f,
                "MQTT 5 reason code {reason_code:#x} is not allowed on {packet_type}"
            ),
        }
    }
}

impl Error for Mqtt5ParseError {}

pub fn parse_variable_byte_integer(input: &[u8]) -> Result<(&[u8], u32), Mqtt5ParseError> {
    let mut value = 0u32;
    let mut multiplier = 1u32;

    for index in 0..4 {
        let Some(encoded_byte) = input.get(index).copied() else {
            return Err(Mqtt5ParseError::Incomplete("variable byte integer"));
        };

        value += ((encoded_byte & 0x7f) as u32) * multiplier;

        if encoded_byte & 0x80 == 0 {
            return Ok((&input[(index + 1)..], value));
        }

        multiplier *= 128;
    }

    Err(Mqtt5ParseError::MalformedVariableByteInteger)
}

pub fn encode_variable_byte_integer(
    mut value: u32,
    buffer: &mut BytesMut,
) -> Result<(), Mqtt5ParseError> {
    if value > MAX_VARIABLE_BYTE_INTEGER {
        return Err(Mqtt5ParseError::VariableByteIntegerTooLarge(value));
    }

    loop {
        let mut encoded_byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            encoded_byte |= 0x80;
        }
        buffer.put_u8(encoded_byte);
        if value == 0 {
            return Ok(());
        }
    }
}

pub fn variable_byte_integer_len(mut value: u32) -> Result<usize, Mqtt5ParseError> {
    if value > MAX_VARIABLE_BYTE_INTEGER {
        return Err(Mqtt5ParseError::VariableByteIntegerTooLarge(value));
    }

    let mut length = 1;
    while value >= 128 {
        value /= 128;
        length += 1;
    }
    Ok(length)
}

pub fn parse_u8<'a>(
    input: &'a [u8],
    context: &'static str,
) -> Result<(&'a [u8], u8), Mqtt5ParseError> {
    let Some(value) = input.first().copied() else {
        return Err(Mqtt5ParseError::Incomplete(context));
    };
    Ok((&input[1..], value))
}

pub fn parse_u16<'a>(
    input: &'a [u8],
    context: &'static str,
) -> Result<(&'a [u8], u16), Mqtt5ParseError> {
    if input.len() < 2 {
        return Err(Mqtt5ParseError::Incomplete(context));
    }
    Ok((&input[2..], u16::from_be_bytes([input[0], input[1]])))
}

pub fn parse_u32<'a>(
    input: &'a [u8],
    context: &'static str,
) -> Result<(&'a [u8], u32), Mqtt5ParseError> {
    if input.len() < 4 {
        return Err(Mqtt5ParseError::Incomplete(context));
    }
    Ok((
        &input[4..],
        u32::from_be_bytes([input[0], input[1], input[2], input[3]]),
    ))
}

pub fn parse_binary(input: &[u8]) -> Result<(&[u8], Bytes), Mqtt5ParseError> {
    let (input, length) = parse_u16(input, "binary data length")?;
    let length = length as usize;
    if input.len() < length {
        return Err(Mqtt5ParseError::Incomplete("binary data"));
    }
    Ok((&input[length..], Bytes::copy_from_slice(&input[..length])))
}

pub fn parse_utf8_string(input: &[u8]) -> Result<(&[u8], String), Mqtt5ParseError> {
    let (input, data) = parse_binary(input)?;
    let value = str::from_utf8(&data).map_err(|_| Mqtt5ParseError::InvalidUtf8)?;
    Ok((input, value.to_string()))
}

pub fn parse_boolean<'a>(
    input: &'a [u8],
    property: &'static str,
) -> Result<(&'a [u8], bool), Mqtt5ParseError> {
    let (input, value) = parse_u8(input, property)?;
    match value {
        0 => Ok((input, false)),
        1 => Ok((input, true)),
        _ => Err(Mqtt5ParseError::InvalidPropertyValue(property)),
    }
}

pub fn encode_utf8_string(value: &str, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    let length = value.len();
    if length > u16::MAX as usize {
        return Err(Mqtt5ParseError::StringTooLong(length));
    }
    buffer.put_u16(length as u16);
    buffer.extend_from_slice(value.as_bytes());
    Ok(())
}

pub fn encode_binary(value: &Bytes, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    let length = value.len();
    if length > u16::MAX as usize {
        return Err(Mqtt5ParseError::BinaryTooLong(length));
    }
    buffer.put_u16(length as u16);
    buffer.extend_from_slice(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variable_byte_integer_round_trips_boundaries() {
        let cases = [
            (0, vec![0x00]),
            (127, vec![0x7f]),
            (128, vec![0x80, 0x01]),
            (16_383, vec![0xff, 0x7f]),
            (16_384, vec![0x80, 0x80, 0x01]),
            (MAX_VARIABLE_BYTE_INTEGER, vec![0xff, 0xff, 0xff, 0x7f]),
        ];

        for (value, expected) in cases {
            let mut buffer = BytesMut::new();
            encode_variable_byte_integer(value, &mut buffer).unwrap();
            assert_eq!(buffer.to_vec(), expected);
            assert_eq!(
                parse_variable_byte_integer(&buffer).unwrap(),
                (&[][..], value)
            );
        }
    }

    #[test]
    fn variable_byte_integer_rejects_malformed_fifth_continuation() {
        let err = parse_variable_byte_integer(&[0xff, 0xff, 0xff, 0xff, 0x00]).unwrap_err();
        assert_eq!(err, Mqtt5ParseError::MalformedVariableByteInteger);
    }

    #[test]
    fn variable_byte_integer_rejects_encode_value_above_mqtt_limit() {
        let mut buffer = BytesMut::new();
        let err =
            encode_variable_byte_integer(MAX_VARIABLE_BYTE_INTEGER + 1, &mut buffer).unwrap_err();
        assert_eq!(
            err,
            Mqtt5ParseError::VariableByteIntegerTooLarge(MAX_VARIABLE_BYTE_INTEGER + 1)
        );
    }

    #[test]
    fn utf8_string_parser_rejects_invalid_utf8() {
        let err = parse_utf8_string(&[0x00, 0x01, 0xff]).unwrap_err();
        assert_eq!(err, Mqtt5ParseError::InvalidUtf8);
    }
}
