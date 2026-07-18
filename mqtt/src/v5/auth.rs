use bytes::{BufMut, BytesMut};

use crate::packet::{Auth, Packet, Properties, ReasonCode};

use super::{
    common::{parse_u8, Mqtt5ParseError},
    fixed_header::{self, ControlPacketType, FixedHeader},
    properties::{self, PropertyScope},
    reason_code,
};

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != ControlPacketType::Auth {
        return Err(Mqtt5ParseError::MalformedPacket("expected AUTH packet"));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if input.len() < remaining_length {
        return Err(Mqtt5ParseError::Incomplete("AUTH body"));
    }
    let (body, input) = input.split_at(remaining_length);
    let (_, auth) = parse_body(body)?;

    Ok((input, Packet::Auth(auth)))
}

fn parse_body(input: &[u8]) -> Result<(&[u8], Auth), Mqtt5ParseError> {
    let (reason_code, properties) = if input.is_empty() {
        (ReasonCode::Success, Properties::default())
    } else {
        let (input, reason_code) = parse_u8(input, "AUTH reason code")?;
        let reason_code = reason_code::decode_for_packet(reason_code, ControlPacketType::Auth)?;
        if input.is_empty() {
            (reason_code, Properties::default())
        } else {
            let (input, properties) = properties::parse(input, PropertyScope::Auth)?;
            if !input.is_empty() {
                return Err(Mqtt5ParseError::MalformedPacket(
                    "AUTH payload has trailing bytes",
                ));
            }
            (reason_code, properties)
        }
    };

    Ok((
        &[][..],
        Auth {
            reason_code,
            properties,
        },
    ))
}

pub fn encode(packet: &Auth, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    reason_code::validate_for_packet(packet.reason_code, ControlPacketType::Auth)?;
    let mut body = BytesMut::new();
    if packet.reason_code != ReasonCode::Success || packet.properties != Properties::default() {
        body.put_u8(reason_code::encode(packet.reason_code));
        if packet.properties != Properties::default() {
            properties::encode(&packet.properties, PropertyScope::Auth, &mut body)?;
        }
    }

    let fixed_header = FixedHeader::new(
        ControlPacketType::Auth,
        0,
        body.len()
            .try_into()
            .map_err(|_| Mqtt5ParseError::VariableByteIntegerTooLarge(body.len() as u32))?,
    )?;
    fixed_header.encode(buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

pub fn to_bytes(packet: &Auth) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_shortest_auth() {
        assert_eq!(
            parse(&[0xf0, 0x00], 1024).unwrap(),
            (
                &[][..],
                Packet::Auth(Auth {
                    reason_code: ReasonCode::Success,
                    properties: Properties::default(),
                })
            )
        );
    }

    #[test]
    fn parses_continue_authentication_fixture() {
        let (_, packet) = parse(&[0xf0, 0x02, 0x18, 0x00], 1024).expect("parse AUTH");

        assert_eq!(
            packet,
            Packet::Auth(Auth {
                reason_code: ReasonCode::ContinueAuthentication,
                properties: Properties::default(),
            })
        );
    }

    #[test]
    fn auth_round_trips_with_properties() {
        let packet = Auth {
            reason_code: ReasonCode::ContinueAuthentication,
            properties: Properties {
                authentication_method: Some("method".to_string()),
                authentication_data: Some(bytes::Bytes::from_static(b"data")),
                ..Properties::default()
            },
        };

        let encoded = to_bytes(&packet).expect("encode AUTH");
        let (_, decoded) = parse(&encoded, 1024).expect("parse AUTH");

        assert_eq!(decoded, Packet::Auth(packet));
    }

    #[test]
    fn rejects_reason_code_not_allowed_on_auth() {
        let err = parse(&[0xf0, 0x02, 0x87, 0x00], 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::ReasonCodeNotAllowed {
                reason_code: 0x87,
                packet_type: "AUTH",
            }
        );
    }
}
