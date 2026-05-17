use bytes::{BufMut, Bytes, BytesMut};

use crate::packet::{Connect, Packet, Properties, ProtocolVersion, Will};

use super::{
    common::{
        encode_binary, encode_utf8_string, parse_binary, parse_u16, parse_u8, parse_utf8_string,
        Mqtt5ParseError,
    },
    fixed_header::{self, ControlPacketType, FixedHeader},
    properties::{self, PropertyScope},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConnectFlags {
    username: bool,
    password: bool,
    will_retain: bool,
    will_qos: u8,
    will: bool,
    clean_start: bool,
}

impl ConnectFlags {
    fn parse(value: u8) -> Result<Self, Mqtt5ParseError> {
        if value & 0b0000_0001 != 0 {
            return Err(Mqtt5ParseError::MalformedPacket(
                "CONNECT reserved flag bit must be zero",
            ));
        }

        let username = value & 0b1000_0000 != 0;
        let password = value & 0b0100_0000 != 0;
        if password && !username {
            return Err(Mqtt5ParseError::MalformedPacket(
                "CONNECT password flag requires username flag",
            ));
        }

        let will_retain = value & 0b0010_0000 != 0;
        let will_qos = (value & 0b0001_1000) >> 3;
        let will = value & 0b0000_0100 != 0;
        if will_qos == 0b11 {
            return Err(Mqtt5ParseError::MalformedPacket(
                "CONNECT will QoS must not be 3",
            ));
        }
        if !will && (will_qos != 0 || will_retain) {
            return Err(Mqtt5ParseError::MalformedPacket(
                "CONNECT will QoS and retain require will flag",
            ));
        }

        Ok(Self {
            username,
            password,
            will_retain,
            will_qos,
            will,
            clean_start: value & 0b0000_0010 != 0,
        })
    }

    fn encode(self) -> Result<u8, Mqtt5ParseError> {
        if self.will_qos > 2 {
            return Err(Mqtt5ParseError::MalformedPacket(
                "CONNECT will QoS must not be greater than 2",
            ));
        }

        let mut value = 0u8;
        if self.username {
            value |= 0b1000_0000;
        }
        if self.password {
            value |= 0b0100_0000;
        }
        if self.will_retain {
            value |= 0b0010_0000;
        }
        value |= self.will_qos << 3;
        if self.will {
            value |= 0b0000_0100;
        }
        if self.clean_start {
            value |= 0b0000_0010;
        }
        ConnectFlags::parse(value)?;
        Ok(value)
    }
}

pub fn parse(input: &[u8], max_message_size: u32) -> Result<(&[u8], Packet), Mqtt5ParseError> {
    let (input, fixed_header) = fixed_header::parse(input, max_message_size)?;
    if fixed_header.packet_type != ControlPacketType::Connect {
        return Err(Mqtt5ParseError::MalformedPacket("expected CONNECT packet"));
    }

    let remaining_length = fixed_header.remaining_length as usize;
    if input.len() < remaining_length {
        return Err(Mqtt5ParseError::Incomplete("CONNECT body"));
    }
    let (body, input) = input.split_at(remaining_length);
    let (_, connect) = parse_body(body)?;

    Ok((input, Packet::Connect(connect)))
}

fn parse_body(input: &[u8]) -> Result<(&[u8], Connect), Mqtt5ParseError> {
    let (input, protocol_name) = parse_utf8_string(input)?;
    if protocol_name != "MQTT" {
        return Err(Mqtt5ParseError::MalformedPacket(
            "CONNECT protocol name must be MQTT",
        ));
    }

    let (input, protocol_level) = parse_u8(input, "CONNECT protocol level")?;
    if protocol_level != 5 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "CONNECT protocol level must be 5",
        ));
    }

    let (input, flags) = parse_u8(input, "CONNECT flags")?;
    let flags = ConnectFlags::parse(flags)?;
    let (input, keep_alive) = parse_u16(input, "CONNECT keep alive")?;
    let (input, properties) = properties::parse(input, PropertyScope::Connect)?;
    reject_enhanced_auth(&properties)?;

    let (input, client_id) = parse_utf8_string(input)?;
    let (input, will) = if flags.will {
        let (input, will_properties) = properties::parse(input, PropertyScope::Will)?;
        let (input, topic) = parse_utf8_string(input)?;
        let (input, payload) = parse_binary(input)?;
        (
            input,
            Some(Will {
                topic,
                payload,
                qos: flags.will_qos,
                retain: flags.will_retain,
                properties: will_properties,
            }),
        )
    } else {
        (input, None)
    };
    let (input, username) = if flags.username {
        let (input, username) = parse_utf8_string(input)?;
        (input, Some(username))
    } else {
        (input, None)
    };
    let (input, password) = if flags.password {
        let (input, password) = parse_binary(input)?;
        let password =
            String::from_utf8(password.to_vec()).map_err(|_| Mqtt5ParseError::InvalidUtf8)?;
        (input, Some(password))
    } else {
        (input, None)
    };

    if !input.is_empty() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "CONNECT payload has trailing bytes",
        ));
    }

    let session_expiry_interval = properties.session_expiry_interval;
    Ok((
        input,
        Connect {
            protocol_version: ProtocolVersion::V5_0,
            client_id,
            username,
            password,
            keep_alive,
            clean_start: flags.clean_start,
            session_expiry_interval,
            will,
            properties,
        },
    ))
}

pub fn encode(packet: &Connect, buffer: &mut BytesMut) -> Result<(), Mqtt5ParseError> {
    if packet.protocol_version != ProtocolVersion::V5_0 {
        return Err(Mqtt5ParseError::MalformedPacket(
            "CONNECT packet is not MQTT 5.0",
        ));
    }
    let properties = normalized_connect_properties(packet)?;
    reject_enhanced_auth(&properties)?;

    let mut body = BytesMut::new();
    encode_utf8_string("MQTT", &mut body)?;
    body.put_u8(0x05);
    let flags = ConnectFlags {
        username: packet.username.is_some(),
        password: packet.password.is_some(),
        will_retain: packet.will.as_ref().is_some_and(|will| will.retain),
        will_qos: packet.will.as_ref().map_or(0, |will| will.qos),
        will: packet.will.is_some(),
        clean_start: packet.clean_start,
    };
    body.put_u8(flags.encode()?);
    body.put_u16(packet.keep_alive);
    properties::encode(&properties, PropertyScope::Connect, &mut body)?;
    encode_utf8_string(&packet.client_id, &mut body)?;
    if let Some(will) = &packet.will {
        properties::encode(&will.properties, PropertyScope::Will, &mut body)?;
        encode_utf8_string(&will.topic, &mut body)?;
        encode_binary(&will.payload, &mut body)?;
    }
    if let Some(username) = &packet.username {
        encode_utf8_string(username, &mut body)?;
    }
    if let Some(password) = &packet.password {
        encode_binary(&Bytes::copy_from_slice(password.as_bytes()), &mut body)?;
    }

    let fixed_header = FixedHeader::new(
        ControlPacketType::Connect,
        0,
        body.len()
            .try_into()
            .map_err(|_| Mqtt5ParseError::VariableByteIntegerTooLarge(body.len() as u32))?,
    )?;
    fixed_header.encode(buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

pub fn to_bytes(packet: &Connect) -> Result<BytesMut, Mqtt5ParseError> {
    let mut buffer = BytesMut::new();
    encode(packet, &mut buffer)?;
    Ok(buffer)
}

fn reject_enhanced_auth(properties: &Properties) -> Result<(), Mqtt5ParseError> {
    if properties.authentication_method.is_some() || properties.authentication_data.is_some() {
        return Err(Mqtt5ParseError::MalformedPacket(
            "enhanced authentication is not supported",
        ));
    }
    Ok(())
}

fn normalized_connect_properties(packet: &Connect) -> Result<Properties, Mqtt5ParseError> {
    let mut properties = packet.properties.clone();
    match (
        packet.session_expiry_interval,
        properties.session_expiry_interval,
    ) {
        (Some(packet_value), Some(property_value)) if packet_value != property_value => {
            return Err(Mqtt5ParseError::MalformedPacket(
                "CONNECT session expiry interval disagrees with properties",
            ));
        }
        (Some(packet_value), None) => {
            properties.session_expiry_interval = Some(packet_value);
        }
        _ => {}
    }
    Ok(properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_connect_packet() {
        let input = [
            0x10, 0x19, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3c, 0x00, 0x00,
            0x0c, b'm', b'q', b't', b't', b'5', b'-', b'c', b'l', b'i', b'e', b'n', b't',
        ];

        let (remaining, packet) = parse(&input, 1024).expect("parse MQTT 5 CONNECT");

        assert!(remaining.is_empty());
        let Packet::Connect(connect) = packet else {
            panic!("expected CONNECT packet");
        };
        assert_eq!(connect.protocol_version, ProtocolVersion::V5_0);
        assert_eq!(connect.client_id, "mqtt5-client");
        assert!(connect.clean_start);
        assert_eq!(connect.keep_alive, 60);
        assert_eq!(connect.session_expiry_interval, None);
        assert!(connect.properties.user_properties.is_empty());
        assert!(connect.will.is_none());
    }

    #[test]
    fn parses_connect_with_properties_will_username_and_password() {
        let mut connect = Connect {
            protocol_version: ProtocolVersion::V5_0,
            client_id: "client-with-auth".to_string(),
            username: Some("user".to_string()),
            password: Some("password".to_string()),
            keep_alive: 30,
            clean_start: false,
            session_expiry_interval: Some(120),
            will: Some(Will {
                topic: "will/topic".to_string(),
                payload: Bytes::from_static(b"gone"),
                qos: 1,
                retain: true,
                properties: Properties {
                    content_type: Some("text/plain".to_string()),
                    ..Properties::default()
                },
            }),
            properties: Properties {
                session_expiry_interval: Some(120),
                user_properties: vec![("source".to_string(), "test".to_string())],
                ..Properties::default()
            },
        };

        let encoded = to_bytes(&connect).expect("encode CONNECT");
        let (_, packet) = parse(&encoded, 1024).expect("parse encoded CONNECT");
        let Packet::Connect(parsed) = packet else {
            panic!("expected CONNECT packet");
        };

        connect.session_expiry_interval = connect.properties.session_expiry_interval;
        assert_eq!(parsed, connect);
    }

    #[test]
    fn rejects_bad_connect_flags() {
        let input = [
            0x10, 0x0d, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x01, 0x00, 0x3c, 0x00, 0x00,
            0x00,
        ];

        let err = parse(&input, 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::MalformedPacket("CONNECT reserved flag bit must be zero")
        );
    }

    #[test]
    fn rejects_enhanced_auth_connect_properties() {
        let input = [
            0x10, 0x16, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3c, 0x09, 0x15,
            0x00, 0x06, b'm', b'e', b't', b'h', b'o', b'd', 0x00, 0x00,
        ];

        let err = parse(&input, 1024).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::MalformedPacket("enhanced authentication is not supported")
        );
    }

    #[test]
    fn encodes_session_expiry_from_explicit_connect_field() {
        let connect = Connect {
            protocol_version: ProtocolVersion::V5_0,
            client_id: "session-client".to_string(),
            username: None,
            password: None,
            keep_alive: 30,
            clean_start: true,
            session_expiry_interval: Some(45),
            will: None,
            properties: Properties::default(),
        };

        let encoded = to_bytes(&connect).expect("encode CONNECT");
        let (_, packet) = parse(&encoded, 1024).expect("parse encoded CONNECT");
        let Packet::Connect(parsed) = packet else {
            panic!("expected CONNECT packet");
        };

        assert_eq!(parsed.session_expiry_interval, Some(45));
        assert_eq!(parsed.properties.session_expiry_interval, Some(45));
    }

    #[test]
    fn rejects_invalid_will_qos_on_encode() {
        let connect = Connect {
            protocol_version: ProtocolVersion::V5_0,
            client_id: "invalid-will".to_string(),
            username: None,
            password: None,
            keep_alive: 30,
            clean_start: true,
            session_expiry_interval: None,
            will: Some(Will {
                topic: "will/topic".to_string(),
                payload: Bytes::from_static(b"gone"),
                qos: 3,
                retain: false,
                properties: Properties::default(),
            }),
            properties: Properties::default(),
        };

        let err = to_bytes(&connect).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::MalformedPacket("CONNECT will QoS must not be greater than 2")
        );
    }
}
