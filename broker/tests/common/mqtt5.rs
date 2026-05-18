#![allow(dead_code)]

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connack {
    pub session_present: bool,
    pub reason_code: u8,
    pub properties: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Puback {
    pub packet_id: u16,
    pub reason_code: u8,
    pub properties: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suback {
    pub packet_id: u16,
    pub properties: Vec<u8>,
    pub reason_codes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    pub topic: String,
    pub packet_id: Option<u16>,
    pub properties: Vec<u8>,
    pub payload: Vec<u8>,
}

pub fn connect_packet(client_id: &str, clean_start: bool, keep_alive_secs: u16) -> Vec<u8> {
    connect_packet_with_properties(client_id, clean_start, keep_alive_secs, &[])
}

pub fn connect_packet_with_properties(
    client_id: &str,
    clean_start: bool,
    keep_alive_secs: u16,
    properties: &[u8],
) -> Vec<u8> {
    let mut variable_header = Vec::new();
    variable_header.extend_from_slice(&utf8_string("MQTT"));
    variable_header.push(0x05);
    variable_header.push(if clean_start { 0x02 } else { 0x00 });
    variable_header.extend_from_slice(&keep_alive_secs.to_be_bytes());
    variable_header.extend_from_slice(&encode_variable_byte_integer(properties.len() as u32));
    variable_header.extend_from_slice(properties);

    let mut payload = Vec::new();
    payload.extend_from_slice(&utf8_string(client_id));

    control_packet(0x10, [variable_header, payload].concat())
}

pub fn subscribe_packet(packet_id: u16, topic: &str, qos: u8) -> Vec<u8> {
    subscribe_packet_with_options(packet_id, topic, qos & 0x03)
}

pub fn subscribe_packet_with_options(packet_id: u16, topic: &str, options: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0x00);
    body.extend_from_slice(&utf8_string(topic));
    body.push(options);

    control_packet(0x82, body)
}

pub fn publish_packet(
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
    packet_id: Option<u16>,
) -> Vec<u8> {
    publish_packet_with_properties(topic, payload, qos, retain, packet_id, &[])
}

pub fn publish_packet_with_properties(
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
    packet_id: Option<u16>,
    properties: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&utf8_string(topic));
    if qos > 0 {
        body.extend_from_slice(&packet_id.unwrap_or(1).to_be_bytes());
    }
    body.extend_from_slice(&encode_variable_byte_integer(properties.len() as u32));
    body.extend_from_slice(properties);
    body.extend_from_slice(payload);

    let flags = ((qos & 0x03) << 1) | u8::from(retain);
    control_packet(0x30 | flags, body)
}

pub fn disconnect_packet() -> Vec<u8> {
    vec![0xE0, 0x00]
}

pub fn session_expiry_interval_property(seconds: u32) -> Vec<u8> {
    let mut property = Vec::with_capacity(5);
    property.push(0x11);
    property.extend_from_slice(&seconds.to_be_bytes());
    property
}

pub fn pubrec_packet(packet_id: u16) -> Vec<u8> {
    ack_packet(0x50, packet_id)
}

pub fn puback_packet(packet_id: u16) -> Vec<u8> {
    ack_packet(0x40, packet_id)
}

pub fn pubrel_packet(packet_id: u16) -> Vec<u8> {
    ack_packet(0x62, packet_id)
}

pub fn pubcomp_packet(packet_id: u16) -> Vec<u8> {
    ack_packet(0x70, packet_id)
}

pub fn pingreq_packet() -> Vec<u8> {
    vec![0xC0, 0x00]
}

pub fn parse_connack(packet: &[u8]) -> Result<Connack, String> {
    let (packet_type, flags, body, consumed) = parse_control_packet(packet)?;
    if consumed != packet.len() {
        return Err("trailing bytes after CONNACK".to_string());
    }
    if packet_type != 0x02 || flags != 0 {
        return Err(format!(
            "expected CONNACK, got type={packet_type} flags={flags}"
        ));
    }
    if body.len() < 3 {
        return Err("CONNACK body is too short".to_string());
    }

    let session_present = body[0] & 0x01 == 0x01;
    let reason_code = body[1];
    let (property_len, property_len_bytes) = decode_variable_byte_integer(&body[2..])?;
    let property_start = 2 + property_len_bytes;
    let property_end = property_start + property_len as usize;
    if property_end != body.len() {
        return Err("CONNACK property length does not match body".to_string());
    }

    Ok(Connack {
        session_present,
        reason_code,
        properties: body[property_start..property_end].to_vec(),
    })
}

pub fn parse_puback(packet: &[u8]) -> Result<Puback, String> {
    parse_publish_ack(packet, 0x04, 0x00, "PUBACK")
}

pub fn parse_pubrec(packet: &[u8]) -> Result<Puback, String> {
    parse_publish_ack(packet, 0x05, 0x00, "PUBREC")
}

pub fn parse_pubrel(packet: &[u8]) -> Result<Puback, String> {
    parse_publish_ack(packet, 0x06, 0x02, "PUBREL")
}

pub fn parse_pubcomp(packet: &[u8]) -> Result<Puback, String> {
    parse_publish_ack(packet, 0x07, 0x00, "PUBCOMP")
}

fn parse_publish_ack(
    packet: &[u8],
    expected_packet_type: u8,
    expected_flags: u8,
    label: &str,
) -> Result<Puback, String> {
    let (packet_type, flags, body, consumed) = parse_control_packet(packet)?;
    if consumed != packet.len() {
        return Err(format!("trailing bytes after {label}"));
    }
    if packet_type != expected_packet_type || flags != expected_flags {
        return Err(format!(
            "expected {label}, got type={packet_type} flags={flags}"
        ));
    }
    if body.len() < 2 {
        return Err(format!("{label} body is too short"));
    }

    let packet_id = u16::from_be_bytes([body[0], body[1]]);
    if body.len() == 2 {
        return Ok(Puback {
            packet_id,
            reason_code: 0x00,
            properties: Vec::new(),
        });
    }

    let reason_code = body[2];
    let (property_len, property_len_bytes) = decode_variable_byte_integer(&body[3..])?;
    let property_start = 3 + property_len_bytes;
    let property_end = property_start + property_len as usize;
    if property_end != body.len() {
        return Err(format!("{label} property length does not match body"));
    }

    Ok(Puback {
        packet_id,
        reason_code,
        properties: body[property_start..property_end].to_vec(),
    })
}

pub fn parse_suback(packet: &[u8]) -> Result<Suback, String> {
    let (packet_type, flags, body, consumed) = parse_control_packet(packet)?;
    if consumed != packet.len() {
        return Err("trailing bytes after SUBACK".to_string());
    }
    if packet_type != 0x09 || flags != 0 {
        return Err(format!(
            "expected SUBACK, got type={packet_type} flags={flags}"
        ));
    }
    if body.len() < 4 {
        return Err("SUBACK body is too short".to_string());
    }

    let packet_id = u16::from_be_bytes([body[0], body[1]]);
    let (property_len, property_len_bytes) = decode_variable_byte_integer(&body[2..])?;
    let property_start = 2 + property_len_bytes;
    let reason_start = property_start + property_len as usize;
    if reason_start > body.len() {
        return Err("SUBACK property length exceeds body".to_string());
    }
    if reason_start == body.len() {
        return Err("SUBACK has no reason codes".to_string());
    }

    Ok(Suback {
        packet_id,
        properties: body[property_start..reason_start].to_vec(),
        reason_codes: body[reason_start..].to_vec(),
    })
}

pub fn parse_publish(packet: &[u8]) -> Result<Publish, String> {
    let (packet_type, flags, body, consumed) = parse_control_packet(packet)?;
    if consumed != packet.len() {
        return Err("trailing bytes after PUBLISH".to_string());
    }
    if packet_type != 0x03 {
        return Err(format!("expected PUBLISH, got type={packet_type}"));
    }

    let dup = flags & 0b1000 != 0;
    let qos = (flags & 0b0110) >> 1;
    let retain = flags & 0b0001 != 0;
    if qos == 3 {
        return Err("invalid PUBLISH QoS 3".to_string());
    }

    let (topic, mut offset) = parse_utf8_string(body)?;
    let packet_id = if qos > 0 {
        if body.len() < offset + 2 {
            return Err("PUBLISH packet id is missing".to_string());
        }
        let packet_id = u16::from_be_bytes([body[offset], body[offset + 1]]);
        offset += 2;
        Some(packet_id)
    } else {
        None
    };

    let (property_len, property_len_bytes) = decode_variable_byte_integer(&body[offset..])?;
    offset += property_len_bytes;
    let property_end = offset + property_len as usize;
    if property_end > body.len() {
        return Err("PUBLISH property length exceeds body".to_string());
    }
    let properties = body[offset..property_end].to_vec();
    let payload = body[property_end..].to_vec();

    Ok(Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        properties,
        payload,
    })
}

fn control_packet(first_byte: u8, body: Vec<u8>) -> Vec<u8> {
    let mut packet = Vec::with_capacity(1 + 4 + body.len());
    packet.push(first_byte);
    packet.extend_from_slice(&encode_variable_byte_integer(body.len() as u32));
    packet.extend_from_slice(&body);
    packet
}

fn ack_packet(first_byte: u8, packet_id: u16) -> Vec<u8> {
    control_packet(first_byte, packet_id.to_be_bytes().to_vec())
}

fn utf8_string(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).expect("MQTT UTF-8 string length fits into u16");
    let mut out = Vec::with_capacity(2 + bytes.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

fn parse_utf8_string(input: &[u8]) -> Result<(String, usize), String> {
    if input.len() < 2 {
        return Err("UTF-8 string length is missing".to_string());
    }
    let len = u16::from_be_bytes([input[0], input[1]]) as usize;
    if input.len() < 2 + len {
        return Err("UTF-8 string bytes are incomplete".to_string());
    }
    let value = std::str::from_utf8(&input[2..2 + len])
        .map_err(|err| format!("invalid UTF-8 string: {err}"))?
        .to_string();
    Ok((value, 2 + len))
}

fn parse_control_packet(input: &[u8]) -> Result<(u8, u8, &[u8], usize), String> {
    if input.is_empty() {
        return Err("empty MQTT packet".to_string());
    }
    let packet_type = input[0] >> 4;
    let flags = input[0] & 0x0F;
    let (remaining_len, len_bytes) = decode_variable_byte_integer(&input[1..])?;
    let body_start = 1 + len_bytes;
    let body_end = body_start + remaining_len as usize;
    if input.len() < body_end {
        return Err("MQTT packet body is incomplete".to_string());
    }
    Ok((packet_type, flags, &input[body_start..body_end], body_end))
}

fn encode_variable_byte_integer(mut value: u32) -> Vec<u8> {
    assert!(
        value <= 268_435_455,
        "MQTT variable byte integer is too large"
    );
    let mut out = Vec::new();
    loop {
        let mut encoded = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            encoded |= 128;
        }
        out.push(encoded);
        if value == 0 {
            return out;
        }
    }
}

fn decode_variable_byte_integer(input: &[u8]) -> Result<(u32, usize), String> {
    let mut multiplier = 1u32;
    let mut value = 0u32;

    for (idx, byte) in input.iter().copied().enumerate() {
        value += ((byte & 0x7F) as u32) * multiplier;
        if byte & 0x80 == 0 {
            return Ok((value, idx + 1));
        }
        multiplier = multiplier
            .checked_mul(128)
            .ok_or_else(|| "malformed variable byte integer".to_string())?;
        if idx == 3 {
            return Err("malformed variable byte integer".to_string());
        }
    }

    Err("incomplete variable byte integer".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_minimal_connect_packet() {
        let packet = connect_packet("mqtt5-client", true, 60);
        assert_eq!(
            packet,
            vec![
                0x10, 0x19, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
                0x0C, b'm', b'q', b't', b't', b'5', b'-', b'c', b'l', b'i', b'e', b'n', b't',
            ]
        );
    }

    #[test]
    fn encodes_subscribe_packet() {
        let packet = subscribe_packet(7, "test/mqtt5/basic", 1);
        assert_eq!(
            packet,
            vec![
                0x82, 0x16, 0x00, 0x07, 0x00, 0x00, 0x10, b't', b'e', b's', b't', b'/', b'm', b'q',
                b't', b't', b'5', b'/', b'b', b'a', b's', b'i', b'c', 0x01,
            ]
        );
    }

    #[test]
    fn encodes_and_parses_publish_packet() {
        let packet = publish_packet("test/mqtt5/basic", b"hello", 1, false, Some(9));
        let publish = parse_publish(&packet).expect("parse publish");
        assert!(!publish.dup);
        assert_eq!(publish.qos, 1);
        assert!(!publish.retain);
        assert_eq!(publish.topic, "test/mqtt5/basic");
        assert_eq!(publish.packet_id, Some(9));
        assert!(publish.properties.is_empty());
        assert_eq!(publish.payload, b"hello");
    }

    #[test]
    fn parses_connack_success_packet() {
        let connack = parse_connack(&[0x20, 0x03, 0x00, 0x00, 0x00]).expect("parse connack");
        assert!(!connack.session_present);
        assert_eq!(connack.reason_code, 0x00);
        assert!(connack.properties.is_empty());
    }

    #[test]
    fn parses_short_success_puback_packet() {
        let puback = parse_puback(&[0x40, 0x02, 0x00, 0x2A]).expect("parse puback");
        assert_eq!(puback.packet_id, 42);
        assert_eq!(puback.reason_code, 0x00);
        assert!(puback.properties.is_empty());
    }

    #[test]
    fn builds_and_parses_qos2_ack_packets() {
        assert_eq!(pubrec_packet(42), vec![0x50, 0x02, 0x00, 0x2A]);
        assert_eq!(pubrel_packet(42), vec![0x62, 0x02, 0x00, 0x2A]);
        assert_eq!(pubcomp_packet(42), vec![0x70, 0x02, 0x00, 0x2A]);

        let pubrec = parse_pubrec(&[0x50, 0x02, 0x00, 0x2A]).expect("parse pubrec");
        assert_eq!(pubrec.packet_id, 42);
        assert_eq!(pubrec.reason_code, 0x00);

        let pubrel = parse_pubrel(&[0x62, 0x02, 0x00, 0x2A]).expect("parse pubrel");
        assert_eq!(pubrel.packet_id, 42);
        assert_eq!(pubrel.reason_code, 0x00);

        let pubcomp = parse_pubcomp(&[0x70, 0x02, 0x00, 0x2A]).expect("parse pubcomp");
        assert_eq!(pubcomp.packet_id, 42);
        assert_eq!(pubcomp.reason_code, 0x00);
    }

    #[test]
    fn parses_suback_packet() {
        let suback = parse_suback(&[0x90, 0x04, 0x00, 0x2A, 0x00, 0x01]).expect("parse suback");
        assert_eq!(suback.packet_id, 42);
        assert!(suback.properties.is_empty());
        assert_eq!(suback.reason_codes, vec![0x01]);
    }

    #[test]
    fn rejects_malformed_remaining_length() {
        let err = parse_connack(&[0x20, 0x80, 0x80, 0x80, 0x80, 0x00]).unwrap_err();
        assert!(err.contains("malformed variable byte integer"));
    }
}
