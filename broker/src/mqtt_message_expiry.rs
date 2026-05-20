use std::time::{SystemTime, UNIX_EPOCH};

use yedmq_mqtt::packet::{Packet, ProtocolVersion, Publish};

pub(crate) fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before unix epoch")
        .as_secs()
}

pub(crate) fn stamp_publish_expiry(publish: &mut Publish, now: u64) {
    if publish.expires_at_unix_secs.is_none() {
        if let Some(interval) = publish.properties.message_expiry_interval {
            publish.expires_at_unix_secs = Some(now.saturating_add(interval as u64));
        }
    }
}

pub(crate) fn is_publish_expired(publish: &Publish, now: u64) -> bool {
    publish
        .expires_at_unix_secs
        .map(|expires_at| now >= expires_at)
        .unwrap_or(false)
}

pub(crate) fn is_packet_expired(packet: &Packet, now: u64) -> bool {
    match packet {
        Packet::Publish(publish) => is_publish_expired(publish, now),
        _ => false,
    }
}

pub(crate) fn prepare_publish_for_delivery(
    publish: &mut Publish,
    protocol_version: ProtocolVersion,
    now: u64,
) -> bool {
    let Some(expires_at) = publish.expires_at_unix_secs else {
        return true;
    };

    if now >= expires_at {
        return false;
    }

    if protocol_version == ProtocolVersion::V5_0 {
        let remaining = expires_at.saturating_sub(now).min(u32::MAX as u64) as u32;
        publish.properties.message_expiry_interval = Some(remaining);
    }

    true
}

pub(crate) fn prepare_packet_for_delivery(
    packet: &mut Packet,
    protocol_version: ProtocolVersion,
    now: u64,
) -> bool {
    match packet {
        Packet::Publish(publish) => prepare_publish_for_delivery(publish, protocol_version, now),
        _ => true,
    }
}

pub(crate) fn route_expiry_at(packet: &Packet, route_ttl_seconds: u64, now: u64) -> u64 {
    let route_expiry = now.saturating_add(route_ttl_seconds);
    match packet {
        Packet::Publish(publish) => publish
            .expires_at_unix_secs
            .map(|expires_at| expires_at.min(route_expiry))
            .unwrap_or(route_expiry),
        _ => route_expiry,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use yedmq_mqtt::packet::{Properties, ProtocolVersion, Publish};

    use super::*;

    fn publish_with_expiry(interval: u32) -> Publish {
        Publish {
            protocol_version: ProtocolVersion::V5_0,
            topic_name: "test/expiry".to_string(),
            payload: Bytes::from_static(b"payload"),
            qos: 1,
            retain: false,
            dup: false,
            packet_identifier: Some(7),
            properties: Properties {
                message_expiry_interval: Some(interval),
                ..Properties::default()
            },
            expires_at_unix_secs: None,
        }
    }

    #[test]
    fn stamp_publish_expiry_uses_absolute_deadline() {
        let mut publish = publish_with_expiry(10);

        stamp_publish_expiry(&mut publish, 100);

        assert_eq!(publish.expires_at_unix_secs, Some(110));
    }

    #[test]
    fn prepare_delivery_updates_remaining_interval_for_mqtt5() {
        let mut publish = publish_with_expiry(60);
        publish.expires_at_unix_secs = Some(160);

        assert!(prepare_publish_for_delivery(
            &mut publish,
            ProtocolVersion::V5_0,
            100
        ));

        assert_eq!(publish.properties.message_expiry_interval, Some(60));
    }

    #[test]
    fn prepare_delivery_rejects_expired_publish() {
        let mut publish = publish_with_expiry(1);
        publish.expires_at_unix_secs = Some(100);

        assert!(!prepare_publish_for_delivery(
            &mut publish,
            ProtocolVersion::V5_0,
            100
        ));
    }

    #[test]
    fn route_expiry_uses_earlier_message_deadline() {
        let mut publish = publish_with_expiry(30);
        publish.expires_at_unix_secs = Some(120);

        assert_eq!(route_expiry_at(&Packet::Publish(publish), 3600, 100), 120);
    }
}
