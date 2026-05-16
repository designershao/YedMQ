use std::{error::Error, fmt};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    v3::{
        connack::ConnackReturnCode,
        connect::ConnectPacketBuilder,
        disconnect::DisconnectPacket,
        fixed_header::FixHeader,
        pingreq::PingreqPacketBuilder,
        pingresp::PingrespPacket,
        puback::PubAckPacket,
        pubcomp::PubCompPacket,
        publish::PublishPacketBuilder,
        pubrec::PubRecPacket,
        pubrel::PubRelPacket,
        suback::{ReturnCode, SubackPacket},
        subscribe::{SubscribePacketBuilder, TopicFilter as SubscribeTopicFilter},
        unsuback::UnSubackPacket,
        unsubscribe::{TopicFilter as UnsubscribeTopicFilter, UnsubscribePacketBuilder},
    },
    MqttPacketV3, PacketType,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolVersion {
    V3_1_1,
    V5_0,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Packet {
    Connect(Connect),
    Connack(Connack),
    Publish(Publish),
    Puback(Ack),
    Pubrec(Ack),
    Pubrel(Ack),
    Pubcomp(Ack),
    Subscribe(Subscribe),
    Suback(Suback),
    Unsubscribe(Unsubscribe),
    Unsuback(Unsuback),
    Pingreq,
    Pingresp,
    Disconnect(Disconnect),
    Auth(Auth),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Connect {
    pub protocol_version: ProtocolVersion,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: u16,
    pub clean_start: bool,
    pub session_expiry_interval: Option<u32>,
    pub will: Option<Will>,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Will {
    pub topic: String,
    pub payload: Bytes,
    pub qos: u8,
    pub retain: bool,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Connack {
    pub protocol_version: ProtocolVersion,
    pub session_present: bool,
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Publish {
    pub protocol_version: ProtocolVersion,
    pub topic_name: String,
    pub payload: Bytes,
    pub qos: u8,
    pub retain: bool,
    pub dup: bool,
    pub packet_identifier: Option<u16>,
    pub properties: Properties,
    pub expires_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub protocol_version: ProtocolVersion,
    pub packet_identifier: u16,
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscribe {
    pub protocol_version: ProtocolVersion,
    pub packet_identifier: u16,
    pub topics: Vec<SubscribeTopic>,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeTopic {
    pub topic_filter: String,
    pub qos: u8,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetainHandling {
    SendAtSubscribe,
    SendAtSubscribeIfNew,
    DoNotSend,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Suback {
    pub protocol_version: ProtocolVersion,
    pub packet_identifier: u16,
    pub reason_codes: Vec<ReasonCode>,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unsubscribe {
    pub protocol_version: ProtocolVersion,
    pub packet_identifier: u16,
    pub topics: Vec<String>,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unsuback {
    pub protocol_version: ProtocolVersion,
    pub packet_identifier: u16,
    pub reason_codes: Vec<ReasonCode>,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Disconnect {
    pub protocol_version: ProtocolVersion,
    pub reason_code: ReasonCode,
    pub session_expiry_interval: Option<u32>,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Auth {
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Properties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Bytes>,
    pub subscription_identifiers: Vec<u32>,
    pub session_expiry_interval: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub server_keep_alive: Option<u16>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Bytes>,
    pub request_problem_information: Option<bool>,
    pub will_delay_interval: Option<u32>,
    pub request_response_information: Option<bool>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub reason_string: Option<String>,
    pub receive_maximum: Option<u16>,
    pub topic_alias_maximum: Option<u16>,
    pub topic_alias: Option<u16>,
    pub maximum_qos: Option<u8>,
    pub retain_available: Option<bool>,
    pub user_properties: Vec<(String, String)>,
    pub maximum_packet_size: Option<u32>,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifier_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReasonCode {
    Success,
    GrantedQos0,
    GrantedQos1,
    GrantedQos2,
    NormalDisconnection,
    NoMatchingSubscribers,
    UnspecifiedError,
    MalformedPacket,
    ProtocolError,
    ImplementationSpecificError,
    UnsupportedProtocolVersion,
    ClientIdentifierNotValid,
    BadUserNameOrPassword,
    NotAuthorized,
    ServerUnavailable,
    TopicFilterInvalid,
    TopicNameInvalid,
    PacketIdentifierInUse,
    PacketTooLarge,
    QuotaExceeded,
    PayloadFormatInvalid,
    RetainNotSupported,
    QosNotSupported,
    BadAuthenticationMethod,
    SharedSubscriptionsNotSupported,
    SubscriptionIdentifiersNotSupported,
    WildcardSubscriptionsNotSupported,
}

impl ReasonCode {
    pub fn as_u8(self) -> u8 {
        match self {
            ReasonCode::Success | ReasonCode::GrantedQos0 | ReasonCode::NormalDisconnection => 0x00,
            ReasonCode::GrantedQos1 => 0x01,
            ReasonCode::GrantedQos2 => 0x02,
            ReasonCode::NoMatchingSubscribers => 0x10,
            ReasonCode::UnspecifiedError => 0x80,
            ReasonCode::MalformedPacket => 0x81,
            ReasonCode::ProtocolError => 0x82,
            ReasonCode::ImplementationSpecificError => 0x83,
            ReasonCode::UnsupportedProtocolVersion => 0x84,
            ReasonCode::ClientIdentifierNotValid => 0x85,
            ReasonCode::BadUserNameOrPassword => 0x86,
            ReasonCode::NotAuthorized => 0x87,
            ReasonCode::ServerUnavailable => 0x88,
            ReasonCode::TopicFilterInvalid => 0x8F,
            ReasonCode::TopicNameInvalid => 0x90,
            ReasonCode::PacketIdentifierInUse => 0x91,
            ReasonCode::PacketTooLarge => 0x95,
            ReasonCode::QuotaExceeded => 0x97,
            ReasonCode::PayloadFormatInvalid => 0x99,
            ReasonCode::RetainNotSupported => 0x9A,
            ReasonCode::QosNotSupported => 0x9B,
            ReasonCode::BadAuthenticationMethod => 0x8C,
            ReasonCode::SharedSubscriptionsNotSupported => 0x9E,
            ReasonCode::SubscriptionIdentifiersNotSupported => 0xA1,
            ReasonCode::WildcardSubscriptionsNotSupported => 0xA2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PacketConversionError {
    UnsupportedPacketForMqtt3(&'static str),
    InvalidQos(u8),
}

impl fmt::Display for PacketConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacketConversionError::UnsupportedPacketForMqtt3(packet) => {
                write!(f, "{packet} cannot be encoded as MQTT v3.1.1")
            }
            PacketConversionError::InvalidQos(qos) => write!(f, "invalid QoS {qos}"),
        }
    }
}

impl Error for PacketConversionError {}

impl Packet {
    pub fn protocol_version(&self) -> Option<ProtocolVersion> {
        match self {
            Packet::Connect(packet) => Some(packet.protocol_version),
            Packet::Connack(packet) => Some(packet.protocol_version),
            Packet::Publish(packet) => Some(packet.protocol_version),
            Packet::Puback(packet)
            | Packet::Pubrec(packet)
            | Packet::Pubrel(packet)
            | Packet::Pubcomp(packet) => Some(packet.protocol_version),
            Packet::Subscribe(packet) => Some(packet.protocol_version),
            Packet::Suback(packet) => Some(packet.protocol_version),
            Packet::Unsubscribe(packet) => Some(packet.protocol_version),
            Packet::Unsuback(packet) => Some(packet.protocol_version),
            Packet::Disconnect(packet) => Some(packet.protocol_version),
            Packet::Pingreq | Packet::Pingresp | Packet::Auth(_) => None,
        }
    }
}

impl From<MqttPacketV3> for Packet {
    fn from(packet: MqttPacketV3) -> Self {
        match packet {
            MqttPacketV3::Connect(packet) => Packet::Connect(Connect {
                protocol_version: ProtocolVersion::V3_1_1,
                client_id: packet.payload.client_identifier,
                username: packet.payload.username,
                password: packet.payload.password,
                keep_alive: packet.variable_header.keep_alive,
                clean_start: packet.variable_header.clean_session,
                session_expiry_interval: None,
                will: packet.payload.will_topic.map(|topic| Will {
                    topic,
                    payload: Bytes::from(
                        packet.payload.will_message.unwrap_or_default().into_bytes(),
                    ),
                    qos: packet.variable_header.will_qos,
                    retain: packet.variable_header.will_retain,
                    properties: Properties::default(),
                }),
                properties: Properties::default(),
            }),
            MqttPacketV3::Connack(packet) => Packet::Connack(Connack {
                protocol_version: ProtocolVersion::V3_1_1,
                session_present: packet.variable_header.session_present,
                reason_code: v3_connack_return_code_to_reason(
                    packet.variable_header.connect_return_code,
                ),
                properties: Properties::default(),
            }),
            MqttPacketV3::Publish(packet) => Packet::Publish(Publish {
                protocol_version: ProtocolVersion::V3_1_1,
                topic_name: packet.variable_header.topic_name,
                payload: packet.payload.payload,
                qos: packet.fix_header.qos.unwrap_or_default() as u8,
                retain: packet.fix_header.retain.unwrap_or_default(),
                dup: packet.fix_header.dup.unwrap_or_default() == 1,
                packet_identifier: packet.variable_header.packet_identifier,
                properties: Properties::default(),
                expires_at_unix_secs: None,
            }),
            MqttPacketV3::Puback(packet) => {
                Packet::Puback(v3_ack(packet.variable_header.packet_identifier))
            }
            MqttPacketV3::Pubrec(packet) => {
                Packet::Pubrec(v3_ack(packet.variable_header.packet_identifier))
            }
            MqttPacketV3::Pubrel(packet) => {
                Packet::Pubrel(v3_ack(packet.variable_header.packet_identifier))
            }
            MqttPacketV3::Pubcomp(packet) => {
                Packet::Pubcomp(v3_ack(packet.variable_header.packet_identifier))
            }
            MqttPacketV3::Subscribe(packet) => Packet::Subscribe(Subscribe {
                protocol_version: ProtocolVersion::V3_1_1,
                packet_identifier: packet.variable_header.packet_identifier,
                topics: packet
                    .payload
                    .topic_filters
                    .into_iter()
                    .map(|topic| SubscribeTopic {
                        topic_filter: topic.topic_name,
                        qos: topic.qos,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: RetainHandling::SendAtSubscribe,
                    })
                    .collect(),
                properties: Properties::default(),
            }),
            MqttPacketV3::Suback(packet) => Packet::Suback(Suback {
                protocol_version: ProtocolVersion::V3_1_1,
                packet_identifier: packet.variable_header.packet_identifier,
                reason_codes: packet
                    .payload
                    .return_code
                    .into_iter()
                    .map(v3_suback_return_code_to_reason)
                    .collect(),
                properties: Properties::default(),
            }),
            MqttPacketV3::Unsubscribe(packet) => Packet::Unsubscribe(Unsubscribe {
                protocol_version: ProtocolVersion::V3_1_1,
                packet_identifier: packet.variable_header.packet_identifier,
                topics: packet
                    .payload
                    .topic_filters
                    .into_iter()
                    .map(|topic| topic.topic_name)
                    .collect(),
                properties: Properties::default(),
            }),
            MqttPacketV3::Unsuback(packet) => Packet::Unsuback(Unsuback {
                protocol_version: ProtocolVersion::V3_1_1,
                packet_identifier: packet.variable_header.packet_identifier,
                reason_codes: vec![ReasonCode::Success],
                properties: Properties::default(),
            }),
            MqttPacketV3::Pingreq(_) => Packet::Pingreq,
            MqttPacketV3::Pingresp(_) => Packet::Pingresp,
            MqttPacketV3::Disconnect(_) => Packet::Disconnect(Disconnect {
                protocol_version: ProtocolVersion::V3_1_1,
                reason_code: ReasonCode::NormalDisconnection,
                session_expiry_interval: None,
                properties: Properties::default(),
            }),
        }
    }
}

impl TryFrom<Packet> for MqttPacketV3 {
    type Error = PacketConversionError;

    fn try_from(packet: Packet) -> Result<Self, Self::Error> {
        match packet {
            Packet::Connect(packet) => {
                let mut builder = ConnectPacketBuilder::new(packet.client_id)
                    .keep_alive(packet.keep_alive)
                    .clean_session(packet.clean_start);
                if let Some(will) = packet.will {
                    builder = builder.will_msg(
                        will.topic,
                        String::from_utf8_lossy(&will.payload).into_owned(),
                        will.qos,
                        will.retain,
                    );
                }
                if let Some(username) = packet.username {
                    builder = builder.username(username);
                }
                if let Some(password) = packet.password {
                    builder = builder.password(password);
                }
                Ok(MqttPacketV3::Connect(builder.build()))
            }
            Packet::Connack(packet) => {
                let builder = crate::v3::connack::ConnAckPacketBuilder::new()
                    .set_session_present(packet.session_present)
                    .set_return_code(reason_to_v3_connack_return_code(packet.reason_code));
                Ok(MqttPacketV3::Connack(builder.build()))
            }
            Packet::Publish(packet) => {
                validate_qos(packet.qos)?;
                let mut builder = PublishPacketBuilder::new(packet.topic_name, packet.payload)
                    .qos(packet.qos)
                    .retain(packet.retain)
                    .dup(packet.dup);
                if let Some(packet_identifier) = packet.packet_identifier {
                    builder = builder.packet_identifier(packet_identifier);
                }
                Ok(MqttPacketV3::Publish(builder.build()))
            }
            Packet::Puback(packet) => Ok(MqttPacketV3::Puback(PubAckPacket::new(
                packet.packet_identifier,
            ))),
            Packet::Pubrec(packet) => Ok(MqttPacketV3::Pubrec(PubRecPacket::new(
                packet.packet_identifier,
            ))),
            Packet::Pubrel(packet) => Ok(MqttPacketV3::Pubrel(PubRelPacket::new(
                packet.packet_identifier,
            ))),
            Packet::Pubcomp(packet) => Ok(MqttPacketV3::Pubcomp(PubCompPacket::new(
                packet.packet_identifier,
            ))),
            Packet::Subscribe(packet) => {
                let mut builder = SubscribePacketBuilder::new(packet.packet_identifier);
                for topic in packet.topics {
                    validate_qos(topic.qos)?;
                    builder = builder.add_topic_filter(SubscribeTopicFilter {
                        topic_name: topic.topic_filter,
                        qos: topic.qos,
                    });
                }
                Ok(MqttPacketV3::Subscribe(builder.build()))
            }
            Packet::Suback(packet) => Ok(MqttPacketV3::Suback(SubackPacket::new(
                packet.packet_identifier,
                packet
                    .reason_codes
                    .into_iter()
                    .map(reason_to_v3_suback_return_code)
                    .collect(),
            ))),
            Packet::Unsubscribe(packet) => {
                let mut builder = UnsubscribePacketBuilder::new(packet.packet_identifier);
                for topic in packet.topics {
                    builder =
                        builder.add_topic_filter(UnsubscribeTopicFilter { topic_name: topic });
                }
                Ok(MqttPacketV3::Unsubscribe(builder.build()))
            }
            Packet::Unsuback(packet) => Ok(MqttPacketV3::Unsuback(UnSubackPacket::new(
                packet.packet_identifier,
            ))),
            Packet::Pingreq => Ok(MqttPacketV3::Pingreq(PingreqPacketBuilder::new().build())),
            Packet::Pingresp => Ok(MqttPacketV3::Pingresp(PingrespPacket::new())),
            Packet::Disconnect(_) => Ok(MqttPacketV3::Disconnect(DisconnectPacket {
                fix_header: FixHeader {
                    packet_type: PacketType::DISCONNECT,
                    qos: None,
                    retain: None,
                    dup: None,
                    remaining_length: 0,
                },
            })),
            Packet::Auth(_) => Err(PacketConversionError::UnsupportedPacketForMqtt3("AUTH")),
        }
    }
}

fn v3_ack(packet_identifier: u16) -> Ack {
    Ack {
        protocol_version: ProtocolVersion::V3_1_1,
        packet_identifier,
        reason_code: ReasonCode::Success,
        properties: Properties::default(),
    }
}

fn validate_qos(qos: u8) -> Result<(), PacketConversionError> {
    if qos <= 2 {
        Ok(())
    } else {
        Err(PacketConversionError::InvalidQos(qos))
    }
}

fn v3_connack_return_code_to_reason(return_code: u8) -> ReasonCode {
    match return_code {
        0x00 => ReasonCode::Success,
        0x01 => ReasonCode::UnsupportedProtocolVersion,
        0x02 => ReasonCode::ClientIdentifierNotValid,
        0x04 => ReasonCode::BadUserNameOrPassword,
        0x05 => ReasonCode::NotAuthorized,
        _ => ReasonCode::ServerUnavailable,
    }
}

fn reason_to_v3_connack_return_code(reason_code: ReasonCode) -> ConnackReturnCode {
    match reason_code {
        ReasonCode::Success | ReasonCode::GrantedQos0 | ReasonCode::NormalDisconnection => {
            ConnackReturnCode::Accept
        }
        ReasonCode::UnsupportedProtocolVersion => ConnackReturnCode::UnsupportedProtocolVersion,
        ReasonCode::ClientIdentifierNotValid => ConnackReturnCode::InvalidClientIdentifier,
        ReasonCode::BadUserNameOrPassword => ConnackReturnCode::InvalidUsernameOrPassword,
        ReasonCode::NotAuthorized => ConnackReturnCode::UnAuthorized,
        _ => ConnackReturnCode::ServerUnavailable,
    }
}

fn v3_suback_return_code_to_reason(return_code: ReturnCode) -> ReasonCode {
    match return_code {
        ReturnCode::MaxQos0 => ReasonCode::GrantedQos0,
        ReturnCode::MaxQos1 => ReasonCode::GrantedQos1,
        ReturnCode::MaxQos2 => ReasonCode::GrantedQos2,
        ReturnCode::Failure | ReturnCode::Invalid => ReasonCode::UnspecifiedError,
    }
}

fn reason_to_v3_suback_return_code(reason_code: ReasonCode) -> ReturnCode {
    match reason_code {
        ReasonCode::Success | ReasonCode::GrantedQos0 => ReturnCode::MaxQos0,
        ReasonCode::GrantedQos1 => ReturnCode::MaxQos1,
        ReasonCode::GrantedQos2 => ReturnCode::MaxQos2,
        _ => ReturnCode::Failure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{v3, MqttPacketV3};

    #[test]
    fn v3_connect_round_trips_through_neutral_packet() {
        let original = MqttPacketV3::Connect(
            v3::connect::ConnectPacketBuilder::new("client-a".to_string())
                .clean_session(true)
                .keep_alive(30)
                .username("user".to_string())
                .password("password".to_string())
                .build(),
        );
        let original_bytes = original.to_bytes();

        let neutral = Packet::from(original);
        let rebuilt = MqttPacketV3::try_from(neutral).expect("rebuild v3 connect");

        assert_eq!(rebuilt.to_bytes(), original_bytes);
    }

    #[test]
    fn v3_publish_round_trips_through_neutral_packet() {
        let original = MqttPacketV3::Publish(
            v3::publish::PublishPacketBuilder::new(
                "test/topic".to_string(),
                Bytes::from_static(b"payload"),
            )
            .qos(1)
            .retain(true)
            .dup(true)
            .packet_identifier(42)
            .build(),
        );
        let original_bytes = original.to_bytes();

        let neutral = Packet::from(original);
        let rebuilt = MqttPacketV3::try_from(neutral).expect("rebuild v3 publish");

        assert_eq!(rebuilt.to_bytes(), original_bytes);
    }

    #[test]
    fn v3_subscribe_round_trips_through_neutral_packet() {
        let original = MqttPacketV3::Subscribe(
            v3::subscribe::SubscribePacketBuilder::new(7)
                .add_topic_filter(v3::subscribe::TopicFilter {
                    topic_name: "test/+/value".to_string(),
                    qos: 1,
                })
                .build(),
        );
        let original_bytes = original.to_bytes();

        let neutral = Packet::from(original);
        let rebuilt = MqttPacketV3::try_from(neutral).expect("rebuild v3 subscribe");

        assert_eq!(rebuilt.to_bytes(), original_bytes);
    }

    #[test]
    fn v3_unsubscribe_round_trips_through_neutral_packet() {
        let original = MqttPacketV3::Unsubscribe(
            v3::unsubscribe::UnsubscribePacketBuilder::new(9)
                .add_topic_filter(v3::unsubscribe::TopicFilter {
                    topic_name: "test/topic".to_string(),
                })
                .build(),
        );
        let original_bytes = original.to_bytes();

        let neutral = Packet::from(original);
        let rebuilt = MqttPacketV3::try_from(neutral).expect("rebuild v3 unsubscribe");

        assert_eq!(rebuilt.to_bytes(), original_bytes);
    }

    #[test]
    fn v3_puback_flow_packets_round_trip_through_neutral_packet() {
        let originals = vec![
            MqttPacketV3::Puback(v3::puback::PubAckPacket::new(10)),
            MqttPacketV3::Pubrec(v3::pubrec::PubRecPacket::new(11)),
            MqttPacketV3::Pubrel(v3::pubrel::PubRelPacket::new(12)),
            MqttPacketV3::Pubcomp(v3::pubcomp::PubCompPacket::new(13)),
        ];

        for original in originals {
            let original_bytes = original.to_bytes();
            let neutral = Packet::from(original);
            let rebuilt = MqttPacketV3::try_from(neutral).expect("rebuild v3 publish ack flow");

            assert_eq!(rebuilt.to_bytes(), original_bytes);
        }
    }

    #[test]
    fn v3_suback_and_unsuback_round_trip_through_neutral_packet() {
        let originals = vec![
            MqttPacketV3::Suback(v3::suback::SubackPacket::new(
                7,
                vec![v3::suback::ReturnCode::MaxQos1],
            )),
            MqttPacketV3::Unsuback(v3::unsuback::UnSubackPacket::new(8)),
        ];

        for original in originals {
            let original_bytes = original.to_bytes();
            let neutral = Packet::from(original);
            let rebuilt =
                MqttPacketV3::try_from(neutral).expect("rebuild v3 subscription ack flow");

            assert_eq!(rebuilt.to_bytes(), original_bytes);
        }
    }

    #[test]
    fn auth_packet_cannot_be_encoded_as_mqtt3() {
        let err = MqttPacketV3::try_from(Packet::Auth(Auth {
            reason_code: ReasonCode::Success,
            properties: Properties::default(),
        }))
        .unwrap_err();

        assert_eq!(
            err,
            PacketConversionError::UnsupportedPacketForMqtt3("AUTH")
        );
    }
}
