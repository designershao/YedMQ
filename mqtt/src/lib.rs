use bytes::BytesMut;
use nom::{
    combinator::{consumed, map},
    IResult,
};
use serde::{Deserialize, Serialize};

use self::v3::fixed_header;

pub mod v3;

pub const MQTT_MAX_MESSAGE_SIZE: u32 = 268435456;

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum PacketType {
    CONNECT,
    CONNACK,
    PUBLISH,
    PUBACK,
    PUBREC,
    PUBREL,
    PUBCOMP,
    SUBSCRIBE,
    SUBACK,
    UNSUBSCRIBE,
    UNSUBACK,
    PINGREQ,
    PINGRESP,
    DISCONNECT,
}

trait MqttPacket {
    fn to_bytes(&self) -> BytesMut;

    fn encode(&self, buffer: &mut BytesMut);

    /*
     * Returns the packet type
     */
    fn get_packet_type(&self) -> PacketType;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqttPacketV3 {
    Connect(v3::connect::ConnectPacket),
    Connack(v3::connack::ConnAckPacket),
    Publish(v3::publish::PublishPacket),
    Puback(v3::puback::PubAckPacket),
    Pubrec(v3::pubrec::PubRecPacket),
    Pubrel(v3::pubrel::PubRelPacket),
    Pubcomp(v3::pubcomp::PubCompPacket),
    Subscribe(v3::subscribe::SubscribePacket),
    Suback(v3::suback::SubackPacket),
    Unsubscribe(v3::unsubscribe::UnsubscribePacket),
    Unsuback(v3::unsuback::UnSubackPacket),
    Pingreq(v3::pingreq::PingreqPacket),
    Pingresp(v3::pingresp::PingrespPacket),
    Disconnect(v3::disconnect::DisconnectPacket),
}

impl MqttPacketV3 {
    pub fn set_dup(&mut self, dup: i32) {
        match self {
            MqttPacketV3::Connect(p) => p.fix_header.dup = None,
            MqttPacketV3::Connack(p) => p.fix_header.dup = None,
            MqttPacketV3::Publish(p) => p.fix_header.dup = Some(dup),
            MqttPacketV3::Puback(p) => p.fix_header.dup = Some(dup),
            MqttPacketV3::Pubrec(p) => p.fix_header.dup = Some(dup),
            MqttPacketV3::Pubrel(p) => p.fix_header.dup = Some(dup),
            MqttPacketV3::Pubcomp(p) => p.fix_header.dup = Some(dup),
            MqttPacketV3::Subscribe(p) => p.fix_header.dup = None,
            MqttPacketV3::Suback(p) => p.fix_header.dup = None,
            MqttPacketV3::Unsubscribe(p) => p.fix_header.dup = None,
            MqttPacketV3::Unsuback(p) => p.fix_header.dup = None,

            MqttPacketV3::Pingreq(p) => p.fix_header.dup = None,
            MqttPacketV3::Pingresp(p) => p.fix_header.dup = None,
            MqttPacketV3::Disconnect(p) => p.fix_header.dup = None,
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            MqttPacketV3::Publish(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Connect(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Connack(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Puback(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Pubrec(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Pubrel(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Pubcomp(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Subscribe(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Suback(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Unsubscribe(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Unsuback(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Pingreq(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Pingresp(p) => {
                p.encode(buf);
            }
            MqttPacketV3::Disconnect(p) => {
                p.encode(buf);
            }
        }
    }

    pub fn to_bytes(&self) -> BytesMut {
        match self {
            MqttPacketV3::Connect(p) => p.to_bytes(),
            MqttPacketV3::Connack(p) => p.to_bytes(),
            MqttPacketV3::Publish(p) => p.to_bytes(),
            MqttPacketV3::Puback(p) => p.to_bytes(),
            MqttPacketV3::Pubrec(p) => p.to_bytes(),
            MqttPacketV3::Pubrel(p) => p.to_bytes(),
            MqttPacketV3::Pubcomp(p) => p.to_bytes(),
            MqttPacketV3::Subscribe(p) => p.to_bytes(),
            MqttPacketV3::Suback(p) => p.to_bytes(),
            MqttPacketV3::Unsubscribe(p) => p.to_bytes(),
            MqttPacketV3::Unsuback(p) => p.to_bytes(),
            MqttPacketV3::Pingreq(p) => p.to_bytes(),
            MqttPacketV3::Pingresp(p) => p.to_bytes(),
            MqttPacketV3::Disconnect(p) => p.to_bytes(),
        }
    }
}

pub fn parse(input: &[u8], max_message_size: u32) -> IResult<&[u8], (&[u8], MqttPacketV3)> {
    let fix_header = fixed_header::parse(input);
    match fix_header {
        Ok((_, fix_header)) => {
            let packet_type = fix_header.packet_type;
            match packet_type {
                PacketType::CONNECT => {
                    consumed(map(v3::connect::parse, |p| MqttPacketV3::Connect(p)))(input)
                }
                PacketType::CONNACK => {
                    consumed(map(v3::connack::parse, |p| MqttPacketV3::Connack(p)))(input)
                }
                PacketType::PUBLISH => {
                    let dest_parse = |input| {
                        let max_message_size = max_message_size as usize;
                        v3::publish::parse_with_max_message_size_limit(input, max_message_size)
                    };
                    consumed(map(dest_parse, |p| MqttPacketV3::Publish(p)))(input)
                }
                PacketType::PUBACK => {
                    consumed(map(v3::puback::parse, |p| MqttPacketV3::Puback(p)))(input)
                }
                PacketType::PUBREC => {
                    consumed(map(v3::pubrec::parse, |p| MqttPacketV3::Pubrec(p)))(input)
                }
                PacketType::PUBREL => {
                    consumed(map(v3::pubrel::parse, |p| MqttPacketV3::Pubrel(p)))(input)
                }
                PacketType::PUBCOMP => {
                    consumed(map(v3::pubcomp::parse, |p| MqttPacketV3::Pubcomp(p)))(input)
                }
                PacketType::SUBSCRIBE => {
                    consumed(map(v3::subscribe::parse, |p| MqttPacketV3::Subscribe(p)))(input)
                }
                PacketType::SUBACK => {
                    consumed(map(v3::suback::parse, |p| MqttPacketV3::Suback(p)))(input)
                }
                PacketType::UNSUBSCRIBE => consumed(map(v3::unsubscribe::parse, |p| {
                    MqttPacketV3::Unsubscribe(p)
                }))(input),
                PacketType::UNSUBACK => {
                    consumed(map(v3::unsuback::parse, |p| MqttPacketV3::Unsuback(p)))(input)
                }
                PacketType::PINGREQ => {
                    consumed(map(v3::pingreq::parse, |p| MqttPacketV3::Pingreq(p)))(input)
                }
                PacketType::PINGRESP => {
                    consumed(map(v3::pingresp::parse, |p| MqttPacketV3::Pingresp(p)))(input)
                }
                PacketType::DISCONNECT => {
                    consumed(map(v3::disconnect::parse, |p| MqttPacketV3::Disconnect(p)))(input)
                }
            }
        }
        Err(e) => Err(e),
    }
}
