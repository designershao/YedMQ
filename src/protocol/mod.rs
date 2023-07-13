use bytes::BytesMut;
use nom::{combinator::{map, consumed}, IResult};

use self::v3::fixed_header;

pub mod v3;

#[derive(Debug, PartialEq, Clone, Copy)]
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

    /*
     * Returns the packet type
     */
    fn get_packet_type(&self) -> PacketType;

}

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

    pub fn to_bytes(&self) -> BytesMut {
        match self {
            MqttPacketV3::Connect(p) => {
                p.to_bytes()
            }
            MqttPacketV3::Connack(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Publish(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Puback(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Pubrec(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Pubrel(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Pubcomp(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Subscribe(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Suback(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Unsubscribe(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Unsuback(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Pingreq(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Pingresp(p) => {
                p.to_bytes()
            },
            MqttPacketV3::Disconnect(p) => {
                p.to_bytes()
            },
        }
    }

}

pub fn parse(input: &[u8]) -> IResult<&[u8], (&[u8],MqttPacketV3)> {
    let fix_header = fixed_header::parse(input);
    match fix_header {
        Ok((_, fix_header)) => {
            let packet_type = fix_header.packet_type;
            match packet_type {
                PacketType::CONNECT => {
                    consumed(map(v3::connect::parse,|p|{
                        MqttPacketV3::Connect(p)
                    }))(input)
                }
                PacketType::CONNACK => {
                    consumed(map(v3::connack::parse,|p|{
                        MqttPacketV3::Connack(p)
                    }))(input)
                },
                PacketType::PUBLISH => {
                    consumed(map(v3::publish::parse, |p|{
                        MqttPacketV3::Publish(p)
                    }))(input)
                },
                PacketType::PUBACK => {
                    consumed(map(v3::puback::parse, |p|{
                        MqttPacketV3::Puback(p)
                    }))(input)
                },
                PacketType::PUBREC => {
                    consumed(map(v3::pubrec::parse,|p|{
                        MqttPacketV3::Pubrec(p)
                    }))(input)
                },
                PacketType::PUBREL => {
                    consumed(map(v3::pubrel::parse,|p|{
                        MqttPacketV3::Pubrel(p)
                    }))(input)
                },
                PacketType::PUBCOMP => {
                    consumed(map(v3::pubcomp::parse,|p|{
                        MqttPacketV3::Pubcomp(p)
                    }))(input)
                },
                PacketType::SUBSCRIBE => {
                    consumed(map(v3::subscribe::parse,|p|{
                        MqttPacketV3::Subscribe(p)
                    }))(input)
                },
                PacketType::SUBACK => {
                    consumed(map(v3::suback::parse,|p|{
                        MqttPacketV3::Suback(p)
                    }))(input)
                },
                PacketType::UNSUBSCRIBE => {
                    consumed(map(v3::unsubscribe::parse,|p|{
                        MqttPacketV3::Unsubscribe(p)
                    }))(input)
                },
                PacketType::UNSUBACK => {
                    consumed(map(v3::unsuback::parse,|p|{
                        MqttPacketV3::Unsuback(p)
                    }))(input)
                },
                PacketType::PINGREQ => {
                    consumed(map(v3::pingreq::parse,|p|{
                        MqttPacketV3::Pingreq(p) 
                    }))(input)
                },
                PacketType::PINGRESP => {
                    consumed(map(v3::pingresp::parse,|p|{
                        MqttPacketV3::Pingresp(p)
                    }))(input)
                },
                PacketType::DISCONNECT => {
                    consumed(map(v3::disconnect::parse,|p|{
                        MqttPacketV3::Disconnect(p)
                    }))(input)
                },
            }
        },
        Err(e) => {
            return Err(e); 
        }
    }
}