use bytes::BytesMut;

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

pub trait MqttPacket {

    fn to_bytes(&self) -> BytesMut;

    /*
     * Returns the packet type
     */
    fn get_packet_type(&self) -> PacketType;

}
