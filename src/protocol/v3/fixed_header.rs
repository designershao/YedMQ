use nom::bits::{bits, streaming::take};
use nom::bytes::streaming::take_while_m_n;
use nom::sequence::tuple;
use nom::{IResult, Err, Needed, error::{Error, ErrorKind}, bytes, character};

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, Clone)]
pub struct FixHeader {
    pub packet_type: PacketType,
    pub qos: Option<i32>,
    pub retain: Option<bool>,
    pub dup: Option<i32>,
    pub remaining_length: usize,
}


pub fn has_next(i:u8) -> bool {
    i & 128 != 0
}

pub fn remaining_length(input: &[u8]) -> IResult<&[u8], usize> {
    let r = take_while_m_n(0, 3, has_next)(input);
    match r {
        Err(e) => Err(e),
        Ok((i, length)) => {
            let mut v:i32 = 0;
            let mut multiplier = 1;
            for u in length.iter() {
                v += ((*u & 127) as i32) * multiplier;
                multiplier *= 128;
            }
            match nom::bytes::streaming::take(1u8)(i) {
                Err(e) => Err(e),
                Ok((ii, vv)) => {
                    for u in vv.iter() {
                        v += ((*u & 127) as i32) * multiplier;
                    }
                    return Ok((ii, v as usize));
                }
            }
        }
    }
}

pub fn parse(input: &[u8]) -> IResult<&[u8], FixHeader> {
    let r = bits::<&[u8],(i32,i32,i32,i32),Error<(&[u8], usize)>,_,_>(tuple((take(4usize), take(1usize),take(2usize),take(1usize))))(input);
    match r {
        Err(e) => Err(e) ,
        Ok((i,(packet_type_u, dup_u,qos_u,retain_u))) => {
            let packet_type = match packet_type_u {
                1 => PacketType::CONNECT,
                2 => PacketType::CONNACK,
                3 => PacketType::PUBLISH,
                4 => PacketType::PUBACK,
                5 => PacketType::PUBREC,
                6 => PacketType::PUBREL,
                7 => PacketType::PUBCOMP,
                8 => PacketType::SUBSCRIBE,
                9 => PacketType::SUBACK,
                10 => PacketType::UNSUBSCRIBE,
                11 => PacketType::UNSUBACK,
                12 => PacketType::PINGREQ,
                13 => PacketType::PINGRESP,
                14 => PacketType::DISCONNECT,
                _ => panic!("Unknown packet type"),
            };
            let qos = if packet_type == PacketType::PUBLISH { Some(qos_u) } else { None };
            let retain = if packet_type == PacketType::PUBLISH  { Some(retain_u == 1) } else { None };
            let dup = if packet_type == PacketType::PUBLISH { Some(dup_u) } else { None };
            match remaining_length(i) {
                Err(e) => Err(e),
                Ok((i, remaining_length)) => return Ok((i, FixHeader {
                packet_type,
                qos,
                retain,
                dup,
                remaining_length: remaining_length.try_into().unwrap()
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remaining_length() {
        let input = &[0xFF, 0xFF, 0xFF, 0x7F];
        let output = remaining_length(input).unwrap();
        let length = output.1;
        assert_eq!(length, 268435455);

        let input = &[0x7F];
        let output = remaining_length(input).unwrap();
        let length = output.1;
        assert_eq!(length, 127);

        let input = &[0xFF,0x7F];
        let output = remaining_length(input).unwrap();
        let length = output.1;
        assert_eq!(length, 16383);
    }

    #[test]
    fn test_fix_header() {
        let input = &[0xE0, 0x00]; // Disconnect Message bytes
        let out = parse(input).unwrap();
        let header = out.1;
        assert_eq!(header.packet_type, PacketType::DISCONNECT);
    }
}