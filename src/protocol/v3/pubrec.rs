use byteorder::{BigEndian, ByteOrder};
use nom::{IResult, Parser, number::streaming::{be_u16, be_u8}, combinator::{map_res, flat_map, map, rest}, sequence::tuple, bits, error::Error};
use nom::bytes::{streaming::take};
use super::{fixed_header::{FixHeader, self}, common::parse_utf8};

pub struct PubRecPacket {
    fix_header: FixHeader,
    variable_header: VariableHeader
}

pub struct VariableHeader {
    packet_identifier: u16,
}

pub fn parse(input: &[u8]) -> IResult<&[u8], PubRecPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
            nom::bytes::streaming::take(fixed_header.remaining_length),
            be_u16::<&[u8], Error<&[u8]>>
            ), move |packet_identifier| {
                let cloned_fixed_header = fixed_header.clone();
                PubRecPacket {
                    fix_header: cloned_fixed_header,
                    variable_header: VariableHeader {
                        packet_identifier: packet_identifier.1
                    }
                }
            })
    })(input)
}

#[cfg(test)]
mod tests{
    use crate::protocol::v3::fixed_header::PacketType;

    use super::*;

    #[test]
    fn test_parse() {
        let input = &[0x50, 0x02, 0x00, 0x0A];
        let out = parse(input).unwrap();
        assert_eq!(out.1.variable_header.packet_identifier, 10);
        assert_eq!(out.1.fix_header.packet_type, PacketType::PUBREC);
    }

}

