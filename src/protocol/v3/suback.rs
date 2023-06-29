use byteorder::{BigEndian, ByteOrder};
use nom::{IResult, Parser, number::streaming::{be_u16, be_u8}, combinator::{map_res, flat_map, map, rest}, sequence::tuple, bits, error::Error};
use nom::bytes::{streaming::take};
use super::{fixed_header::{FixHeader, self}, common::parse_utf8};

pub struct SubackPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
    pub payload: Payload
}

pub struct VariableHeader {
    pub packet_identifier: u16,
}

pub struct Payload {
    pub return_code: u8,
}

pub fn parse(input: &[u8]) -> IResult<&[u8], SubackPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
            nom::bytes::streaming::take(fixed_header.remaining_length),
            tuple((be_u16::<&[u8], Error<&[u8]>>,be_u8))),
            move |(_, (packet_identifier, return_code))| {
                let cloned_fixed_header = fixed_header.clone();
                SubackPacket {
                    fix_header: cloned_fixed_header,
                    variable_header: VariableHeader {
                        packet_identifier: packet_identifier
                    },
                    payload: Payload {
                        return_code: return_code
                    }
                }
            })
        })(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let input = &[0x90, 0x03, 0x00,0x01, 0x02];
        let out = parse(input).unwrap();
        assert_eq!(out.1.variable_header.packet_identifier, 1);
        assert_eq!(out.1.payload.return_code, 2);
    }
}