use nom::{IResult, combinator::map};

use super::fixed_header::{FixHeader, self};


pub struct PingreqPacket {
    fix_header: FixHeader,
}

pub fn parse(input: &[u8]) -> IResult<&[u8], PingreqPacket> {
    map(
    fixed_header::parse,
    |fixed_header| {
        PingreqPacket {
            fix_header: fixed_header,
        } 
    })(input)
}

#[cfg(test)]
mod tests {
    use crate::protocol::v3::fixed_header::PacketType;

    use super::*;

    #[test]
    fn test_parse() {
        let input = &[0xC0,0x00];
        let out = parse(input).unwrap();
        assert!(out.1.fix_header.packet_type == PacketType::PINGREQ);
    }
}