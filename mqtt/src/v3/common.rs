use byteorder::{BigEndian, ByteOrder};
use nom::{IResult, combinator::flat_map, Parser};

pub fn parse_utf8(input: &[u8]) -> IResult<&[u8], String> {
    flat_map(nom::bytes::streaming::take(2usize),|w|{
        let length = BigEndian::read_u16(w);
        nom::bytes::streaming::take(length).map(|w|{String::from_utf8_lossy(w).into()})
    })(input)
}

pub fn parse_utf8_complete(input: &[u8]) -> IResult<&[u8], String> {
    flat_map(nom::bytes::complete::take(2usize),|w|{
        let length = BigEndian::read_u16(w);
        nom::bytes::complete::take(length).map(|w|{String::from_utf8_lossy(w).into()})
    })(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_utf8() {
        let input = &[0x00, 0x01, 0x41];
        let out = parse_utf8(input).unwrap();
        assert_eq!(out.1, "A".to_string());
    }

}