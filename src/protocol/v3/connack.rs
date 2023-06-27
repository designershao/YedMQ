use byteorder::{BigEndian, ByteOrder};
use nom::{IResult, Parser, number::streaming::{be_u16, be_u8}, combinator::{map_res, flat_map, map}, sequence::tuple, bits, error::Error};
use nom::bytes::{streaming::take};
use super::fixed_header::{FixHeader, self};

pub struct ConnAckPacket {
    pub fix_header: FixHeader,
    pub variable_header: VariableHeader,
}

pub struct VariableHeader {
    pub session_present: bool,
    pub connect_return_code: u8,
}

// MQTT Connect ACK Variable Header
// +---------------------+----------+---+---+---+---+---+---+---+----+
// |                     | Desc     | 7 | 6 | 5 | 4 | 3 | 2 | 1 | 0  |
// +---------------------+----------+---+---+---+---+---+---+---+----+
// |                     | Reserved |   |   |   |   |   |   |   | SP |
// | byte1               |          | 0 | 0 | 0 | 0 | 0 | 0 | 0 | x  |
// | Connect Return Code                                             |
// | byte2               |          | x | x | x | x | x | x | x | x  |
// +---------------------+----------+---+---+---+---+---+---+---+----+
fn variable_header(input:&[u8]) -> IResult<&[u8], VariableHeader> {
    map(
        tuple((take::<u8, &[u8], Error<&[u8]>>(1u8),take(1u8))),
        |v| {
            let session_present = v.0[0] & 0x1 == 0x1;
            VariableHeader{
                session_present,
                connect_return_code: v.1[0]
            }
        }
    )(input)
}

pub fn parse(input: &[u8]) -> IResult<&[u8], ConnAckPacket> {
    flat_map(fixed_header::parse, |fixed_header| {
        map(
            map_res(
            nom::bytes::streaming::take(fixed_header.remaining_length),
            variable_header
            ), move |(_,(variable_header))| {
                let cloned_fixed_header = fixed_header.clone();
                ConnAckPacket { 
                    fix_header:cloned_fixed_header , 
                    variable_header 
                }
            }
        )
    })(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variable_header() {
        let input = &[0x01, 0x01];
        let output = variable_header(input).unwrap();
        assert_eq!(output.1.session_present, true);
        assert_eq!(output.1.connect_return_code, 1);
    }
}