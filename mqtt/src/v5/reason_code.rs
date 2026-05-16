use crate::packet::ReasonCode;

use super::common::Mqtt5ParseError;

pub fn decode(value: u8) -> Result<ReasonCode, Mqtt5ParseError> {
    match value {
        0x00 => Ok(ReasonCode::Success),
        0x01 => Ok(ReasonCode::GrantedQos1),
        0x02 => Ok(ReasonCode::GrantedQos2),
        0x10 => Ok(ReasonCode::NoMatchingSubscribers),
        0x80 => Ok(ReasonCode::UnspecifiedError),
        0x81 => Ok(ReasonCode::MalformedPacket),
        0x82 => Ok(ReasonCode::ProtocolError),
        0x83 => Ok(ReasonCode::ImplementationSpecificError),
        0x84 => Ok(ReasonCode::UnsupportedProtocolVersion),
        0x85 => Ok(ReasonCode::ClientIdentifierNotValid),
        0x86 => Ok(ReasonCode::BadUserNameOrPassword),
        0x87 => Ok(ReasonCode::NotAuthorized),
        0x88 => Ok(ReasonCode::ServerUnavailable),
        0x8c => Ok(ReasonCode::BadAuthenticationMethod),
        0x8f => Ok(ReasonCode::TopicFilterInvalid),
        0x90 => Ok(ReasonCode::TopicNameInvalid),
        0x91 => Ok(ReasonCode::PacketIdentifierInUse),
        0x95 => Ok(ReasonCode::PacketTooLarge),
        0x97 => Ok(ReasonCode::QuotaExceeded),
        0x99 => Ok(ReasonCode::PayloadFormatInvalid),
        0x9a => Ok(ReasonCode::RetainNotSupported),
        0x9b => Ok(ReasonCode::QosNotSupported),
        0x9e => Ok(ReasonCode::SharedSubscriptionsNotSupported),
        0xa1 => Ok(ReasonCode::SubscriptionIdentifiersNotSupported),
        0xa2 => Ok(ReasonCode::WildcardSubscriptionsNotSupported),
        other => Err(Mqtt5ParseError::UnknownReasonCode(other)),
    }
}

pub fn encode(reason_code: ReasonCode) -> u8 {
    reason_code.as_u8()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_first_release_reason_codes() {
        assert_eq!(decode(0x00).unwrap(), ReasonCode::Success);
        assert_eq!(
            decode(0x84).unwrap(),
            ReasonCode::UnsupportedProtocolVersion
        );
        assert_eq!(
            decode(0xa1).unwrap(),
            ReasonCode::SubscriptionIdentifiersNotSupported
        );
    }

    #[test]
    fn rejects_unknown_reason_code() {
        assert_eq!(
            decode(0xff).unwrap_err(),
            Mqtt5ParseError::UnknownReasonCode(0xff)
        );
    }

    #[test]
    fn encodes_reason_code() {
        assert_eq!(encode(ReasonCode::BadAuthenticationMethod), 0x8c);
    }
}
