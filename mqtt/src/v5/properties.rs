use bytes::{BufMut, BytesMut};

use crate::packet::Properties;

use super::common::{
    encode_binary, encode_utf8_string, encode_variable_byte_integer, parse_binary, parse_boolean,
    parse_u16, parse_u32, parse_u8, parse_utf8_string, parse_variable_byte_integer,
    Mqtt5ParseError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyScope {
    Connect,
    Connack,
    Publish,
    Will,
    Puback,
    Pubrec,
    Pubrel,
    Pubcomp,
    Subscribe,
    Suback,
    Unsubscribe,
    Unsuback,
    Disconnect,
    Auth,
}

impl PropertyScope {
    fn name(self) -> &'static str {
        match self {
            PropertyScope::Connect => "CONNECT",
            PropertyScope::Connack => "CONNACK",
            PropertyScope::Publish => "PUBLISH",
            PropertyScope::Will => "Will Properties",
            PropertyScope::Puback => "PUBACK",
            PropertyScope::Pubrec => "PUBREC",
            PropertyScope::Pubrel => "PUBREL",
            PropertyScope::Pubcomp => "PUBCOMP",
            PropertyScope::Subscribe => "SUBSCRIBE",
            PropertyScope::Suback => "SUBACK",
            PropertyScope::Unsubscribe => "UNSUBSCRIBE",
            PropertyScope::Unsuback => "UNSUBACK",
            PropertyScope::Disconnect => "DISCONNECT",
            PropertyScope::Auth => "AUTH",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropertyIdentifier {
    PayloadFormatIndicator,
    MessageExpiryInterval,
    ContentType,
    ResponseTopic,
    CorrelationData,
    SubscriptionIdentifier,
    SessionExpiryInterval,
    AssignedClientIdentifier,
    ServerKeepAlive,
    AuthenticationMethod,
    AuthenticationData,
    RequestProblemInformation,
    WillDelayInterval,
    RequestResponseInformation,
    ResponseInformation,
    ServerReference,
    ReasonString,
    ReceiveMaximum,
    TopicAliasMaximum,
    TopicAlias,
    MaximumQos,
    RetainAvailable,
    UserProperty,
    MaximumPacketSize,
    WildcardSubscriptionAvailable,
    SubscriptionIdentifierAvailable,
    SharedSubscriptionAvailable,
}

impl PropertyIdentifier {
    fn from_u32(value: u32) -> Result<Self, Mqtt5ParseError> {
        match value {
            0x01 => Ok(PropertyIdentifier::PayloadFormatIndicator),
            0x02 => Ok(PropertyIdentifier::MessageExpiryInterval),
            0x03 => Ok(PropertyIdentifier::ContentType),
            0x08 => Ok(PropertyIdentifier::ResponseTopic),
            0x09 => Ok(PropertyIdentifier::CorrelationData),
            0x0b => Ok(PropertyIdentifier::SubscriptionIdentifier),
            0x11 => Ok(PropertyIdentifier::SessionExpiryInterval),
            0x12 => Ok(PropertyIdentifier::AssignedClientIdentifier),
            0x13 => Ok(PropertyIdentifier::ServerKeepAlive),
            0x15 => Ok(PropertyIdentifier::AuthenticationMethod),
            0x16 => Ok(PropertyIdentifier::AuthenticationData),
            0x17 => Ok(PropertyIdentifier::RequestProblemInformation),
            0x18 => Ok(PropertyIdentifier::WillDelayInterval),
            0x19 => Ok(PropertyIdentifier::RequestResponseInformation),
            0x1a => Ok(PropertyIdentifier::ResponseInformation),
            0x1c => Ok(PropertyIdentifier::ServerReference),
            0x1f => Ok(PropertyIdentifier::ReasonString),
            0x21 => Ok(PropertyIdentifier::ReceiveMaximum),
            0x22 => Ok(PropertyIdentifier::TopicAliasMaximum),
            0x23 => Ok(PropertyIdentifier::TopicAlias),
            0x24 => Ok(PropertyIdentifier::MaximumQos),
            0x25 => Ok(PropertyIdentifier::RetainAvailable),
            0x26 => Ok(PropertyIdentifier::UserProperty),
            0x27 => Ok(PropertyIdentifier::MaximumPacketSize),
            0x28 => Ok(PropertyIdentifier::WildcardSubscriptionAvailable),
            0x29 => Ok(PropertyIdentifier::SubscriptionIdentifierAvailable),
            0x2a => Ok(PropertyIdentifier::SharedSubscriptionAvailable),
            other => Err(Mqtt5ParseError::UnknownProperty(other)),
        }
    }

    fn as_u32(self) -> u32 {
        match self {
            PropertyIdentifier::PayloadFormatIndicator => 0x01,
            PropertyIdentifier::MessageExpiryInterval => 0x02,
            PropertyIdentifier::ContentType => 0x03,
            PropertyIdentifier::ResponseTopic => 0x08,
            PropertyIdentifier::CorrelationData => 0x09,
            PropertyIdentifier::SubscriptionIdentifier => 0x0b,
            PropertyIdentifier::SessionExpiryInterval => 0x11,
            PropertyIdentifier::AssignedClientIdentifier => 0x12,
            PropertyIdentifier::ServerKeepAlive => 0x13,
            PropertyIdentifier::AuthenticationMethod => 0x15,
            PropertyIdentifier::AuthenticationData => 0x16,
            PropertyIdentifier::RequestProblemInformation => 0x17,
            PropertyIdentifier::WillDelayInterval => 0x18,
            PropertyIdentifier::RequestResponseInformation => 0x19,
            PropertyIdentifier::ResponseInformation => 0x1a,
            PropertyIdentifier::ServerReference => 0x1c,
            PropertyIdentifier::ReasonString => 0x1f,
            PropertyIdentifier::ReceiveMaximum => 0x21,
            PropertyIdentifier::TopicAliasMaximum => 0x22,
            PropertyIdentifier::TopicAlias => 0x23,
            PropertyIdentifier::MaximumQos => 0x24,
            PropertyIdentifier::RetainAvailable => 0x25,
            PropertyIdentifier::UserProperty => 0x26,
            PropertyIdentifier::MaximumPacketSize => 0x27,
            PropertyIdentifier::WildcardSubscriptionAvailable => 0x28,
            PropertyIdentifier::SubscriptionIdentifierAvailable => 0x29,
            PropertyIdentifier::SharedSubscriptionAvailable => 0x2a,
        }
    }

    fn name(self) -> &'static str {
        match self {
            PropertyIdentifier::PayloadFormatIndicator => "payload_format_indicator",
            PropertyIdentifier::MessageExpiryInterval => "message_expiry_interval",
            PropertyIdentifier::ContentType => "content_type",
            PropertyIdentifier::ResponseTopic => "response_topic",
            PropertyIdentifier::CorrelationData => "correlation_data",
            PropertyIdentifier::SubscriptionIdentifier => "subscription_identifier",
            PropertyIdentifier::SessionExpiryInterval => "session_expiry_interval",
            PropertyIdentifier::AssignedClientIdentifier => "assigned_client_identifier",
            PropertyIdentifier::ServerKeepAlive => "server_keep_alive",
            PropertyIdentifier::AuthenticationMethod => "authentication_method",
            PropertyIdentifier::AuthenticationData => "authentication_data",
            PropertyIdentifier::RequestProblemInformation => "request_problem_information",
            PropertyIdentifier::WillDelayInterval => "will_delay_interval",
            PropertyIdentifier::RequestResponseInformation => "request_response_information",
            PropertyIdentifier::ResponseInformation => "response_information",
            PropertyIdentifier::ServerReference => "server_reference",
            PropertyIdentifier::ReasonString => "reason_string",
            PropertyIdentifier::ReceiveMaximum => "receive_maximum",
            PropertyIdentifier::TopicAliasMaximum => "topic_alias_maximum",
            PropertyIdentifier::TopicAlias => "topic_alias",
            PropertyIdentifier::MaximumQos => "maximum_qos",
            PropertyIdentifier::RetainAvailable => "retain_available",
            PropertyIdentifier::UserProperty => "user_property",
            PropertyIdentifier::MaximumPacketSize => "maximum_packet_size",
            PropertyIdentifier::WildcardSubscriptionAvailable => "wildcard_subscription_available",
            PropertyIdentifier::SubscriptionIdentifierAvailable => {
                "subscription_identifier_available"
            }
            PropertyIdentifier::SharedSubscriptionAvailable => "shared_subscription_available",
        }
    }

    fn allowed_on(self, scope: PropertyScope) -> bool {
        match scope {
            PropertyScope::Connect => matches!(
                self,
                PropertyIdentifier::SessionExpiryInterval
                    | PropertyIdentifier::ReceiveMaximum
                    | PropertyIdentifier::MaximumPacketSize
                    | PropertyIdentifier::TopicAliasMaximum
                    | PropertyIdentifier::RequestResponseInformation
                    | PropertyIdentifier::RequestProblemInformation
                    | PropertyIdentifier::UserProperty
                    | PropertyIdentifier::AuthenticationMethod
                    | PropertyIdentifier::AuthenticationData
            ),
            PropertyScope::Connack => matches!(
                self,
                PropertyIdentifier::SessionExpiryInterval
                    | PropertyIdentifier::AssignedClientIdentifier
                    | PropertyIdentifier::ServerKeepAlive
                    | PropertyIdentifier::AuthenticationMethod
                    | PropertyIdentifier::AuthenticationData
                    | PropertyIdentifier::ResponseInformation
                    | PropertyIdentifier::ServerReference
                    | PropertyIdentifier::ReasonString
                    | PropertyIdentifier::ReceiveMaximum
                    | PropertyIdentifier::TopicAliasMaximum
                    | PropertyIdentifier::MaximumQos
                    | PropertyIdentifier::RetainAvailable
                    | PropertyIdentifier::UserProperty
                    | PropertyIdentifier::MaximumPacketSize
                    | PropertyIdentifier::WildcardSubscriptionAvailable
                    | PropertyIdentifier::SubscriptionIdentifierAvailable
                    | PropertyIdentifier::SharedSubscriptionAvailable
            ),
            PropertyScope::Publish => matches!(
                self,
                PropertyIdentifier::PayloadFormatIndicator
                    | PropertyIdentifier::MessageExpiryInterval
                    | PropertyIdentifier::ContentType
                    | PropertyIdentifier::ResponseTopic
                    | PropertyIdentifier::CorrelationData
                    | PropertyIdentifier::SubscriptionIdentifier
                    | PropertyIdentifier::TopicAlias
                    | PropertyIdentifier::UserProperty
            ),
            PropertyScope::Will => matches!(
                self,
                PropertyIdentifier::PayloadFormatIndicator
                    | PropertyIdentifier::MessageExpiryInterval
                    | PropertyIdentifier::ContentType
                    | PropertyIdentifier::ResponseTopic
                    | PropertyIdentifier::CorrelationData
                    | PropertyIdentifier::WillDelayInterval
                    | PropertyIdentifier::UserProperty
            ),
            PropertyScope::Puback
            | PropertyScope::Pubrec
            | PropertyScope::Pubrel
            | PropertyScope::Pubcomp
            | PropertyScope::Suback
            | PropertyScope::Unsuback => {
                matches!(
                    self,
                    PropertyIdentifier::ReasonString | PropertyIdentifier::UserProperty
                )
            }
            PropertyScope::Subscribe => {
                matches!(
                    self,
                    PropertyIdentifier::SubscriptionIdentifier | PropertyIdentifier::UserProperty
                )
            }
            PropertyScope::Unsubscribe => matches!(self, PropertyIdentifier::UserProperty),
            PropertyScope::Disconnect => matches!(
                self,
                PropertyIdentifier::SessionExpiryInterval
                    | PropertyIdentifier::ReasonString
                    | PropertyIdentifier::UserProperty
                    | PropertyIdentifier::ServerReference
            ),
            PropertyScope::Auth => matches!(
                self,
                PropertyIdentifier::AuthenticationMethod
                    | PropertyIdentifier::AuthenticationData
                    | PropertyIdentifier::ReasonString
                    | PropertyIdentifier::UserProperty
            ),
        }
    }
}

pub fn parse(input: &[u8], scope: PropertyScope) -> Result<(&[u8], Properties), Mqtt5ParseError> {
    let (input, property_length) = parse_variable_byte_integer(input)?;
    let property_length = property_length as usize;
    if input.len() < property_length {
        return Err(Mqtt5ParseError::Incomplete("properties"));
    }

    let (mut property_input, input) = input.split_at(property_length);
    let mut properties = Properties::default();

    while !property_input.is_empty() {
        let (after_identifier, identifier) = parse_variable_byte_integer(property_input)?;
        property_input = after_identifier;
        let identifier = PropertyIdentifier::from_u32(identifier)?;
        ensure_allowed(identifier, scope)?;

        property_input = match identifier {
            PropertyIdentifier::PayloadFormatIndicator => {
                let (input, value) = parse_u8(property_input, identifier.name())?;
                if value > 1 {
                    return Err(Mqtt5ParseError::InvalidPropertyValue(identifier.name()));
                }
                set_once(
                    &mut properties.payload_format_indicator,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::MessageExpiryInterval => {
                let (input, value) = parse_u32(property_input, identifier.name())?;
                set_once(
                    &mut properties.message_expiry_interval,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::ContentType => {
                let (input, value) = parse_utf8_string(property_input)?;
                set_once(&mut properties.content_type, value, identifier.name())?;
                input
            }
            PropertyIdentifier::ResponseTopic => {
                let (input, value) = parse_utf8_string(property_input)?;
                set_once(&mut properties.response_topic, value, identifier.name())?;
                input
            }
            PropertyIdentifier::CorrelationData => {
                let (input, value) = parse_binary(property_input)?;
                set_once(&mut properties.correlation_data, value, identifier.name())?;
                input
            }
            PropertyIdentifier::SubscriptionIdentifier => {
                let (input, value) = parse_variable_byte_integer(property_input)?;
                if value == 0 {
                    return Err(Mqtt5ParseError::InvalidPropertyValue(identifier.name()));
                }
                properties.subscription_identifiers.push(value);
                input
            }
            PropertyIdentifier::SessionExpiryInterval => {
                let (input, value) = parse_u32(property_input, identifier.name())?;
                set_once(
                    &mut properties.session_expiry_interval,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::AssignedClientIdentifier => {
                let (input, value) = parse_utf8_string(property_input)?;
                set_once(
                    &mut properties.assigned_client_identifier,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::ServerKeepAlive => {
                let (input, value) = parse_u16(property_input, identifier.name())?;
                set_once(&mut properties.server_keep_alive, value, identifier.name())?;
                input
            }
            PropertyIdentifier::AuthenticationMethod => {
                let (input, value) = parse_utf8_string(property_input)?;
                set_once(
                    &mut properties.authentication_method,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::AuthenticationData => {
                let (input, value) = parse_binary(property_input)?;
                set_once(
                    &mut properties.authentication_data,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::RequestProblemInformation => {
                let (input, value) = parse_boolean(property_input, identifier.name())?;
                set_once(
                    &mut properties.request_problem_information,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::WillDelayInterval => {
                let (input, value) = parse_u32(property_input, identifier.name())?;
                set_once(
                    &mut properties.will_delay_interval,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::RequestResponseInformation => {
                let (input, value) = parse_boolean(property_input, identifier.name())?;
                set_once(
                    &mut properties.request_response_information,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::ResponseInformation => {
                let (input, value) = parse_utf8_string(property_input)?;
                set_once(
                    &mut properties.response_information,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::ServerReference => {
                let (input, value) = parse_utf8_string(property_input)?;
                set_once(&mut properties.server_reference, value, identifier.name())?;
                input
            }
            PropertyIdentifier::ReasonString => {
                let (input, value) = parse_utf8_string(property_input)?;
                set_once(&mut properties.reason_string, value, identifier.name())?;
                input
            }
            PropertyIdentifier::ReceiveMaximum => {
                let (input, value) = parse_u16(property_input, identifier.name())?;
                if value == 0 {
                    return Err(Mqtt5ParseError::InvalidPropertyValue(identifier.name()));
                }
                set_once(&mut properties.receive_maximum, value, identifier.name())?;
                input
            }
            PropertyIdentifier::TopicAliasMaximum => {
                let (input, value) = parse_u16(property_input, identifier.name())?;
                set_once(
                    &mut properties.topic_alias_maximum,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::TopicAlias => {
                let (input, value) = parse_u16(property_input, identifier.name())?;
                if value == 0 {
                    return Err(Mqtt5ParseError::InvalidPropertyValue(identifier.name()));
                }
                set_once(&mut properties.topic_alias, value, identifier.name())?;
                input
            }
            PropertyIdentifier::MaximumQos => {
                let (input, value) = parse_u8(property_input, identifier.name())?;
                if value > 1 {
                    return Err(Mqtt5ParseError::InvalidPropertyValue(identifier.name()));
                }
                set_once(&mut properties.maximum_qos, value, identifier.name())?;
                input
            }
            PropertyIdentifier::RetainAvailable => {
                let (input, value) = parse_boolean(property_input, identifier.name())?;
                set_once(&mut properties.retain_available, value, identifier.name())?;
                input
            }
            PropertyIdentifier::UserProperty => {
                let (input, key) = parse_utf8_string(property_input)?;
                let (input, value) = parse_utf8_string(input)?;
                properties.user_properties.push((key, value));
                input
            }
            PropertyIdentifier::MaximumPacketSize => {
                let (input, value) = parse_u32(property_input, identifier.name())?;
                if value == 0 {
                    return Err(Mqtt5ParseError::InvalidPropertyValue(identifier.name()));
                }
                set_once(
                    &mut properties.maximum_packet_size,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::WildcardSubscriptionAvailable => {
                let (input, value) = parse_boolean(property_input, identifier.name())?;
                set_once(
                    &mut properties.wildcard_subscription_available,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::SubscriptionIdentifierAvailable => {
                let (input, value) = parse_boolean(property_input, identifier.name())?;
                set_once(
                    &mut properties.subscription_identifier_available,
                    value,
                    identifier.name(),
                )?;
                input
            }
            PropertyIdentifier::SharedSubscriptionAvailable => {
                let (input, value) = parse_boolean(property_input, identifier.name())?;
                set_once(
                    &mut properties.shared_subscription_available,
                    value,
                    identifier.name(),
                )?;
                input
            }
        };
    }

    Ok((input, properties))
}

pub fn encode(
    properties: &Properties,
    scope: PropertyScope,
    buffer: &mut BytesMut,
) -> Result<(), Mqtt5ParseError> {
    let mut body = BytesMut::new();

    encode_optional_u8(
        &mut body,
        scope,
        PropertyIdentifier::PayloadFormatIndicator,
        properties.payload_format_indicator,
    )?;
    encode_optional_u32(
        &mut body,
        scope,
        PropertyIdentifier::MessageExpiryInterval,
        properties.message_expiry_interval,
    )?;
    encode_optional_string(
        &mut body,
        scope,
        PropertyIdentifier::ContentType,
        properties.content_type.as_ref(),
    )?;
    encode_optional_string(
        &mut body,
        scope,
        PropertyIdentifier::ResponseTopic,
        properties.response_topic.as_ref(),
    )?;
    encode_optional_binary(
        &mut body,
        scope,
        PropertyIdentifier::CorrelationData,
        properties.correlation_data.as_ref(),
    )?;
    for value in &properties.subscription_identifiers {
        if *value == 0 {
            return Err(Mqtt5ParseError::InvalidPropertyValue(
                PropertyIdentifier::SubscriptionIdentifier.name(),
            ));
        }
        write_property_id(&mut body, scope, PropertyIdentifier::SubscriptionIdentifier)?;
        encode_variable_byte_integer(*value, &mut body)?;
    }
    encode_optional_u32(
        &mut body,
        scope,
        PropertyIdentifier::SessionExpiryInterval,
        properties.session_expiry_interval,
    )?;
    encode_optional_string(
        &mut body,
        scope,
        PropertyIdentifier::AssignedClientIdentifier,
        properties.assigned_client_identifier.as_ref(),
    )?;
    encode_optional_u16(
        &mut body,
        scope,
        PropertyIdentifier::ServerKeepAlive,
        properties.server_keep_alive,
    )?;
    encode_optional_string(
        &mut body,
        scope,
        PropertyIdentifier::AuthenticationMethod,
        properties.authentication_method.as_ref(),
    )?;
    encode_optional_binary(
        &mut body,
        scope,
        PropertyIdentifier::AuthenticationData,
        properties.authentication_data.as_ref(),
    )?;
    encode_optional_bool(
        &mut body,
        scope,
        PropertyIdentifier::RequestProblemInformation,
        properties.request_problem_information,
    )?;
    encode_optional_u32(
        &mut body,
        scope,
        PropertyIdentifier::WillDelayInterval,
        properties.will_delay_interval,
    )?;
    encode_optional_bool(
        &mut body,
        scope,
        PropertyIdentifier::RequestResponseInformation,
        properties.request_response_information,
    )?;
    encode_optional_string(
        &mut body,
        scope,
        PropertyIdentifier::ResponseInformation,
        properties.response_information.as_ref(),
    )?;
    encode_optional_string(
        &mut body,
        scope,
        PropertyIdentifier::ServerReference,
        properties.server_reference.as_ref(),
    )?;
    encode_optional_string(
        &mut body,
        scope,
        PropertyIdentifier::ReasonString,
        properties.reason_string.as_ref(),
    )?;
    encode_optional_nonzero_u16(
        &mut body,
        scope,
        PropertyIdentifier::ReceiveMaximum,
        properties.receive_maximum,
    )?;
    encode_optional_u16(
        &mut body,
        scope,
        PropertyIdentifier::TopicAliasMaximum,
        properties.topic_alias_maximum,
    )?;
    encode_optional_nonzero_u16(
        &mut body,
        scope,
        PropertyIdentifier::TopicAlias,
        properties.topic_alias,
    )?;
    if let Some(value) = properties.maximum_qos {
        if value > 1 {
            return Err(Mqtt5ParseError::InvalidPropertyValue(
                PropertyIdentifier::MaximumQos.name(),
            ));
        }
        encode_optional_u8(
            &mut body,
            scope,
            PropertyIdentifier::MaximumQos,
            Some(value),
        )?;
    }
    encode_optional_bool(
        &mut body,
        scope,
        PropertyIdentifier::RetainAvailable,
        properties.retain_available,
    )?;
    for (key, value) in &properties.user_properties {
        write_property_id(&mut body, scope, PropertyIdentifier::UserProperty)?;
        encode_utf8_string(key, &mut body)?;
        encode_utf8_string(value, &mut body)?;
    }
    encode_optional_nonzero_u32(
        &mut body,
        scope,
        PropertyIdentifier::MaximumPacketSize,
        properties.maximum_packet_size,
    )?;
    encode_optional_bool(
        &mut body,
        scope,
        PropertyIdentifier::WildcardSubscriptionAvailable,
        properties.wildcard_subscription_available,
    )?;
    encode_optional_bool(
        &mut body,
        scope,
        PropertyIdentifier::SubscriptionIdentifierAvailable,
        properties.subscription_identifier_available,
    )?;
    encode_optional_bool(
        &mut body,
        scope,
        PropertyIdentifier::SharedSubscriptionAvailable,
        properties.shared_subscription_available,
    )?;

    encode_variable_byte_integer(body.len() as u32, buffer)?;
    buffer.extend_from_slice(&body);
    Ok(())
}

fn set_once<T>(
    field: &mut Option<T>,
    value: T,
    property_name: &'static str,
) -> Result<(), Mqtt5ParseError> {
    if field.is_some() {
        return Err(Mqtt5ParseError::DuplicateProperty(property_name));
    }
    *field = Some(value);
    Ok(())
}

fn ensure_allowed(
    identifier: PropertyIdentifier,
    scope: PropertyScope,
) -> Result<(), Mqtt5ParseError> {
    if identifier.allowed_on(scope) {
        Ok(())
    } else {
        Err(Mqtt5ParseError::PropertyNotAllowed {
            property: identifier.name(),
            scope: scope.name(),
        })
    }
}

fn write_property_id(
    body: &mut BytesMut,
    scope: PropertyScope,
    identifier: PropertyIdentifier,
) -> Result<(), Mqtt5ParseError> {
    ensure_allowed(identifier, scope)?;
    encode_variable_byte_integer(identifier.as_u32(), body)
}

fn encode_optional_u8(
    body: &mut BytesMut,
    scope: PropertyScope,
    identifier: PropertyIdentifier,
    value: Option<u8>,
) -> Result<(), Mqtt5ParseError> {
    if let Some(value) = value {
        write_property_id(body, scope, identifier)?;
        body.put_u8(value);
    }
    Ok(())
}

fn encode_optional_u16(
    body: &mut BytesMut,
    scope: PropertyScope,
    identifier: PropertyIdentifier,
    value: Option<u16>,
) -> Result<(), Mqtt5ParseError> {
    if let Some(value) = value {
        write_property_id(body, scope, identifier)?;
        body.put_u16(value);
    }
    Ok(())
}

fn encode_optional_nonzero_u16(
    body: &mut BytesMut,
    scope: PropertyScope,
    identifier: PropertyIdentifier,
    value: Option<u16>,
) -> Result<(), Mqtt5ParseError> {
    if let Some(0) = value {
        return Err(Mqtt5ParseError::InvalidPropertyValue(identifier.name()));
    }
    encode_optional_u16(body, scope, identifier, value)
}

fn encode_optional_u32(
    body: &mut BytesMut,
    scope: PropertyScope,
    identifier: PropertyIdentifier,
    value: Option<u32>,
) -> Result<(), Mqtt5ParseError> {
    if let Some(value) = value {
        write_property_id(body, scope, identifier)?;
        body.put_u32(value);
    }
    Ok(())
}

fn encode_optional_nonzero_u32(
    body: &mut BytesMut,
    scope: PropertyScope,
    identifier: PropertyIdentifier,
    value: Option<u32>,
) -> Result<(), Mqtt5ParseError> {
    if let Some(0) = value {
        return Err(Mqtt5ParseError::InvalidPropertyValue(identifier.name()));
    }
    encode_optional_u32(body, scope, identifier, value)
}

fn encode_optional_bool(
    body: &mut BytesMut,
    scope: PropertyScope,
    identifier: PropertyIdentifier,
    value: Option<bool>,
) -> Result<(), Mqtt5ParseError> {
    if let Some(value) = value {
        write_property_id(body, scope, identifier)?;
        body.put_u8(u8::from(value));
    }
    Ok(())
}

fn encode_optional_string(
    body: &mut BytesMut,
    scope: PropertyScope,
    identifier: PropertyIdentifier,
    value: Option<&String>,
) -> Result<(), Mqtt5ParseError> {
    if let Some(value) = value {
        write_property_id(body, scope, identifier)?;
        encode_utf8_string(value, body)?;
    }
    Ok(())
}

fn encode_optional_binary(
    body: &mut BytesMut,
    scope: PropertyScope,
    identifier: PropertyIdentifier,
    value: Option<&bytes::Bytes>,
) -> Result<(), Mqtt5ParseError> {
    if let Some(value) = value {
        write_property_id(body, scope, identifier)?;
        encode_binary(value, body)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_properties_parse_and_encode() {
        let bytes = [
            0x0f, 0x11, 0x00, 0x00, 0x00, 0x3c, 0x21, 0x00, 0x0a, 0x26, 0x00, 0x01, b'k', 0x00,
            0x01, b'v',
        ];

        let (remaining, properties) = parse(&bytes, PropertyScope::Connect).unwrap();

        assert!(remaining.is_empty());
        assert_eq!(properties.session_expiry_interval, Some(60));
        assert_eq!(properties.receive_maximum, Some(10));
        assert_eq!(
            properties.user_properties,
            vec![("k".to_string(), "v".to_string())]
        );

        let mut encoded = BytesMut::new();
        encode(&properties, PropertyScope::Connect, &mut encoded).unwrap();

        assert_eq!(encoded.to_vec(), bytes);
    }

    #[test]
    fn duplicate_single_use_property_is_rejected() {
        let bytes = [
            0x0a, 0x11, 0x00, 0x00, 0x00, 0x01, 0x11, 0x00, 0x00, 0x00, 0x02,
        ];

        let err = parse(&bytes, PropertyScope::Connect).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::DuplicateProperty("session_expiry_interval")
        );
    }

    #[test]
    fn unknown_property_identifier_is_rejected() {
        let err = parse(&[0x01, 0x7f], PropertyScope::Connect).unwrap_err();

        assert_eq!(err, Mqtt5ParseError::UnknownProperty(0x7f));
    }

    #[test]
    fn property_not_allowed_on_packet_is_rejected() {
        let err = parse(&[0x02, 0x01, 0x01], PropertyScope::Connect).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::PropertyNotAllowed {
                property: "payload_format_indicator",
                scope: "CONNECT"
            }
        );
    }

    #[test]
    fn invalid_boolean_property_value_is_rejected() {
        let err = parse(&[0x02, 0x17, 0x02], PropertyScope::Connect).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::InvalidPropertyValue("request_problem_information")
        );
    }

    #[test]
    fn publish_properties_support_repeated_subscription_identifiers() {
        let bytes = [0x04, 0x0b, 0x01, 0x0b, 0x02];

        let (_, properties) = parse(&bytes, PropertyScope::Publish).unwrap();

        assert_eq!(properties.subscription_identifiers, vec![1, 2]);
    }

    #[test]
    fn encode_rejects_property_for_wrong_scope() {
        let properties = Properties {
            response_topic: Some("reply/to".to_string()),
            ..Properties::default()
        };
        let mut encoded = BytesMut::new();

        let err = encode(&properties, PropertyScope::Connect, &mut encoded).unwrap_err();

        assert_eq!(
            err,
            Mqtt5ParseError::PropertyNotAllowed {
                property: "response_topic",
                scope: "CONNECT"
            }
        );
    }
}
