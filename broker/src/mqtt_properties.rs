use std::collections::BTreeMap;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use prost_types::{value::Kind, ListValue, Struct, Value};
use yedmq_mqtt::packet::{Properties, RetainHandling, SubscribeTopic};

pub fn properties_to_struct(properties: &Properties) -> Option<Struct> {
    if properties == &Properties::default() {
        return None;
    }

    let mut fields = BTreeMap::new();

    insert_number(
        &mut fields,
        "payload_format_indicator",
        properties.payload_format_indicator,
    );
    insert_number(
        &mut fields,
        "message_expiry_interval",
        properties.message_expiry_interval,
    );
    insert_string(
        &mut fields,
        "content_type",
        properties.content_type.as_ref(),
    );
    insert_string(
        &mut fields,
        "response_topic",
        properties.response_topic.as_ref(),
    );
    insert_bytes_base64(
        &mut fields,
        "correlation_data_base64",
        properties.correlation_data.as_ref(),
    );
    if !properties.subscription_identifiers.is_empty() {
        fields.insert(
            "subscription_identifiers".to_string(),
            number_list_value(&properties.subscription_identifiers),
        );
    }
    insert_number(
        &mut fields,
        "session_expiry_interval",
        properties.session_expiry_interval,
    );
    insert_string(
        &mut fields,
        "assigned_client_identifier",
        properties.assigned_client_identifier.as_ref(),
    );
    insert_number(
        &mut fields,
        "server_keep_alive",
        properties.server_keep_alive,
    );
    insert_string(
        &mut fields,
        "authentication_method",
        properties.authentication_method.as_ref(),
    );
    insert_bytes_base64(
        &mut fields,
        "authentication_data_base64",
        properties.authentication_data.as_ref(),
    );
    insert_bool(
        &mut fields,
        "request_problem_information",
        properties.request_problem_information,
    );
    insert_number(
        &mut fields,
        "will_delay_interval",
        properties.will_delay_interval,
    );
    insert_bool(
        &mut fields,
        "request_response_information",
        properties.request_response_information,
    );
    insert_string(
        &mut fields,
        "response_information",
        properties.response_information.as_ref(),
    );
    insert_string(
        &mut fields,
        "server_reference",
        properties.server_reference.as_ref(),
    );
    insert_string(
        &mut fields,
        "reason_string",
        properties.reason_string.as_ref(),
    );
    insert_number(&mut fields, "receive_maximum", properties.receive_maximum);
    insert_number(
        &mut fields,
        "topic_alias_maximum",
        properties.topic_alias_maximum,
    );
    insert_number(&mut fields, "topic_alias", properties.topic_alias);
    insert_number(&mut fields, "maximum_qos", properties.maximum_qos);
    insert_bool(&mut fields, "retain_available", properties.retain_available);
    if !properties.user_properties.is_empty() {
        fields.insert(
            "user_properties".to_string(),
            user_properties_value(&properties.user_properties),
        );
    }
    insert_number(
        &mut fields,
        "maximum_packet_size",
        properties.maximum_packet_size,
    );
    insert_bool(
        &mut fields,
        "wildcard_subscription_available",
        properties.wildcard_subscription_available,
    );
    insert_bool(
        &mut fields,
        "subscription_identifier_available",
        properties.subscription_identifier_available,
    );
    insert_bool(
        &mut fields,
        "shared_subscription_available",
        properties.shared_subscription_available,
    );

    Some(Struct { fields })
}

pub fn subscribe_context(topic: &SubscribeTopic, properties: &Properties) -> Struct {
    let mut fields = BTreeMap::new();
    fields.insert("no_local".to_string(), bool_value(topic.no_local));
    fields.insert(
        "retain_as_published".to_string(),
        bool_value(topic.retain_as_published),
    );
    fields.insert(
        "retain_handling".to_string(),
        string_value(match topic.retain_handling {
            RetainHandling::SendAtSubscribe => "send_at_subscribe",
            RetainHandling::SendAtSubscribeIfNew => "send_at_subscribe_if_new",
            RetainHandling::DoNotSend => "do_not_send",
        }),
    );
    if let Some(properties) = properties_to_struct(properties) {
        fields.insert(
            "properties".to_string(),
            Value {
                kind: Some(Kind::StructValue(properties)),
            },
        );
    }
    Struct { fields }
}

fn insert_number<T>(fields: &mut BTreeMap<String, Value>, key: &str, value: Option<T>)
where
    T: Into<f64>,
{
    if let Some(value) = value {
        fields.insert(key.to_string(), number_value(value.into()));
    }
}

fn insert_string(fields: &mut BTreeMap<String, Value>, key: &str, value: Option<&String>) {
    if let Some(value) = value {
        fields.insert(key.to_string(), string_value(value));
    }
}

fn insert_bool(fields: &mut BTreeMap<String, Value>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        fields.insert(key.to_string(), bool_value(value));
    }
}

fn insert_bytes_base64(
    fields: &mut BTreeMap<String, Value>,
    key: &str,
    value: Option<&bytes::Bytes>,
) {
    if let Some(value) = value {
        fields.insert(key.to_string(), string_value(&STANDARD.encode(value)));
    }
}

fn number_list_value(values: &[u32]) -> Value {
    Value {
        kind: Some(Kind::ListValue(ListValue {
            values: values
                .iter()
                .map(|value| number_value(*value as f64))
                .collect(),
        })),
    }
}

fn user_properties_value(values: &[(String, String)]) -> Value {
    Value {
        kind: Some(Kind::ListValue(ListValue {
            values: values
                .iter()
                .map(|(key, value)| {
                    let mut fields = BTreeMap::new();
                    fields.insert("key".to_string(), string_value(key));
                    fields.insert("value".to_string(), string_value(value));
                    Value {
                        kind: Some(Kind::StructValue(Struct { fields })),
                    }
                })
                .collect(),
        })),
    }
}

fn number_value(value: f64) -> Value {
    Value {
        kind: Some(Kind::NumberValue(value)),
    }
}

fn string_value(value: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(value.to_string())),
    }
}

fn bool_value(value: bool) -> Value {
    Value {
        kind: Some(Kind::BoolValue(value)),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use prost_types::value::Kind;

    use super::*;

    #[test]
    fn properties_to_struct_uses_stable_keys() {
        let properties = Properties {
            content_type: Some("text/plain".to_string()),
            correlation_data: Some(Bytes::from_static(b"abc")),
            user_properties: vec![("k".to_string(), "v".to_string())],
            ..Properties::default()
        };

        let value = properties_to_struct(&properties).expect("struct");

        assert!(value.fields.contains_key("content_type"));
        assert_eq!(
            value.fields["correlation_data_base64"].kind,
            Some(Kind::StringValue("YWJj".to_string()))
        );
        assert!(value.fields.contains_key("user_properties"));
    }

    #[test]
    fn subscribe_context_contains_options_and_properties() {
        let topic = SubscribeTopic {
            topic_filter: "a/b".to_string(),
            qos: 1,
            no_local: true,
            retain_as_published: true,
            retain_handling: RetainHandling::DoNotSend,
        };
        let properties = Properties {
            user_properties: vec![("k".to_string(), "v".to_string())],
            ..Properties::default()
        };

        let context = subscribe_context(&topic, &properties);

        assert_eq!(context.fields["no_local"].kind, Some(Kind::BoolValue(true)));
        assert_eq!(
            context.fields["retain_handling"].kind,
            Some(Kind::StringValue("do_not_send".to_string()))
        );
        assert!(context.fields.contains_key("properties"));
    }
}
