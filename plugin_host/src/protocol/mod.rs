use std::collections::HashMap;

use crate::{create_message_id, protocol::plugin_protocol::InitializeRequest};

pub mod plugin_protocol;
pub mod protocol_frame;

pub const INIT_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.InitializeRequest";

pub const INIT_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.InitializeResponse";

pub const AUTHENTICATE_REQUEST_TYPE_URL: &str =
    "type.yedmq.com/plugin_protocol.AuthenticateRequest";

pub const AUTHENTICATE_RESPONSE_TYPE_URL: &str =
    "type.yedmq.com/plugin_protocol.AuthenticateResponse";

pub const AUTHORIZE_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.AuthorizeRequest";

pub const AUTHORIZE_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.AuthorizeResponse";

pub const MQTT_MESSAGE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.MqttMessage";

pub const MQTT_MESSAGE_PUBLISH_REQUEST_TYPE_URL: &str =
    "type.yedmq.com/plugin_protocol.MqttMessagePublishRequest";

pub const MQTT_MESSAGE_PUBLISH_RESPONSE_TYPE_URL: &str =
    "type.yedmq.com/plugin_protocol.MqttMessagePublishResponse";

pub const SUBSCRIBE_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.SubscribeRequest";

pub const SUBSCRIBE_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.SubscribeResponse";

pub const CLIENT_CONNECTED_EVENT_TYPE_URL: &str =
    "type.yedmq.com/plugin_protocol.ClientConnectedEvent";

pub const CLIENT_DISCONNECTED_EVENT_TYPE_URL: &str =
    "type.yedmq.com/plugin_protocol.ClientDisconnectedEvent";

pub const STATS_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.StatsRequest";

pub const STATS_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.StatsResponse";

pub const BATCH_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.BatchRequest";

pub const BATCH_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.BatchResponse";

pub const PING_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.PingRequest";

pub const PING_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.PingResponse";

impl InitializeRequest {
    pub fn new_from_plugin_host_config(
        config: &crate::plugin_host_config::PluginHostConfig,
    ) -> Self {
        InitializeRequest {
            broker_info: Some(crate::protocol::plugin_protocol::BrokerInfo {
                version: config.broker_version.clone(),
                node_id: config.broker_node_id as i32,
                cluster_name: config.cluster_name.clone(),
                properties: None,
            }),
            plugin_config: None,
            required_capabilities: vec![],
        }
    }
}

pub struct ProtocolMessageBuilder {
    message: crate::protocol::plugin_protocol::ProtocolMessage,
}

impl Default for ProtocolMessageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtocolMessageBuilder {
    pub fn new() -> Self {
        ProtocolMessageBuilder {
            message: crate::protocol::plugin_protocol::ProtocolMessage {
                id: create_message_id(),
                version: "1.0.0".to_string(),
                r#type: crate::protocol::plugin_protocol::MessageType::Request.into(),
                timestamp: Some(crate::create_timestamp()),
                source: "plugin_host".to_string(),
                target: "plugin".to_string(),
                method: None,
                params: None,
                result: None,
                error: None,
                metadata: HashMap::new(),
            },
        }
    }

    pub fn with_type(mut self, msg_type: crate::protocol::plugin_protocol::MessageType) -> Self {
        self.message.r#type = msg_type.into();
        self
    }

    pub fn with_method(mut self, method: crate::protocol::plugin_protocol::Method) -> Self {
        self.message.method = Some(method.into());
        self
    }

    pub fn with_params(mut self, params: prost_types::Any) -> Self {
        self.message.params = Some(params);
        self
    }

    pub fn with_result(mut self, result: prost_types::Any) -> Self {
        self.message.result = Some(result);
        self
    }

    pub fn build(self) -> crate::protocol::plugin_protocol::ProtocolMessage {
        self.message
    }
}
