use crate::protocol::plugin_protocol::InitializeRequest;

pub mod protocol_frame;
pub mod plugin_protocol;

pub const INIT_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.InitializeRequest";

pub const INIT_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.InitializeResponse";

pub const AUTHENTICATE_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.AuthenticateRequest";

pub const AUTHENTICATE_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.AuthenticateResponse";

pub const AUTHORIZE_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.AuthorizeRequest";

pub const AUTHORIZE_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.AuthorizeResponse";

pub const MQTT_MESSAGE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.MqttMessage";

pub const MQTT_MESSAGE_PUBLISH_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.MqttMessagePublishRequest";

pub const MQTT_MESSAGE_PUBLISH_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.MqttMessagePublishResponse";

pub const SUBSCRIBE_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.SubscribeRequest";

pub const SUBSCRIBE_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.SubscribeResponse";

pub const CLIENT_CONNECTED_EVENT_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.ClientConnectedEvent";

pub const CLIENT_DISCONNECTED_EVENT_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.ClientDisconnectedEvent";

pub const STATS_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.StatsRequest";

pub const STATS_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.StatsResponse";

pub const BATCH_REQUEST_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.BatchRequest";

pub const BATCH_RESPONSE_TYPE_URL: &str = "type.yedmq.com/plugin_protocol.BatchResponse";

impl InitializeRequest {
    pub fn new_from_plugin_host_config(config: &crate::plugin_host_config::PluginHostConfig) -> Self {
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
