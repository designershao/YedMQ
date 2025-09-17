pub mod manager;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Hook {
    OnClientConnected,
    OnClientDisconnected,
    OnAuthenticate,
    OnAuthorize,
    OnMqttMessagePublish,
    OnSubscribe,
    OnStatsRequest,
}

pub fn get_hook_from_name(name: &str) -> Option<Hook> {
    match name {
        "on_client_connected" => Some(Hook::OnClientConnected),
        "on_client_disconnected" => Some(Hook::OnClientDisconnected),
        "on_authenticate" => Some(Hook::OnAuthenticate),
        "on_authorize" => Some(Hook::OnAuthorize),
        "on_mqtt_message_publish" => Some(Hook::OnMqttMessagePublish),
        "on_subscribe" => Some(Hook::OnSubscribe),
        "on_stats_request" => Some(Hook::OnStatsRequest),
        _ => None,
    }
}