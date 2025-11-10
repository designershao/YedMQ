use config::{Config, ConfigError, File};
use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct Settings {
    pub session: Session,
    pub plugin: Plugin,
    pub listener: Listener,
    pub mqtt: Mqtt,
    pub cluster: Cluster
}

#[derive(Debug, PartialEq, Default)]
pub enum DefaultAuthenticationValue {
    #[default]
    Allow,
    Deny
}

impl<'de> Deserialize<'de> for DefaultAuthenticationValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "allow" => Ok(DefaultAuthenticationValue::Allow),
            "deny" => Ok(DefaultAuthenticationValue::Deny),
            _ => Err(serde::de::Error::custom(format!("invalid value: {}", s))),
        }
    }
}

#[derive(Debug, PartialEq, Default)]
pub enum DefaultAuthorizationValue {
    #[default]
    Allow,
    Deny
}

impl<'de> Deserialize<'de> for DefaultAuthorizationValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "allow" => Ok(DefaultAuthorizationValue::Allow),
            "deny" => Ok(DefaultAuthorizationValue::Deny),
            _ => Err(serde::de::Error::custom(format!("invalid value: {}", s))),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Mqtt {
    pub sys_topic_interval_secs: u64, // system topic interval seconds
    pub inflight_retry_interval_secs: u64, // inflight retry interval seconds
    pub max_message_size: u32,
    pub default_authentication: DefaultAuthenticationValue,
    pub default_authorization: DefaultAuthorizationValue,
}

impl Default for Mqtt {
    fn default() -> Self {
        Self {
            sys_topic_interval_secs: 10,
            max_message_size: yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE, // The maximum allowed message size is 256 MB
            default_authentication: DefaultAuthenticationValue::default(),
            default_authorization: DefaultAuthorizationValue::default(),
            inflight_retry_interval_secs: 10
        }
    }
}

#[derive(Debug,Deserialize)]
pub struct Session {
    pub qos_expired_secs: u64, // qos context expired seconds
    pub packet_resend_interval_secs: u64, // session packet resend interval seconds
    pub session_clock_path: String, // session clock path 
}

impl Default for Session {
    fn default() -> Self {
        Self {
            qos_expired_secs: 10,
            packet_resend_interval_secs: 10,
            session_clock_path: "./clock".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Plugin {
    pub dir: String, // plugin dir path

    pub local_socket_path: String, // local socket path to communicate with plugin host

    pub default_authorize_result: bool, // Default result for authorize calls when no plugin is loaded

    pub default_authenticate_result: bool, // Default result for authenticate calls when no plugin is loaded
}

impl Default for Plugin {
    fn default() -> Self {
        Self {
            dir: "./plugins".to_string(),
            default_authenticate_result: true,
            default_authorize_result: true,
            local_socket_path: "/tmp/yedmq_plugin_host.sock".to_string()
        }
    }
}

#[derive(Debug, Deserialize,Default)]
pub struct Listener {
    pub tcp: Tcp,
    pub tcp_tls: TcpTls,
    pub ws: Ws,
    pub wss: Wss,
    pub api: Api
}

#[derive(Debug, Deserialize)]
pub struct TcpTls {
    pub external: String,
    pub cert_file: String,
    pub key_file: String
}

impl Default for TcpTls {
    fn default() -> Self {
        Self {
            external: "0.0.0.0:8883".to_string(),
            cert_file: "".to_string(),
            key_file: "".to_string()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Ws {
    pub external: String
}

impl Default for Ws {
    fn default() -> Self {
        Self {
            external: "0.0.0.0:8083".to_string()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Wss {
    pub external: String,
    pub cert_file: String,
    pub key_file: String
}

impl Default for Wss {
    fn default() -> Self {
        Self {
            external: "0.0.0.0:8084".to_string(),
            cert_file: "".to_string(),
            key_file: "".to_string()
        }
    }
    
}

#[derive(Debug, Deserialize)]
pub struct Tcp {
    pub external: String
}

impl Default for Tcp {
    fn default() -> Self {
        Self {
            external: "0.0.0.0:1883".to_string()
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Cluster {

    pub cluster_name: String,

    pub heartbeat_interval: u32,

    pub node_id: u64,

    pub store_dir: String,

    pub rpc: RPC,

    pub nodes: Vec<Node>,

    pub session_ttl: u64

}

#[derive(Debug, Deserialize, Clone)]
pub struct Node {
    pub id: u64,
    pub rpc_address: String,
    pub api_address: String,
}


impl Default for Cluster {

    fn default() -> Self {
        let rpc = RPC::default();
        Self {
            cluster_name: "YedMQ".to_string(),
            heartbeat_interval: 10,
            store_dir: "./store".to_string(),
            node_id: 1,
            rpc: rpc.clone(),
            nodes: vec![],
            session_ttl: 10
        }
    }
}


#[derive(Debug, Deserialize,Clone)]
pub struct RPC {
    pub external: String
}

impl Default for RPC  {
    fn default() -> Self {
        Self {
            external: "0.0.0.0:3457".to_string()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Api {

    pub external: String,

    pub auth: AuthConfig

}


#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    pub users: Vec<User>
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            users: vec![]
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct User {
    pub username: String,
    pub password: String,
}


impl Default for Api {
    fn default() -> Self {
        Self {
            external: "0.0.0.0:3456".to_string(),
            auth: AuthConfig::default()
        }
    }
}


impl Settings {

    pub fn new() -> Result<Self, ConfigError> {
        let s = Config::builder()
            .set_default("mqtt.default_authentication", "allow")?
            .set_default("mqtt.default_authorization", "allow")?
            .set_default("mqtt.sys_topic_interval_secs", 10)? 
            .set_default("session.qos_expired_secs", 10)?
            .set_default("session.packet_resend_interval_secs", 10)?
            .set_default("session.session_clock_path", "./clock")?
            .set_default("plugin.dir", "./plugins")?
            .set_default("listener.tcp.external", "0.0.0.0:1883")?
            .set_default("listener.tcp_tls.external", "0.0.0.0:8883")?
            .set_default("listener.tcp_tls.cert_file", "")?
            .set_default("listener.tcp_tls.key_file", "")?
            .set_default("listener.ws.external", "0.0.0.0:8083")?
            .set_default("listener.wss.external", "0.0.0.0:8084")?
            .set_default("listener.wss.cert_file", "")?
            .set_default("listener.wss.key_file", "")?
            .set_default("listener.api.external", "0.0.0.0:3456")?
            .set_default("cluster.cluster_name", "YedMQ")?  
            .set_default("cluster.heartbeat_interval", 10)?
            .set_default("cluster.rpc.external", "0.0.0.0:3457")?
            .set_default("cluster.session_ttl", 10)?
            .add_source(
                File::with_name("/etc/yedmq/config.toml").required(false)
            )
            .add_source(
                File::with_name("./yedmq.toml")
            )
            .build()?;
        s.try_deserialize()
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settings_load() {
        let s = Settings::new().unwrap();
        assert_eq!(10, s.session.qos_expired_secs);
        assert_eq!(10, s.session.packet_resend_interval_secs);
        assert_eq!("0.0.0.0:1883", s.listener.tcp.external);
        assert_eq!(DefaultAuthenticationValue::Allow, s.mqtt.default_authentication);
        assert_eq!(DefaultAuthorizationValue::Allow, s.mqtt.default_authorization);
        assert_eq!(1001, s.cluster.nodes[0].id);
    }
}