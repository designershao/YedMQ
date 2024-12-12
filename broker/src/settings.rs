use config::{Config, ConfigError, File};
use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct Settings {
    pub session: Session,
    pub plugin: Plugin,
    pub listener: Listener,
    pub mqtt: Mqtt,
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
    pub default_authentication: DefaultAuthenticationValue,
    pub default_authorization: DefaultAuthorizationValue,
}

impl Default for Mqtt {
    fn default() -> Self {
        Self {
            sys_topic_interval_secs: 10,
            default_authentication: DefaultAuthenticationValue::default(),
            default_authorization: DefaultAuthorizationValue::default()
        }
    }
}

#[derive(Debug,Deserialize)]
pub struct Session {
    pub qos_expired_secs: u64, // qos context expired seconds
    pub packet_resend_interval_secs: u64 // session packet resend interval seconds
}

impl Default for Session {
    fn default() -> Self {
        Self {
            qos_expired_secs: 10,
            packet_resend_interval_secs: 10
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Plugin {
    pub dir: String // plugin dir path
}

impl Default for Plugin {
    fn default() -> Self {
        Self {
            dir: "./plugins".to_string()
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

#[derive(Debug, Deserialize)]
pub struct Api {
    pub external: String
}

impl Default for Api {
    fn default() -> Self {
        Self {
            external: "0.0.0.0:3456".to_string()
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
            .add_source(
                File::with_name("/etc/samoye/config.toml").required(false)
            )
            .add_source(
                File::with_name("./samoye.toml")
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
    }
}