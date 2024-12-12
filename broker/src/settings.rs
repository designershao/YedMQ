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

#[derive(Debug, Deserialize, Default)]
pub struct Mqtt {
    pub sys_topic_interval_secs: u64, // system topic interval seconds
    pub default_authentication: DefaultAuthenticationValue,
    pub default_authorization: DefaultAuthorizationValue,
}

#[derive(Debug,Deserialize,Default)]
pub struct Session {
    pub qos_expired_secs: u64, // qos context expired seconds
    pub packet_resend_interval_secs: u64 // session packet resend interval seconds
}

#[derive(Debug, Deserialize,Default)]
pub struct Plugin {
    pub dir: String // plugin dir path
}

#[derive(Debug, Deserialize,Default)]
pub struct Listener {
    pub tcp: Tcp,
    pub tcp_tls: TcpTls,
    pub ws: Ws,
    pub wss: Wss,
    pub api: Api
}

#[derive(Debug, Deserialize, Default)]
pub struct TcpTls {
    pub external: String,
    pub cert_file: String,
    pub key_file: String
}

#[derive(Debug, Deserialize, Default)]
pub struct Ws {
    pub external: String
}

#[derive(Debug, Deserialize, Default)]
pub struct Wss {
    pub external: String,
    pub cert_file: String,
    pub key_file: String
}

#[derive(Debug, Deserialize, Default)]
pub struct Tcp {
    pub external: String
}

#[derive(Debug, Deserialize, Default)]
pub struct Api {
    pub external: String
}


impl Settings {

    pub fn new() -> Result<Self, ConfigError> {
        let s = Config::builder()
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