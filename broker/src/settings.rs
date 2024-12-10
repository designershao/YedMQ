use config::{Config, ConfigError, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub session: Session,
    pub plugin: Plugin,
    pub listener: Listener,
    pub mqtt: Mqtt,
}

#[derive(Debug, Deserialize)]
pub struct Mqtt {
    pub sys_topic_interval_secs: u64, // system topic interval seconds
}

#[derive(Debug,Deserialize)]
pub struct Session {
    pub qos_expired_secs: u64, // qos context expired seconds
    pub packet_resend_interval_secs: u64 // session packet resend interval seconds
}

#[derive(Debug, Deserialize)]
pub struct Plugin {
    pub dir: String // plugin dir path
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
pub struct Ws {
    pub external: String
}

#[derive(Debug, Deserialize)]
pub struct Wss {
    pub external: String,
    pub cert_file: String,
    pub key_file: String
}

#[derive(Debug, Deserialize)]
pub struct Tcp {
    pub external: String
}

#[derive(Debug, Deserialize)]
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
    }
}