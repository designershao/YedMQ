use std::fs;

use anyhow::anyhow;
use log::{info, warn};
use r2d2_postgres::{postgres::NoTls, r2d2, PostgresConnectionManager};
use serde::Deserialize;
use yedmq_plugin::{
    plugin::{AuthenticationResultValue, Plugin},
    register_plugin,
};

pub struct AclPostgresql {
    pub connection_pool: Option<r2d2_postgres::r2d2::Pool<PostgresConnectionManager<NoTls>>>,
}

#[derive(Deserialize)]
struct Config {
    postgresql: PostgresqlConfig,
}

#[derive(Deserialize)]
struct PostgresqlConfig {
    db_url: String,
    tls_mode: bool,
}

impl AclPostgresql {
    pub fn new() -> std::result::Result<AclPostgresql, anyhow::Error> {
        Ok(
            AclPostgresql {
                connection_pool: None,
            }
        )
    }
}

impl Plugin for AclPostgresql {
    fn on_activate(&mut self) -> anyhow::Result<()> {
        env_logger::init();
        let config_content = fs::read_to_string("./plugins/acl_postgresql/acl_postgresql.toml");
        if let Err(e) = config_content {
            return Err(anyhow!("load acl rule file error: {}", e));
        } else {
            let config = toml::from_str::<Config>(config_content.unwrap().as_str());

            if let Err(e) = config {
                return Err(anyhow!("load acl rule file error: {}", e));
            } else {
                let config = config.unwrap();
                let tls_mode = match config.postgresql.tls_mode {
                    true => NoTls,
                    false => NoTls,
                };
                let manager = PostgresConnectionManager::new(
                    config.postgresql.db_url.as_str().parse().unwrap(),
                    tls_mode,
                );
                let pool = r2d2::Pool::new(manager).unwrap();
                self.connection_pool = Some(pool);
            }
        }
        Ok(())
    }

    fn on_deactivate(&self) -> anyhow::Result<()> {
        info!("acl_postgresql plugin on_deactivate");
        Ok(())
    }

    fn connect_authenticate(
        &self,
        packet: &yedmq_mqtt::v3::connect::ConnectPacket,
    ) -> anyhow::Result<yedmq_plugin::plugin::AuthenticationResult> {
        if self.connection_pool.is_some() {
            let client_option = self.connection_pool.clone().unwrap().get();
            if client_option.is_err() {
                warn!(
                    "get postgresql connection from pool error: {}",
                    client_option.err().unwrap()
                );
                return Ok(yedmq_plugin::plugin::AuthenticationResult::Next());
            }
            let mut client = client_option.unwrap();
            let empty_string = String::from("");
            let select_query = client.query_one(
                "SELECT id, username, password, tenant FROM users WHERE username = $1 AND password = $2",
                &[&packet.payload.username.as_ref().unwrap_or_else(|| &empty_string), &packet.payload.password.as_ref().unwrap_or_else(|| &empty_string)],
            );
            if let Err(e) = select_query {
                return Err(anyhow!("query user error: {}", e));
            } else {
                let users_result = select_query;
                if let Err(e) = users_result {
                    return Err(anyhow!("query user error: {}", e));
                } else {
                    let users_row = users_result.unwrap();
                    if users_row.is_empty() {
                        return Ok(yedmq_plugin::plugin::AuthenticationResult::Next());
                    } else {
                        let tenant: &str = users_row.get(3);
                        return Ok(yedmq_plugin::plugin::AuthenticationResult::Result(
                            AuthenticationResultValue::Success(tenant.to_string()),
                        ));
                    }
                }
            }
        } else {
            return Ok(yedmq_plugin::plugin::AuthenticationResult::Next());
        }
    }

    fn on_publish(
        &self,
        _client: &yedmq_plugin::plugin::Client,
        _packet: &yedmq_mqtt::v3::publish::PublishPacket,
    ) {
    }

    fn on_disconnect(&self, _client: &yedmq_plugin::plugin::Client) {}

    fn authorizate_acl_check(
        &self,
        client: &yedmq_plugin::plugin::Client,
        topic: &String,
        action: yedmq_plugin::plugin::Action,
    ) -> anyhow::Result<yedmq_plugin::plugin::AuthorizationResult> {
        let action_params = match action {
            yedmq_plugin::plugin::Action::Publish => "publish",
            yedmq_plugin::plugin::Action::Subscribe => "subscribe",
        };
        if self.connection_pool.is_some() {
            let client_option = self.connection_pool.clone().unwrap().get();
            if client_option.is_err() {
                warn!(
                    "get postgresql connection from pool error: {}",
                    client_option.err().unwrap()
                );
                return Ok(yedmq_plugin::plugin::AuthorizationResult::Next());
            }
            let mut db_client = client_option.unwrap();
            let empty_string = String::from("");
            let select_query = db_client.query_one(
                "SELECT result FROM acls WHERE username = $1 AND topic = $2 AND action = $3",
                &[
                    &client
                        .properties
                        .username
                        .as_ref()
                        .unwrap_or_else(|| &empty_string),
                    &topic,
                    &action_params,
                ],
            );
            if let Err(e) = select_query {
                return Err(anyhow!("query acl error: {}", e));
            } else {
                let result = select_query;
                if let Err(e) = result {
                    return Err(anyhow!("query acl error: {}", e));
                }
                let row = result.unwrap();
                if row.is_empty() {
                    return Ok(yedmq_plugin::plugin::AuthorizationResult::Next());
                } else {
                    let result: &str = row.get(0);
                    if result == "allow" {
                        return Ok(yedmq_plugin::plugin::AuthorizationResult::Result(true));
                    } else {
                        return Ok(yedmq_plugin::plugin::AuthorizationResult::Result(false));
                    }
                }
            }
        } else {
            return Ok(yedmq_plugin::plugin::AuthorizationResult::Next());
        }
    }
}

register_plugin!(AclPostgresql, AclPostgresql::new);
