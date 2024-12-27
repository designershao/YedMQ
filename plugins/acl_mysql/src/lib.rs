use std::{fs, path::Path};

use anyhow::{anyhow, Result};
use log::{debug, info, warn};
use mysql::{params, prelude::Queryable, Pool};
use serde::Deserialize;
use yedmq_plugin::{
    plugin::{AuthenticationResult, AuthenticationResultValue, Plugin},
    register_plugin,
};

pub struct AclMySql {
    connection_pool: Pool,
}

#[derive(Deserialize)]
struct Config {
    mysql: MySqlConfig,
}

#[derive(Deserialize)]
struct MySqlConfig {
    db_url: String,
}

impl AclMySql {
    pub fn new(context: yedmq_plugin::context::Context) -> std::result::Result<AclMySql, anyhow::Error> {
        env_logger::init();
        let root_path = Path::new(context.get_current_plugin_dir());
        let mysql_config_file = root_path.join("acl_mysql.toml");
        if !mysql_config_file.exists() {
            return Err(anyhow!("acl_mysql.toml config file not exist"));
        }
        
        let config_content = fs::read_to_string(&mysql_config_file);
        if let Err(e) = config_content {
            warn!("load acl rule file error: {}", e);
            return Err(anyhow!("load acl rule file error: {}", e));
        } else {
            let config = toml::from_str::<Config>(config_content.unwrap().as_str());

            if let Err(e) = config {
                warn!("load acl rule file error: {}", e);
                return Err(anyhow!("load acl rule file error: {}", e));
            } else {
                let config = config.unwrap();
                let pool_result = mysql::Pool::new(config.mysql.db_url.as_str());
                if let Err(e) = pool_result {
                    warn!("create mysql pool error: {}", e);
                    return Err(anyhow!("create mysql pool error: {}", e));
                } else {
                    Ok(AclMySql {
                        connection_pool: pool_result.unwrap(),
                    })
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct User {
    id: i32,
    username: Option<String>,
    password: Option<String>,
    tenant: Option<String>,
}

impl Plugin for AclMySql {
    fn on_activate(&mut self) -> Result<()> {
        Ok(())
    }

    fn on_deactivate(&self) -> Result<()> {
        info!("acl_mysql plugin on_deactivate");
        Ok(())
    }

    fn connect_authenticate(
        &self,
        packet: &yedmq_mqtt::v3::connect::ConnectPacket,
    ) -> anyhow::Result<yedmq_plugin::plugin::AuthenticationResult> {
        let mut conn = self.connection_pool.get_conn().unwrap();

        let empty_string = String::from("");

        let select_query=  conn.exec_first(
                "SELECT id, username, password, tenant FROM users WHERE username = :username AND password = :password",
                params! {
                    "username" => packet.payload.username.as_ref().unwrap_or_else(|| &empty_string),
                    "password" => packet.payload.password.as_ref().unwrap_or_else(|| &empty_string),
                }
            )
            .map(|row| {
                row.map(|(id, username, password, tenant)| User {
                    id,
                    username,
                    password,
                    tenant
                })
            });

        if let Err(e) = select_query {
            return Err(anyhow!("query user error: {}", e));
        } else {
            let users_option = select_query.unwrap();
            if users_option.is_none() {
                debug!("user not found, skip to next plugin");
                Ok(AuthenticationResult::Next())
            } else {
                let user = users_option.unwrap();
                if user.tenant.is_none() {
                    Ok(AuthenticationResult::Result(
                        AuthenticationResultValue::Success("public".into()),
                    ))
                } else {
                    Ok(AuthenticationResult::Result(
                        AuthenticationResultValue::Success(user.tenant.as_ref().unwrap().into()),
                    ))
                }
            }
        }
    }

    fn on_publish(
        &self,
        _client: &yedmq_plugin::plugin::Client,
        _packet: &yedmq_mqtt::v3::publish::PublishPacket,
    ) {
        // Do nothing
    }

    fn on_disconnect(&self, _client: &yedmq_plugin::plugin::Client) {
        // Do nothing
    }

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
        let conn_result = self.connection_pool.get_conn();

        if let Err(e) = conn_result {
            return Err(anyhow!("get connection error: {}", e));
        }
        let mut conn = conn_result.unwrap();

        let empty_string = String::from("");

        let select_query=  conn.exec_first(
                "SELECT result FROM acls WHERE username = :username AND topic = :topic AND action = :action",
                params! {
                    "username" => client.properties.username.as_ref().unwrap_or_else(|| &empty_string),
                    "topic" => topic,
                    "action" => action_params
                }
            ).map(|row:Option<String>|{
                row.map(|result| result)
            });

        if let Err(e) = select_query {
            return Err(anyhow!("query acl error: {}", e));
        } else {
            let result = select_query.unwrap();
            if result.is_none() {
                debug!("acl not found, skip to next plugin");
                return Ok(yedmq_plugin::plugin::AuthorizationResult::Next());
            } else {
                let result = result.unwrap();
                if result == "allow" {
                    return Ok(yedmq_plugin::plugin::AuthorizationResult::Result(true));
                } else {
                    return Ok(yedmq_plugin::plugin::AuthorizationResult::Result(false));
                }
            }
        }
    }
}

impl Drop for AclMySql {
    fn drop(&mut self) {
        info!("acl_mysql plugin drop");
    }
}

register_plugin!(AclMySql, AclMySql::new);
