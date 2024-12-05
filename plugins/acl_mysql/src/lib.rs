use std::fs;

use log::info;
use mysql::{params, prelude::Queryable, Pool};
use samoye_plugin::{plugin::{AuthenticationResult, AuthenticationResultValue, Plugin}, register_plugin};
use anyhow::{Result, anyhow};
use serde::Deserialize;

pub struct AclMySql{
    connection_pool: Option<Pool>
}

#[derive(Deserialize)]
struct Config {
    mysql: MySqlConfig
}

#[derive(Deserialize)]
struct MySqlConfig {
    db_url: String
}

impl AclMySql{

    pub fn new() -> AclMySql {
        AclMySql {
            connection_pool: None
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct User {
    id: i32,
    username: Option<String>,
    password: Option<String>,
    tenant: Option<String>
}

impl Plugin for AclMySql {
    fn on_activate(&mut self) -> Result<()> {
        let config_content = fs::read_to_string("./plugins/acl_mysql/acl_mysql.toml");
        if let Err(e) = config_content {
            return Err(anyhow!("load acl rule file error: {}", e));
        } else {
            let config = toml::from_str::<Config>(config_content.unwrap().as_str());

            if let Err(e) = config {
                return Err(anyhow!("load acl rule file error: {}", e));
            } else {
                let config = config.unwrap();
                let pool = mysql::Pool::new(config.mysql.db_url.as_str()).unwrap();
                self.connection_pool = Some(pool);
            }
        }
        Ok(())
    }

    fn on_deactivate(&self) -> Result<()> {
        info!("acl_mysql plugin on_deactivate");
        Ok(())
    }

    fn connect_authenticate(&self, packet: &samoye_mqtt::v3::connect::ConnectPacket) -> anyhow::Result<samoye_plugin::plugin::AuthenticationResult> {

        if let Some(pool) = &self.connection_pool {
            let mut conn = pool.get_conn().unwrap();

            let select_query=  conn.exec_first(
                "SELECT id, username, password, tenant FROM users WHERE username = :username AND password = :password",
                params! {
                    "username" => packet.payload.username.as_ref().unwrap(),
                    "password" => packet.payload.password.as_ref().unwrap(),
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
                if users_option.is_none(){
                    Ok(AuthenticationResult::Next())
                } else {
                    let user = users_option.unwrap();
                    if user.tenant.is_none() {
                        Ok(AuthenticationResult::Result(AuthenticationResultValue::Success("public".into())))
                    } else {
                        Ok(AuthenticationResult::Result(AuthenticationResultValue::Success(user.tenant.as_ref().unwrap().into())))
                    }
                }
            }
        } else {
            Ok(AuthenticationResult::Next())
        }
    }

    fn on_publish(&self, _client: &samoye_plugin::plugin::Client, _packet: &samoye_mqtt::v3::publish::PublishPacket) {
        // Do nothing
    }

    fn on_disconnect(&self, _client: &samoye_plugin::plugin::Client) {
        // Do nothing
    }

    fn authorizate_acl_check(&self, client: &samoye_plugin::plugin::Client, topic: &String, action: samoye_plugin::plugin::Action) -> anyhow::Result<samoye_plugin::plugin::AuthorizationResult> {
        if let Some(pool) = &self.connection_pool {
            let action_params = match action {
                samoye_plugin::plugin::Action::Publish => "publish",
                samoye_plugin::plugin::Action::Subscribe => "subscribe",
            };
            let conn_result = pool.get_conn();

            if let Err(e) = conn_result {
                return Err(anyhow!("get connection error: {}", e));
            }
            let mut conn = conn_result.unwrap();

            let select_query=  conn.exec_first(
                "SELECT result FROM acls WHERE username = :username AND topic = :topic AND action = :action",
                params! {
                    "username" => client.properties.username.as_ref().unwrap(),
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
                    return Ok(samoye_plugin::plugin::AuthorizationResult::Next());
                } else {
                    let result = result.unwrap();
                    if result == "allow" {
                        return Ok(samoye_plugin::plugin::AuthorizationResult::Result(true));
                    } else {
                        return Ok(samoye_plugin::plugin::AuthorizationResult::Result(false));
                    }
                }
            }
        }else {
            return Ok(samoye_plugin::plugin::AuthorizationResult::Next());
        }
    }
}

impl Drop for AclMySql {
    fn drop(&mut self) {
        info!("acl_mysql plugin drop");
    }
}

register_plugin!(AclMySql, AclMySql::new);