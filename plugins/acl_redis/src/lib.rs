use anyhow::{anyhow, Ok};
use log::{info, warn};
use redis::Commands;
use serde::Deserialize;
use std::{fs, path::Path};
use yedmq_plugin::{
    plugin::{AuthenticationResultValue, ConnectReturnCode, Plugin},
    register_plugin,
};

pub struct AclRedis {
    pub connection_pool: r2d2::Pool<redis::Client>,
}

#[derive(Deserialize)]
struct Config {
    redis: RedisConfig,
}

#[derive(Deserialize)]
struct RedisConfig {
    db_url: String,
}

impl AclRedis {
    pub fn new(
        context: yedmq_plugin::context::Context,
    ) -> std::result::Result<AclRedis, anyhow::Error> {
        env_logger::init();
        let root_path = Path::new(context.get_current_plugin_dir());
        let redis_config_file = root_path.join("acl_redis.toml");

        if !redis_config_file.exists() {
            return Err(anyhow!("acl_redis.toml config file not exist"));
        }

        let config_content = fs::read_to_string(&redis_config_file);
        if let Err(e) = config_content {
            return Err(anyhow!("load plugin config file error: {}", e));
        } else {
            let config = toml::from_str::<Config>(config_content.unwrap().as_str());
            if let Err(e) = config {
                return Err(anyhow!("load plugin config file error: {}", e));
            } else {
                let config = config.unwrap();
                let clinet_result = redis::Client::open(config.redis.db_url.as_str());
                if let Err(e) = clinet_result  {
                   warn!("create redis client error: {}", e);
                   return Err(anyhow!("create redis client error: {}", e)); 
                }
                let connection_result = clinet_result.as_ref().unwrap().get_connection();
                if let Err(e) = connection_result {
                    warn!("create redis connection error: {}", e);
                    return Err(anyhow!("create redis connection error: {}", e));
                }
            
                let connection_pool =
                    r2d2::Pool::new(clinet_result.unwrap());
                if let Err(e) = connection_pool {
                    warn!("create redis pool error: {}", e);
                    return Err(anyhow!("create redis pool error: {}", e));
                }
                Ok(AclRedis {
                    connection_pool: connection_pool.unwrap(),
                })
            }
        }
    }
}

impl Plugin for AclRedis {
    fn on_activate(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn on_deactivate(&self) -> anyhow::Result<()> {
        info!("acl_redis plugin on_deactivate");
        Ok(())
    }

    // connect_authenticate
    // add user:
    // username is yedmq, password is 123456, tenant id is tenant_1
    // ```
    // HSET mqtt_user:yedmq passowrd 123456 tenant_id tenant_1
    // ```
    fn connect_authenticate(
        &self,
        packet: &yedmq_mqtt::v3::connect::ConnectPacket,
    ) -> anyhow::Result<yedmq_plugin::plugin::AuthenticationResult> {
        if let std::result::Result::Ok(mut connection) = self.connection_pool.get() {
            let key = format!(
                "mqtt_user:{}",
                packet
                    .payload
                    .username
                    .as_ref()
                    .unwrap_or(&String::from(""))
            );
            let value = connection.hgetall(key);
            if let Err(e) = value {
                warn!(
                    "acl redis get authenticate result from redis, forbidden. error: {}",
                    e
                );
                return Ok(yedmq_plugin::plugin::AuthenticationResult::Result(
                    AuthenticationResultValue::Fail(ConnectReturnCode::ConnectionForbidenUnauth),
                ));
            } else {
                let value_hashmap: std::collections::HashMap<String, String> = value.unwrap();
                if value_hashmap.is_empty() {
                    return Ok(yedmq_plugin::plugin::AuthenticationResult::Result(
                        AuthenticationResultValue::Fail(
                            ConnectReturnCode::ConnectionForbidenUnauth,
                        ),
                    ));
                }
                if let Some(password) = value_hashmap.get("password") {
                    if password.as_str()
                        == packet
                            .payload
                            .password
                            .as_ref()
                            .unwrap_or(&String::from(""))
                    {
                        if let Some(tenant_id) = value_hashmap.get("tenant_id") {
                            return Ok(yedmq_plugin::plugin::AuthenticationResult::Result(
                                AuthenticationResultValue::Success(tenant_id.to_string()),
                            ));
                        } else {
                            warn!("acl redis get authenticate result from redis, skip to next plugin. error: user:{} tenant_id not found",
                                packet
                                    .payload
                                    .username
                                    .as_ref()
                                    .unwrap_or(&String::from(""))
                                );
                            return Ok(yedmq_plugin::plugin::AuthenticationResult::Result(
                                AuthenticationResultValue::Fail(
                                    ConnectReturnCode::ConnectionForbidenUnauth,
                                ),
                            ));
                        }
                    } else {
                        return Ok(yedmq_plugin::plugin::AuthenticationResult::Result(
                            AuthenticationResultValue::Fail(
                                ConnectReturnCode::ConnectionForbidenUnauth,
                            ),
                        ));
                    }
                } else {
                    warn!(
                        "acl redis authenticate user:{} password empty, skip to next plugin",
                        packet
                            .payload
                            .username
                            .as_ref()
                            .unwrap_or(&String::from(""))
                    );
                    return Ok(yedmq_plugin::plugin::AuthenticationResult::Next());
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

    // authorizate_acl_check
    // add permission
    // allow user yedmq to subscribe topic /a/b
    // ```
    // HSET mqtt_acl:yedmq /a/b '{"action": "subscribe"}'
    // ```
    // allow user yedmq to publish topic /a/b
    // ```
    // HSET mqtt_acl:yedmq /a/b '{"action": "publish"}'
    // ```
    // allow user yedmq to publish and subscribe topic /a/b
    // ```
    // HSET mqtt_acl:yedmq /a/b '{"action": "all"}'
    // ```
    fn authorizate_acl_check(
        &self,
        client: &yedmq_plugin::plugin::Client,
        topic: &String,
        action: yedmq_plugin::plugin::Action,
    ) -> anyhow::Result<yedmq_plugin::plugin::AuthorizationResult> {
        if let std::result::Result::Ok(mut connection) = self.connection_pool.get() {
            let key = format!(
                "mqtt_acl:{}",
                client
                    .properties
                    .username
                    .as_ref()
                    .unwrap_or(&String::from(""))
            );
            let values: Result<String, redis::RedisError> = connection.hget(key, topic);
            if let Err(e) = values {
                warn!(
                    "acl redis get acl result from redis, skip to next plugin, forbiden. error: {}",
                    e
                );
                return Ok(yedmq_plugin::plugin::AuthorizationResult::Result(false));
            } else {
                let values = values.unwrap();
                if values.is_empty() {
                    warn!("acl redis get acl result from redis, skip to next plugin. error: user:{} topic:{} not found", client.properties.username.as_ref().unwrap_or(&String::from("")), topic);
                    return Ok(yedmq_plugin::plugin::AuthorizationResult::Result(false));
                } else {
                    let action_params = match action {
                        yedmq_plugin::plugin::Action::Publish => "publish",
                        yedmq_plugin::plugin::Action::Subscribe => "subscribe",
                    };
                    let action_params_all = String::from("all");
                    if values.contains(action_params) || values.contains(&action_params_all) {
                        return Ok(yedmq_plugin::plugin::AuthorizationResult::Result(true));
                    } else {
                        return Ok(yedmq_plugin::plugin::AuthorizationResult::Result(false));
                    }
                }
            }
        } else {
            warn!("acl redis get acl result from redis, skip to next plugin. error: get connection error");
            return Ok(yedmq_plugin::plugin::AuthorizationResult::Next());
        }
    }
}

register_plugin!(AclRedis, AclRedis::new);
