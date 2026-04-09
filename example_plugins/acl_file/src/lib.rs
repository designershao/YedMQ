use std::{fs, path::Path};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use yedmq_plugin_host::{
    create_timestamp,
    protocol::{
        plugin_protocol::{
            AuthAction, AuthenticateRequest, AuthenticateResponse, AuthorizeRequest,
            AuthorizeResponse, Hook, InitializeRequest, InitializeResponse, MessageType, Method,
            ProtocolMessage,
        },
        AUTHENTICATE_REQUEST_TYPE_URL, AUTHENTICATE_RESPONSE_TYPE_URL, AUTHORIZE_REQUEST_TYPE_URL,
        AUTHORIZE_RESPONSE_TYPE_URL, INIT_REQUEST_TYPE_URL, INIT_RESPONSE_TYPE_URL,
        PING_RESPONSE_TYPE_URL,
    },
};

const SOURCE_NAME: &str = "acl_file";
const TARGET_NAME: &str = "plugin_host";
const DEFAULT_TENANT: &str = "public";
const WILDCARD: &str = "%";

fn default_match_all() -> Vec<String> {
    vec![WILDCARD.to_string()]
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleAction {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AclConfig {
    #[serde(default)]
    pub authenticate: Option<AuthenticateSection>,
    #[serde(default)]
    pub authorize: Option<AuthorizeSection>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthenticateSection {
    pub rules: Vec<AuthenticateRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthenticateRule {
    pub action: RuleAction,
    pub tenant: Option<String>,
    #[serde(default = "default_match_all")]
    pub username: Vec<String>,
    #[serde(default = "default_match_all")]
    pub password: Vec<String>,
    #[serde(default = "default_match_all")]
    pub client_ip: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthorizeSection {
    pub rules: Vec<AuthorizeRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthorizeRule {
    pub action: RuleAction,
    #[serde(default = "default_match_all")]
    pub tenant: Vec<String>,
    #[serde(default = "default_match_all")]
    pub username: Vec<String>,
    #[serde(default)]
    pub subscribe: Vec<String>,
    #[serde(default)]
    pub publish: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AclPlugin {
    config: AclConfig,
    auth_code: String,
    authenticate_priority: u32,
    authorize_priority: u32,
}

impl AclConfig {
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read ACL file {}", path.display()))?;
        let config = serde_json::from_str::<Self>(&content)
            .with_context(|| format!("failed to parse ACL file {}", path.display()))?;
        Ok(config)
    }

    pub fn declared_hooks(&self, authenticate_priority: u32, authorize_priority: u32) -> Vec<Hook> {
        let mut hooks = Vec::new();

        if self.authenticate.is_some() {
            hooks.push(Hook {
                name: "Authenticate".to_string(),
                priority: authenticate_priority,
                filter: None,
            });
        }

        if self.authorize.is_some() {
            hooks.push(Hook {
                name: "Authorize".to_string(),
                priority: authorize_priority,
                filter: None,
            });
        }

        hooks
    }

    pub fn authenticate(&self, request: &AuthenticateRequest) -> AuthenticateResponse {
        let Some(section) = &self.authenticate else {
            return AuthenticateResponse {
                authenticated: false,
                permissions: vec![],
                session_data: None,
                error_reason: Some("authenticate ACL is not configured".to_string()),
                tenant_id: None,
            };
        };

        for rule in &section.rules {
            if matches_patterns(&rule.username, &request.username)
                && matches_patterns(&rule.password, &request.password)
                && matches_patterns(&rule.client_ip, &request.client_ip)
            {
                return match rule.action {
                    RuleAction::Allow => AuthenticateResponse {
                        authenticated: true,
                        permissions: vec![],
                        session_data: None,
                        error_reason: None,
                        tenant_id: Some(
                            rule.tenant
                                .clone()
                                .unwrap_or_else(|| DEFAULT_TENANT.to_string()),
                        ),
                    },
                    RuleAction::Deny => AuthenticateResponse {
                        authenticated: false,
                        permissions: vec![],
                        session_data: None,
                        error_reason: Some("denied by authenticate ACL rule".to_string()),
                        tenant_id: None,
                    },
                };
            }
        }

        AuthenticateResponse {
            authenticated: false,
            permissions: vec![],
            session_data: None,
            error_reason: Some("no authenticate ACL rule matched".to_string()),
            tenant_id: None,
        }
    }

    pub fn authorize(&self, request: &AuthorizeRequest) -> AuthorizeResponse {
        let Some(section) = &self.authorize else {
            return AuthorizeResponse {
                authorized: false,
                reason: Some("authorize ACL is not configured".to_string()),
                modified_context: None,
            };
        };

        let action = match request.action {
            value if value == AuthAction::Publish as i32 => AuthAction::Publish,
            value if value == AuthAction::Subscribe as i32 => AuthAction::Subscribe,
            value if value == AuthAction::Connect as i32 => AuthAction::Connect,
            value if value == AuthAction::Unsubscribe as i32 => AuthAction::Unsubscribe,
            _ => AuthAction::Unspecified,
        };

        if !matches!(action, AuthAction::Publish | AuthAction::Subscribe) {
            return AuthorizeResponse {
                authorized: false,
                reason: Some(format!("unsupported authorize action {}", request.action)),
                modified_context: None,
            };
        }

        for rule in &section.rules {
            let topic_patterns = match action {
                AuthAction::Publish if !rule.publish.is_empty() => rule.publish.as_slice(),
                AuthAction::Subscribe if !rule.subscribe.is_empty() => rule.subscribe.as_slice(),
                _ => continue,
            };

            if matches_patterns(&rule.tenant, &request.tenant_id)
                && matches_patterns(&rule.username, &request.username)
                && matches_patterns(topic_patterns, &request.topic)
            {
                return match rule.action {
                    RuleAction::Allow => AuthorizeResponse {
                        authorized: true,
                        reason: None,
                        modified_context: None,
                    },
                    RuleAction::Deny => AuthorizeResponse {
                        authorized: false,
                        reason: Some("denied by authorize ACL rule".to_string()),
                        modified_context: None,
                    },
                };
            }
        }

        AuthorizeResponse {
            authorized: false,
            reason: Some("no authorize ACL rule matched".to_string()),
            modified_context: None,
        }
    }
}

impl AclPlugin {
    pub fn new(
        config: AclConfig,
        auth_code: String,
        authenticate_priority: u32,
        authorize_priority: u32,
    ) -> Self {
        Self {
            config,
            auth_code,
            authenticate_priority,
            authorize_priority,
        }
    }

    pub fn handle_request(&self, request: &ProtocolMessage) -> Result<ProtocolMessage> {
        match request.method() {
            Method::Initialize => {
                let _: InitializeRequest = decode_params(request, INIT_REQUEST_TYPE_URL)?;
                Ok(response_message(
                    request,
                    INIT_RESPONSE_TYPE_URL,
                    InitializeResponse {
                        status: "ready".to_string(),
                        capabilities: vec![],
                        hooks: self
                            .config
                            .declared_hooks(self.authenticate_priority, self.authorize_priority),
                        plugin_info: None,
                        auth_code: self.auth_code.clone(),
                    },
                ))
            }
            Method::Authenticate => {
                let authenticate_request: AuthenticateRequest =
                    decode_params(request, AUTHENTICATE_REQUEST_TYPE_URL)?;
                Ok(response_message(
                    request,
                    AUTHENTICATE_RESPONSE_TYPE_URL,
                    self.config.authenticate(&authenticate_request),
                ))
            }
            Method::Authorize => {
                let authorize_request: AuthorizeRequest =
                    decode_params(request, AUTHORIZE_REQUEST_TYPE_URL)?;
                Ok(response_message(
                    request,
                    AUTHORIZE_RESPONSE_TYPE_URL,
                    self.config.authorize(&authorize_request),
                ))
            }
            Method::Ping => Ok(empty_response_message(request, PING_RESPONSE_TYPE_URL)),
            unsupported => bail!("unsupported method {:?}", unsupported),
        }
    }
}

fn matches_patterns(patterns: &[String], value: &str) -> bool {
    patterns
        .iter()
        .any(|pattern| pattern == WILDCARD || pattern == value)
}

fn decode_params<T>(request: &ProtocolMessage, expected_type_url: &str) -> Result<T>
where
    T: prost::Message + Default,
{
    let params = request.params.as_ref().context("missing request params")?;
    if params.type_url != expected_type_url {
        bail!(
            "unexpected type_url {}, expected {}",
            params.type_url,
            expected_type_url
        );
    }
    T::decode(params.value.as_slice()).context("failed to decode request payload")
}

fn response_message<T>(request: &ProtocolMessage, type_url: &str, payload: T) -> ProtocolMessage
where
    T: prost::Message,
{
    ProtocolMessage {
        version: request.version.clone(),
        r#type: MessageType::Response as i32,
        id: request.id.clone(),
        timestamp: Some(create_timestamp()),
        source: SOURCE_NAME.to_string(),
        target: TARGET_NAME.to_string(),
        method: None,
        params: None,
        result: Some(prost_types::Any {
            type_url: type_url.to_string(),
            value: payload.encode_to_vec(),
        }),
        error: None,
        metadata: Default::default(),
    }
}

fn empty_response_message(request: &ProtocolMessage, type_url: &str) -> ProtocolMessage {
    ProtocolMessage {
        version: request.version.clone(),
        r#type: MessageType::Response as i32,
        id: request.id.clone(),
        timestamp: Some(create_timestamp()),
        source: SOURCE_NAME.to_string(),
        target: TARGET_NAME.to_string(),
        method: None,
        params: None,
        result: Some(prost_types::Any {
            type_url: type_url.to_string(),
            value: vec![],
        }),
        error: None,
        metadata: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yedmq_plugin_host::protocol::plugin_protocol::{
        AuthAction, AuthenticateRequest, AuthorizeRequest,
    };

    fn sample_authenticate_request() -> AuthenticateRequest {
        AuthenticateRequest {
            client_id: "client-1".to_string(),
            username: "normal".to_string(),
            password: "normal123".to_string(),
            client_ip: "192.168.1.10".to_string(),
            client_cert: Vec::new(),
            protocol_version: "3.1.1".to_string(),
            properties: None,
        }
    }

    fn sample_authorize_request(action: AuthAction, topic: &str) -> AuthorizeRequest {
        AuthorizeRequest {
            tenant_id: "public".to_string(),
            client_id: "client-1".to_string(),
            username: "normal".to_string(),
            action: action as i32,
            topic: topic.to_string(),
            qos: 0,
            context: None,
        }
    }

    #[test]
    fn authenticate_uses_first_match_and_defaults_tenant() {
        let config = AclConfig {
            authenticate: Some(AuthenticateSection {
                rules: vec![
                    AuthenticateRule {
                        action: RuleAction::Allow,
                        tenant: None,
                        username: default_match_all(),
                        password: default_match_all(),
                        client_ip: vec!["192.168.1.10".to_string()],
                    },
                    AuthenticateRule {
                        action: RuleAction::Allow,
                        tenant: Some("custom".to_string()),
                        username: vec!["normal".to_string()],
                        password: vec!["normal123".to_string()],
                        client_ip: vec!["192.168.1.10".to_string()],
                    },
                ],
            }),
            authorize: None,
        };

        let response = config.authenticate(&sample_authenticate_request());
        assert!(response.authenticated);
        assert_eq!(response.tenant_id, Some(DEFAULT_TENANT.to_string()));
    }

    #[test]
    fn authorize_distinguishes_publish_and_subscribe_rules() {
        let config = AclConfig {
            authenticate: None,
            authorize: Some(AuthorizeSection {
                rules: vec![
                    AuthorizeRule {
                        action: RuleAction::Deny,
                        tenant: vec!["public".to_string()],
                        username: vec!["normal".to_string()],
                        subscribe: vec!["/a/b".to_string()],
                        publish: Vec::new(),
                    },
                    AuthorizeRule {
                        action: RuleAction::Allow,
                        tenant: vec!["public".to_string()],
                        username: vec!["normal".to_string()],
                        subscribe: Vec::new(),
                        publish: vec!["/a/b".to_string()],
                    },
                ],
            }),
        };

        let subscribe_response =
            config.authorize(&sample_authorize_request(AuthAction::Subscribe, "/a/b"));
        assert!(!subscribe_response.authorized);

        let publish_response =
            config.authorize(&sample_authorize_request(AuthAction::Publish, "/a/b"));
        assert!(publish_response.authorized);
    }

    #[test]
    fn declared_hooks_only_include_present_sections() {
        let config = AclConfig {
            authenticate: Some(AuthenticateSection { rules: vec![] }),
            authorize: None,
        };

        let hooks = config.declared_hooks(10, 20);
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].name, "Authenticate");
        assert_eq!(hooks[0].priority, 10);
    }

    #[test]
    fn unmatched_authorize_request_is_denied() {
        let config = AclConfig {
            authenticate: None,
            authorize: Some(AuthorizeSection {
                rules: vec![AuthorizeRule {
                    action: RuleAction::Allow,
                    tenant: vec!["public".to_string()],
                    username: vec!["super".to_string()],
                    subscribe: vec!["%".to_string()],
                    publish: vec!["%".to_string()],
                }],
            }),
        };

        let response = config.authorize(&sample_authorize_request(AuthAction::Publish, "/missing"));
        assert!(!response.authorized);
        assert_eq!(
            response.reason,
            Some("no authorize ACL rule matched".to_string())
        );
    }
}
