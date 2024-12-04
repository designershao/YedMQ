use std::fs;

use anyhow::{anyhow, Result};
use log::info;
use samoye_plugin::{
    plugin::{Action, AuthenticationResult, AuthorizationResult, Plugin, PluginError},
    register_plugin,
};
use serde::{Deserialize, Serialize};

pub struct AclFile {
    acl_rules: AclRules,
}

pub struct AclRules {
    rules: Vec<AclRuleItem>,
}

#[derive(Serialize, Deserialize)]
pub struct AclRuleItem {
    tenant: Vec<String>,
    action: String,
    username: Vec<String>,
    ipaddr: Vec<String>,
    subscribe: Vec<String>,
    publish: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub enum AclAction {
    Allow,
    Deny,
}

impl AclRuleItem {
    fn validate_rule(&self) -> Result<()> {
        if self.action != "allow" && self.action != "deny" {
            return Err(anyhow!("action must be allow or deny"));
        }
        Ok(())
    }

    fn get_action(&self) -> AclAction {
        if self.action == "allow" {
            AclAction::Allow
        } else {
            AclAction::Deny
        }
    }

    pub fn match_rule(
        &self,
        tenant: &str,
        username: &Option<String>,
        ipaddr: &str,
        topic: &str,
        action: &Action,
    ) -> Option<AclAction> {
        match action {
            Action::Subscribe => {
                if username.is_some() {
                    if self
                        .username
                        .iter()
                        .any(|u| u == "%" || u == username.as_ref().unwrap().as_str())
                    {
                        if self.tenant.iter().any(|t| t == "%" || t == tenant) {
                            if self.ipaddr.iter().any(|ip| ip == "%" || ip == ipaddr) {
                                if self.subscribe.iter().any(|s| s == "%" || s == topic) {
                                    return Some(self.get_action());
                                }
                            }
                        }
                    }
                } else {
                    if self.ipaddr.iter().any(|ip| ip == "%" || ip == ipaddr) {
                        if self.tenant.iter().any(|t| t == "%" || t == tenant) {
                            if self.subscribe.iter().any(|s| s == "%" || s == topic) {
                                return Some(self.get_action());
                            }
                        }
                    }
                }
            }
            Action::Publish => {
                if username.is_some() {
                    if self
                        .username
                        .iter()
                        .any(|u| u == "%" || u == username.as_ref().unwrap().as_str())
                    {
                        if self.tenant.iter().any(|t| t == "%" || t == tenant) {
                            if self.ipaddr.iter().any(|ip| ip == "%" || ip == ipaddr) {
                                if self.publish.iter().any(|s| s == "%" || s == topic) {
                                    return Some(self.get_action());
                                }
                            }
                        }
                    }
                } else {
                    if self.ipaddr.iter().any(|ip| ip == "%" || ip == ipaddr) {
                        if self.tenant.iter().any(|t| t == "%" || t == tenant) {
                            if self.publish.iter().any(|s| s == "%" || s == topic) {
                                return Some(self.get_action());
                            }
                        }
                    }
                }
            }
        }
        None
    }
}

impl AclRules {
    pub fn new() -> Result<AclRules> {
        let rule_content = fs::read_to_string("./plugins/acl_file/acl_rule.json");

        match rule_content {
            Ok(content) => {
                let rules: Vec<AclRuleItem> = serde_json::from_str(content.as_str())?;
                for (i, rule) in rules.iter().enumerate() {
                    if let Err(e) = rule.validate_rule() {
                        return Err(anyhow!("rule item {} error: {}", i, e));
                    }
                }
                Ok(AclRules { rules })
            }
            Err(e) => Err(anyhow!("load acl rule file error: {}", e)),
        }
    }

    pub fn get_match_rule(
        &self,
        tenant: &str,
        username: &Option<String>,
        ipaddr: &str,
        topic: &str,
        action: &Action,
    ) -> Option<AclAction> {
        for rule in self.rules.iter() {
            if let Some(action) = rule.match_rule(tenant, username, ipaddr, topic, action) {
                return Some(action);
            }
        }
        None
    }
}

impl AclFile {
    pub fn new() -> AclFile {
        let acl_rules = AclRules::new().unwrap();
        AclFile { acl_rules }
    }
}

impl Plugin for AclFile {
    fn on_activate(&self) -> Result<()> {
        env_logger::init();
        info!("acl_file plugin on activate, start loading the default acl file");
        Ok(())
    }

    fn on_deactivate(&self) -> Result<()> {
        info!("acl_file plugin on_deactivate");
        Ok(())
    }

    fn connect_authenticate(
        &self,
        _packet: &samoye_mqtt::v3::connect::ConnectPacket,
    ) -> Result<AuthenticationResult> {
        return Err(PluginError::PluginHookNotImplement().into());
    }

    fn on_publish(
        &self,
        _client: &samoye_plugin::plugin::Client,
        _packet: &samoye_mqtt::v3::publish::PublishPacket,
    ) {
        // Do Nothing
    }

    fn on_disconnect(&self, _client: &samoye_plugin::plugin::Client) {
        // Do Nothing
    }

    fn authorizate_acl_check(
        &self,
        client: &samoye_plugin::plugin::Client,
        topic: &String,
        action: samoye_plugin::plugin::Action,
    ) -> Result<AuthorizationResult> {
        let match_rule = self.acl_rules.get_match_rule(
            &client.tenant_id.as_str(),
            &client.properties.username,
            &client.client_identifier,
            topic,
            &action,
        );
        match match_rule {
            Some(AclAction::Allow) => Ok(AuthorizationResult::Result(true)),
            Some(AclAction::Deny) => Ok(AuthorizationResult::Result(false)),
            None => Ok(AuthorizationResult::Next()),
        }
    }
}

impl Drop for AclFile {
    fn drop(&mut self) {
        info!("acl_file plugin drop");
    }
}

register_plugin!(AclFile, AclFile::new);

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::{AclAction, AclFile, AclRuleItem, AclRules};

    #[test]
    fn test_rule_match() {
        let acl_rules = vec![AclRuleItem {
            tenant: vec!["%".to_string()],
            action: "allow".to_string(),
            username: vec!["%".to_string()],
            ipaddr: vec!["%".to_string()],
            subscribe: vec!["/a/b".to_string()],
            publish: vec!["/a/b".to_string()],
        }];
        let acl_file = AclFile {
            acl_rules: AclRules { rules: acl_rules },
        };

        assert_eq!(
            acl_file.acl_rules.get_match_rule(
                "tenant",
                &Some("user".to_string()),
                "127.0.0.1",
                "/a/b",
                &samoye_plugin::plugin::Action::Subscribe
            ),
            Some(AclAction::Allow)
        );
    }
}
