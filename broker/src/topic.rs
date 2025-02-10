use core::fmt;
use std::{collections::HashMap, sync::{Arc, RwLock}};

use base64::{engine::general_purpose, Engine};
use log::warn;
use serde::{Deserialize, Serialize};
use yedmq_mqtt::MqttPacketV3;

#[derive(Debug, PartialEq)]
pub enum Error {
    TopicNotFound(String),
    TenantNotFound(String),
    InvalidTopicFilter(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::TopicNotFound(topic) => write!(f, "topic {topic} not found"), 
            Error::TenantNotFound(tenant) => write!(f, "tenant {tenant} not found"),
            Error::InvalidTopicFilter(topic) => write!(f, "invalid topic filter {topic}"),
        }
    }
}


fn test_topic(topic: &String) -> bool {
    let i = topic.split("/");
    let mut index = 0;
    let length = i.clone().count();
    for s in i {
        index += 1;
        if s.len() > 1 {
            if s.contains('#') || s.contains('+') {
                return false
            }
        } else {
            if s.contains('#') {
                if index != length {
                    return false
                }
            }
        }
    }
    true
}


/// Generates a key string that can be used to store subscriptions in a `SubscriptionMap`.
///
/// The key is formatted as "encoded_client_id:encoded_topic:qos", where:
///
/// - `encoded_client_id` is the Base64-encoded client ID.
/// - `encoded_topic` is the Base64-encoded topic.
/// - `qos` is the QoS level.
///
/// # Arguments
///
/// * `client_id` - The client ID.
/// * `topic` - The topic.
/// * `qos` - The QoS level.
///
/// # Returns
///
/// A string slice that can be used as a key in a `SubscriptionMap`.
fn generate_key(client_id: &str, topic: &str) -> String {
    let encoded_client_id = general_purpose::STANDARD.encode(client_id);
    let encoded_topic = general_purpose::STANDARD.encode(topic);
    format!("{}:{}", encoded_client_id, encoded_topic)
}

/// Extracts and decodes information from a key.
///
/// This function takes a key string formatted as "encoded_client_id:encoded_topic:qos",
/// splits it into parts, and decodes the client ID and topic using Base64 decoding.
/// It returns a tuple containing the decoded client ID, decoded topic, and the QoS level.
///
/// # Arguments
///
/// * `key` - A string slice that holds the key in the format "encoded_client_id:encoded_topic:qos"
///
/// # Returns
///
/// * A tuple containing:
///   - `String`: Decoded client ID
///   - `String`: Decoded topic
///   - `u8`: QoS level
///
/// # Panics
///
/// This function will panic if the Base64 decoding fails or if the QoS level cannot be parsed as a `u8`.
fn extract_info_from_key(key: &str) -> (String, String) {
    let i = key.split(":");
    let mut index = 0;
    let mut encoded_client_id = String::new();
    let mut encoded_topic = String::new();
    for s in i {
        index += 1;
        if index == 1 {
            encoded_client_id = s.to_string();
        } else if index == 2 {
            encoded_topic = s.to_string();
        }
    }
    let binding = general_purpose::STANDARD.decode(&encoded_client_id).unwrap();
    let client_id = std::str::from_utf8(&binding).unwrap();
    let binding = general_purpose::STANDARD.decode(&encoded_topic).unwrap();
    let topic = std::str::from_utf8(&binding).unwrap();
    (client_id.to_string(), topic.to_string())
}


pub struct TopicManager {

    retain_message_recorder: RwLock<HashMap<String, HashMap<String, (String, u8)>>>,

    topic_info_recorder: RwLock<HashMap<String, HashMap<String, u8>>>,

    topic_tree: Arc<RwLock<HashMap<String, Arc<RwLock<TopicNode>>>>>,

}

impl TopicManager {
    // generate mqtt topic test 


    pub fn new() -> Self {
        Self {
            retain_message_recorder: RwLock::new(HashMap::new()),

            topic_info_recorder: RwLock::new(HashMap::new()),

            topic_tree: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_topic_list_with_pagination(&self, tenant_id:&String, offset: u64, limit: u64) -> anyhow::Result<(u64,Vec<(String,String, u8)>)> {
      let topic_info_recorder = self.topic_info_recorder.read().unwrap();  
      if topic_info_recorder.contains_key(tenant_id) {
        let items = topic_info_recorder.get(tenant_id).unwrap();
        let mut result_items = Vec::new();
        for (key, qos) in items.iter().skip(offset as usize).take(limit as usize) {
            let (client_id, topic) = extract_info_from_key(key);
            result_items.push((client_id, topic, *qos));
        }
        let total = items.len();
        return Ok((total as u64, result_items));
      } else {
          return Err(anyhow::anyhow!(Error::TenantNotFound(tenant_id.to_string())));
      }
    }

    pub fn create_tenant(&mut self, tenant:String) {
        let topic_tree = self.topic_tree.clone();
        let mut topic_tree = topic_tree.write().unwrap();
        let tenant_ref = &tenant;
        if !topic_tree.contains_key(tenant_ref) {
            topic_tree.insert(tenant_ref.clone(), Arc::new(RwLock::new(TopicNode::new("/".to_string()))));
        }

        let mut topic_info_recorder = self.topic_info_recorder.write().unwrap();
        if !topic_info_recorder.contains_key(tenant_ref) {
            topic_info_recorder.insert(tenant_ref.clone(), HashMap::new());
        }

        let mut retain_message_recorder = self.retain_message_recorder.write().unwrap();
        if !retain_message_recorder.contains_key(tenant_ref) {
            retain_message_recorder.insert(tenant_ref.clone(), HashMap::new());
        }
    }


    pub fn subscription(&mut self, tenant_id:String, client_identifier:String, topic_filter: String, qos: u8) -> Result<(), Error> {

        if !test_topic(&topic_filter) {
            return Err(Error::InvalidTopicFilter(topic_filter));
        }

        let topic_patterns:Vec<String> = topic_filter.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read().unwrap();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(Error::TenantNotFound(tenant_id))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            let result = Self::recursion_subscription(tenant_topic_root, topic_patterns, client_identifier.clone(), qos);
            if result.is_err() {
                return Err(result.err().unwrap());
            } else {
                // Update the topic_info_recorder
                let mut topic_info_recorder = self.topic_info_recorder.write().unwrap();
                let topic_info_recorder_optional = topic_info_recorder.get_mut(&tenant_id);

                let key = generate_key(&client_identifier, &topic_filter);

                let topic_info_state_item = topic_info_recorder_optional.unwrap();
                if !topic_info_state_item.contains_key(&key) {
                    topic_info_state_item.insert(key, qos);
                }
                //
                Ok(())
            }

        }
    }

    fn recursion_subscription(topic_node:Arc<RwLock<TopicNode>>, mut topic_partterns: Vec<String>, client_identifier: String, qos: u8) -> Result<(), Error> {
        if topic_partterns.len() > 0 {
            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node.write().unwrap().find_or_create_leaf(topic_pattern.to_string());
            let topic_patterns_rest = topic_partterns.drain(1..).collect();
            Self::recursion_subscription(topic_node_next, topic_patterns_rest, client_identifier, qos)
        } else {
            topic_node.write().unwrap().add_subscription(Subscription{client_identifier, qos});
            Ok(())
        }
    }

    pub fn unsubscription(&mut self, tenant_id:String, client_identifier:String, topic_filter: String) -> Result<(), Error> {
        let topic_patterns:Vec<String> = topic_filter.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read().unwrap();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(Error::TenantNotFound(tenant_id))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            let result = Self::recursion_unsubscription(tenant_topic_root, topic_patterns, &client_identifier);
            if result.is_err() {
                return Err(result.err().unwrap());
            } else {
                // Update the topic_info_recorder
                let mut topic_info_recorder = self.topic_info_recorder.write().unwrap();
                let topic_info_state_optional = topic_info_recorder.get_mut(&tenant_id);
                let key = generate_key(&client_identifier, &topic_filter);
                let topic_info_state_item = topic_info_state_optional.unwrap();
                if topic_info_state_item.contains_key(&key) {
                    topic_info_state_item.remove(&key);
                }
                //
                Ok(())
            }
        }
    }

    fn recursion_unsubscription(topic_node:Arc<RwLock<TopicNode>>, mut topic_partterns: Vec<String>, client_identifier:&String) -> Result<(), Error> {
        if topic_partterns.len() > 0 {
            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node.write().unwrap().get_leaf(topic_pattern.to_string());
            if topic_node_next.is_none() {
                Err(Error::TopicNotFound(topic_pattern.to_string()))
            } else {
                let topic_patterns_rest = topic_partterns.drain(1..).collect();
                Self::recursion_unsubscription(topic_node_next.unwrap(), topic_patterns_rest, client_identifier)
            }
        } else {
            topic_node.write().unwrap().remove_subscription(client_identifier);
            Ok(())
        }
    }

    pub fn get_subscriptions(&self, tenant_id:String, msg_topic: String) -> Result<Vec<Arc<Subscription>>, Error> {
        let topic_patterns:Vec<String> = msg_topic.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read().unwrap();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(Error::TenantNotFound(tenant_id))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            Ok(Self::recursion_get_subscriptions(tenant_topic_root, topic_patterns))
        }
    }

    fn recursion_get_subscriptions(topic_node:Arc<RwLock<TopicNode>>, mut topic_partterns: Vec<String>) -> Vec<Arc<Subscription>> {
        if topic_partterns.len() > 0 {
            let mut result = vec![];
            // Get # wildcard subscriptions
            let topic_sharp_wildcard_option = topic_node.write().unwrap().get_leaf("#".to_string());
            if topic_sharp_wildcard_option.is_some() {
                let sharp_wildcard_subscriptions = topic_sharp_wildcard_option.unwrap().read().unwrap().get_subscriptions();
                result.extend(sharp_wildcard_subscriptions);
            }
            // Get + wildcard subscriptions
            let topic_plus_wildcard_option = topic_node.write().unwrap().get_leaf("+".to_string());
            if topic_plus_wildcard_option.is_some() {
                let mut topic_plus_left = topic_partterns.clone();
                let topic_plus_patterns_rest = topic_plus_left.drain(1..).collect();
                let topic_node_clone = topic_plus_wildcard_option.unwrap().clone();
                let plus_wildcard_subscriptions = Self::recursion_get_subscriptions(topic_node_clone, topic_plus_patterns_rest);
                result.extend(plus_wildcard_subscriptions);
            }

            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node.write().unwrap().get_leaf(topic_pattern.to_string());
            if topic_node_next.is_some() {
                let topic_patterns_rest = topic_partterns.drain(1..).collect();
                let subscriptions = Self::recursion_get_subscriptions(topic_node_next.unwrap(), topic_patterns_rest);
                result.extend(subscriptions);
            }
            result
        } else {
            topic_node.read().unwrap().get_subscriptions()
        }
    }

    fn recursion_retain_publish_packet(topic_node:Arc<RwLock<TopicNode>>, mut topic_partterns: Vec<String>, publish_packet:MqttPacketV3) -> Result<(), Error> {
        if topic_partterns.len() > 0 {
            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node.write().unwrap().find_or_create_leaf(topic_pattern.to_string());
            let topic_patterns_rest = topic_partterns.drain(1..).collect();
            Self::recursion_retain_publish_packet(topic_node_next, topic_patterns_rest, publish_packet)
        } else {
            topic_node.write().unwrap().set_retain_publish_message(publish_packet);
            Ok(())
        }
    }

    pub fn get_retain_message_list_with_pagination(
        &self, 
        tenant_identifier: &str,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(u64,Vec<(String, String,u8)>)> {
        if !self.retain_message_recorder.read().unwrap().contains_key(tenant_identifier) {
            Err(anyhow::anyhow!(Error::TenantNotFound(tenant_identifier.to_string())))
        } else {
            let retain_message_recorder = self.retain_message_recorder.read().unwrap();
            let retain_message_list = retain_message_recorder.get(tenant_identifier).unwrap();
            let total = retain_message_list.len() as u64;
            let mut result = vec![];
            for (topic_filter, (client_id, qos)) in retain_message_list.iter().skip(offset as usize).take(limit as usize) {
                result.push((topic_filter.clone(), client_id.clone(), *qos));
            }  
            Ok((total, result))
        }
    }

    fn recursion_clean_retain_publish_packet(topic_node:Arc<RwLock<TopicNode>>,mut topic_partterns:Vec<String>)  -> Result<(), Error> {
        if topic_partterns.len() > 0 {
            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node.write().unwrap().get_leaf(topic_pattern.to_string());
            if topic_node_next.is_none() {
                Err(Error::TopicNotFound(topic_pattern.to_string()))
            } else {
                let topic_patterns_rest = topic_partterns.drain(1..).collect();
                Self::recursion_clean_retain_publish_packet(topic_node_next.unwrap(), topic_patterns_rest)
            }
        } else {
            topic_node.write().unwrap().clean_retain_publish_message();
            Ok(())
        }
    }

    // clean retain publish packet from the topic tree
    pub fn clean_retain_publish_packet(&mut self, tenant_id:String, topic_filter:&String) -> Result<(), Error> {
        let topic_patterns:Vec<String> = topic_filter.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read().unwrap();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(Error::TenantNotFound(tenant_id))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            let result = Self::recursion_clean_retain_publish_packet(tenant_topic_root, topic_patterns);
            if let Err(err) = result  {
                warn!("clean retain publish packet from the topic tree error: {}", err);
                Err(err)
            } else {
                let mut retain_message_recorder = self.retain_message_recorder.write().unwrap();
                let retain_message_recorder_optional = retain_message_recorder.get_mut(&tenant_id);
                retain_message_recorder_optional.unwrap().remove(topic_filter);
                Ok(())
            }
        }
    }

    fn recursion_get_retain_packet(topic_node:Arc<RwLock<TopicNode>>, mut topic_patterns:Vec<String>) -> Vec<Arc<MqttPacketV3>> {
        let mut result:Vec<Arc<MqttPacketV3>> = vec![];
        if topic_patterns.len() > 0 {
            let topic_pattern = &topic_patterns[0];
            if topic_pattern == &"+".to_string() || topic_pattern == &"#".to_string() {
                for sub in topic_node.write().unwrap().leaves.read().unwrap().iter() {
                    let topic_patterns_rest = topic_patterns.clone().drain(1..).collect();
                    result.append(&mut Self::recursion_get_retain_packet(sub.clone(), topic_patterns_rest));
                }
            } else {
                let topic_node_next = topic_node.write().unwrap().get_leaf(topic_pattern.to_string());
                if !topic_node_next.is_none() {
                    let topic_patterns_rest = topic_patterns.drain(1..).collect();
                    result.append(
                        &mut Self::recursion_get_retain_packet(topic_node_next.unwrap(), topic_patterns_rest)
                    );
                }
            }
        } else {
            let topic_node = topic_node.read().unwrap();
            if let Some(p) = &topic_node.retain_publish_packet  {
                result.append(&mut vec![p.clone()])
            }  
        }
        result
    }

    pub fn get_retain_publish_packet(&mut self, tenant_id:String, topic_filter: String) -> Result<Vec<Arc<MqttPacketV3>>, Error> {
        if !test_topic(&topic_filter) {
            return Err(Error::InvalidTopicFilter(topic_filter));
        }

        let topic_patterns:Vec<String> = topic_filter.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read().unwrap();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(Error::TenantNotFound(tenant_id))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            Ok(Self::recursion_get_retain_packet(tenant_topic_root, topic_patterns))
        }
    }

    // register retain publish packet to the topic tree
    pub fn register_retain_publish_packet(&mut self, tenant_id:String, source_client_identifier:String, publish_packet:&MqttPacketV3) -> Result<(), Error> {
        if let MqttPacketV3::Publish(publish_packet) = publish_packet {
            let topic_filter = publish_packet.variable_header.topic_name.clone();
            if !test_topic(&publish_packet.variable_header.topic_name) {
                return Err(Error::InvalidTopicFilter(topic_filter));
            }

            let topic_patterns:Vec<String> = topic_filter.split("/").map(String::from).collect();
            let map = self.topic_tree.clone();
            let tenant_topic_root_rwlock = map.read().unwrap();
            let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
            if tenant_topic_root_optional.is_none() {
                Err(Error::TenantNotFound(tenant_id))
            } else {
                let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
                let result = Self::recursion_retain_publish_packet(tenant_topic_root, topic_patterns,MqttPacketV3::Publish(publish_packet.clone()));
                if let Err(e) = result  {
                    Err(e)
                } else {
                    // Update retain recorder
                    let mut retain_message_recorder = self.retain_message_recorder.write().unwrap();
                    let retain_message_recorder_optional = retain_message_recorder.get_mut(&tenant_id);
                    let retain_message_recorder_item = retain_message_recorder_optional.unwrap();
                    let qos = publish_packet.fix_header.qos.unwrap_or(0);
                    retain_message_recorder_item.insert(publish_packet.variable_header.topic_name.clone(), (source_client_identifier, qos as u8));
                    //
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }

    // get all tenant names
    pub fn get_tenant_names(&self) -> Vec<String> {
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read().unwrap();
        tenant_topic_root_rwlock.keys().map(String::from).collect()
    }

}

#[derive(Debug)]
struct TopicNode {

    pub topic_parttern: String,

    subscriptions: RwLock<Vec<Arc<Subscription>>>,

    retain_publish_packet: Option<Arc<MqttPacketV3>>,

    leaves: Arc<RwLock<Vec<Arc<RwLock<TopicNode>>>>>,
}

impl TopicNode {   

    pub fn new(topic_pattern: String) -> TopicNode {
        TopicNode {
            topic_parttern: topic_pattern,
            subscriptions: RwLock::new(vec![]),
            retain_publish_packet: None,
            leaves: Arc::new(RwLock::new(vec![])),
        }
    }

    pub fn get_subscriptions(&self) -> Vec<Arc<Subscription>> {
        let mut out = Vec::with_capacity(self.subscriptions.read().unwrap().len());
        for subscribtion in self.subscriptions.read().unwrap().iter() {
            out.push(Arc::clone(subscribtion));
        }
        out
    }

    pub fn set_retain_publish_message(&mut self, publish_packet:MqttPacketV3) {
        self.retain_publish_packet = Some(Arc::new(publish_packet));
    }

    pub fn clean_retain_publish_message(&mut self) {
        self.retain_publish_packet = None;
    }

    pub fn add_subscription(&mut self, subscribtion:Subscription) {
        let find_result = self.subscriptions.write().unwrap().binary_search_by(|f| {
            f.client_identifier.cmp(&subscribtion.client_identifier)
        });

        if let Err(_) = find_result { // not found in subscriptions
            self.subscriptions.write().unwrap().push(Arc::new(subscribtion));
            self.subscriptions.write().unwrap().sort_by(|sub_a, sub_b| {
                sub_a.client_identifier.cmp(&sub_b.client_identifier)
            });
        }
    }

    pub fn remove_subscription(&mut self, client_identifier:&String) {

        let find_result = self.subscriptions.read().unwrap().binary_search_by(|subscribtion| {
            subscribtion.client_identifier.cmp(&client_identifier)
        });

        match find_result  {
            Ok(index) => {
                self.subscriptions.write().unwrap().remove(index);
                self.subscriptions.write().unwrap().sort_by(|sub_a, sub_b| {
                    sub_a.client_identifier.cmp(&sub_b.client_identifier)
                });
            },
            Err(_) => {}
        }
    }

    fn get_leaf(&self, topic_pattern:String) -> Option<Arc<RwLock<TopicNode>>> {
        let find_result = self.leaves.read().unwrap().binary_search_by(|leaf| {
           leaf.read().unwrap().topic_parttern.cmp(&topic_pattern) 
        });

        match find_result {
            Ok(index) => {
                return Some(Arc::clone(&self.leaves.read().unwrap()[index]));
            },
            Err(_) => {
                None
            }
        }
    }

    fn find_or_create_leaf(&mut self, topic_pattern: String) -> Arc<RwLock<TopicNode>> {
        let find_result = self.leaves.read().unwrap().binary_search_by(|leaf| {
           leaf.read().unwrap().topic_parttern.cmp(&topic_pattern) 
        });

        match find_result {
            Ok(index) => {
                return Arc::clone(&self.leaves.read().unwrap()[index]); 
            },
            Err(_) => {
                let leaf = Arc::new(RwLock::new(TopicNode {
                    topic_parttern: topic_pattern,
                    subscriptions: RwLock::new(vec![]),
                    leaves: Arc::new(RwLock::new(vec![])),
                    retain_publish_packet: None
                }));
                let topic_node_cloned = leaf.clone();
                self.leaves.write().unwrap().push(leaf);
                self.leaves.write().unwrap().sort_by(|topic_node_a,topic_node_b| {
                    topic_node_a.read().unwrap().topic_parttern.cmp(&topic_node_b.read().unwrap().topic_parttern)
                });
                topic_node_cloned
            }
        }
    }

    
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Subscription {
    pub client_identifier: String,
    pub qos: u8,
}

#[cfg(test)]
mod tests {

    use yedmq_mqtt::{v3::{fixed_header::FixHeader, publish::{Payload, PublishPacket, VariableHeader}}, PacketType};

    use super::*;
    use std::thread;

    #[test]
    fn test_add_subscription_and_get_subscriptions() {
        let mut topic_node = TopicNode {
            topic_parttern: "a".to_string(),
            subscriptions: RwLock::new(vec![]),
            leaves: Arc::new(RwLock::new(vec![])),
            retain_publish_packet: None
        };

        topic_node.add_subscription(Subscription { client_identifier: "1".to_string(), qos: 0 });
        topic_node.add_subscription(Subscription { client_identifier: "2".to_string(), qos: 0 });
        topic_node.add_subscription(Subscription { client_identifier: "3".to_string(), qos: 0 });

        let subscriptions = topic_node.get_subscriptions();
        assert_eq!(subscriptions.len(), 3);
    }

    #[test]
    fn test_find_or_create_leaf() {
        let mut topic_node = TopicNode {
            topic_parttern: "a".to_string(),
            subscriptions: RwLock::new(vec![]),
            leaves: Arc::new(RwLock::new(vec![])),
            retain_publish_packet: None
        };

        let new_topic_node = topic_node.find_or_create_leaf("b".to_string());
        assert_eq!(new_topic_node.read().unwrap().topic_parttern, "b");

        assert_eq!(topic_node.leaves.read().unwrap()[0].read().unwrap().topic_parttern, "b");
    }

    #[test]
    fn test_multiple_thread() {
        let topic_node = TopicNode {
            topic_parttern: "a".to_string(),
            subscriptions: RwLock::new(vec![]),
            leaves: Arc::new(RwLock::new(vec![])),
            retain_publish_packet: None
        };

        let topic_node_arc = Arc::new(RwLock::new(topic_node));
        let topic_node_arc_clone = topic_node_arc.clone();
        let topic_node_arc_read_clone = topic_node_arc.clone();
        let thread_1 = thread::spawn(move || {
            let new_topic_node = topic_node_arc.write().unwrap().find_or_create_leaf("b".to_string());
            assert_eq!(new_topic_node.read().unwrap().topic_parttern, "b");
        });
        let thread_3 = thread::spawn(move || {
            let subscriptions = topic_node_arc_read_clone.read().unwrap().get_subscriptions();
            assert_eq!(subscriptions.len(), 0);
        });
        let thread_2 = thread::spawn(move || {
            let new_topic_node = topic_node_arc_clone.write().unwrap().find_or_create_leaf("c".to_string());
            assert_eq!(new_topic_node.read().unwrap().topic_parttern, "c");
        });

        let _ = thread_1.join();
        let _ = thread_2.join();
        let _ = thread_3.join();
    }

    #[test]
    fn test_subscribe_topic() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        let _ = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 1);
        let topic_info_recorder = topic_manager.topic_info_recorder.read().unwrap();
        assert_eq!(topic_info_recorder.len(), 1);

        assert_eq!(topic_info_recorder.get("hello").unwrap().contains_key(&generate_key(&"clientA".to_string(), "a/b/c")), true);
        assert_eq!(topic_info_recorder.get("hello").unwrap().get(&generate_key(&"clientA".to_string(), "a/b/c")).unwrap(), &(0 as u8));
    }

    #[test]
    fn test_unsubscribe_topic() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        let _ = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
        let _ = topic_manager.unsubscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string());
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 0);

        let topic_info_recorder = topic_manager.topic_info_recorder.read().unwrap();
        assert_eq!(topic_info_recorder.len(), 1);

        assert_eq!(topic_info_recorder.get("hello").unwrap().contains_key(&generate_key(&"clientA".to_string(), "a/b/c")), false);
    }

    #[test]
    fn test_sharp_wildcard_subscriptions() {
        let mut topic_manager = TopicManager::new();
        let _ = topic_manager.create_tenant("hello".to_string());
        let _ = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/#".to_string(), 0);
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 1);
    }

    #[test]
    fn test_plus_wildcard_subscriptions() {
        let mut topic_manager = TopicManager::new();
        let _ = topic_manager.create_tenant("hello".to_string());
        let _ = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/+/c".to_string(), 0);
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 1);
    }

    #[test]
    fn test_multiple_subscription() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        let _ = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
        let _ = topic_manager.subscription("hello".to_string(), "clientB".to_string(), "a/b/#".to_string(), 0);
        let _ = topic_manager.subscription("hello".to_string(), "clientC".to_string(), "a/+/+".to_string(), 0);
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 3);
    }

    #[test]
    fn test_mix_wildcard_subscription() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        let _ = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
        let _ = topic_manager.subscription("hello".to_string(), "clientB".to_string(), "a/+/#".to_string(), 0);
        let _ = topic_manager.subscription("hello".to_string(), "clientC".to_string(), "a/+/+".to_string(), 0);
        let _ = topic_manager.subscription("hello".to_string(), "clientD".to_string(), "a/+/+/+".to_string(), 0);
        let _ = topic_manager.subscription("hello".to_string(), "clientE".to_string(), "a/+".to_string(), 0);
        let _ = topic_manager.subscription("hello".to_string(), "clientF".to_string(), "a/+/c".to_string(), 0);
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c/d".to_string());
        let clients = clients.unwrap();
        assert_eq!(clients.len(), 2);
        assert_eq!(clients.clone()[0].client_identifier, "clientB");
        assert_eq!(clients.clone()[1].client_identifier, "clientD");

    }

    #[test]
    fn test_invalid_topic_filter() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        let result = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/#/c".to_string(), 0);
        assert_eq!(result, Err(Error::InvalidTopicFilter("a/#/c".to_string())));
        let result = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "sport+".to_string(), 0);
        assert_eq!(result, Err(Error::InvalidTopicFilter("sport+".to_string())));
        let result = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "sport+".to_string(), 0);
        assert_eq!(result, Err(Error::InvalidTopicFilter("sport+".to_string())));
    }

    #[test]
    fn test_topic_subscription_multiple_thread() {
        let topic_manager = Arc::new(RwLock::new(TopicManager::new()));
        topic_manager.write().unwrap().create_tenant("hello".to_string());
        let topic_manager_t_1 = topic_manager.clone();
        let topic_manager_t_2 = topic_manager.clone();
        let topic_manager_t_3 = topic_manager.clone();
        let thread_1 = thread::spawn(move || {
            let _ = topic_manager_t_1.write().unwrap().subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
            let _ = topic_manager_t_1.write().unwrap().subscription("hello".to_string(), "clientB".to_string(), "a/+/#".to_string(), 0);
            let _ = topic_manager_t_1.write().unwrap().subscription("hello".to_string(), "clientC".to_string(), "a/+/+".to_string(), 0);
        });
        let thread_3 = thread::spawn(move || {
            let _ = topic_manager_t_2.write().unwrap().subscription("hello".to_string(), "clientD".to_string(), "a/+/+/+".to_string(), 0);
            let _ = topic_manager_t_2.write().unwrap().subscription("hello".to_string(), "clientE".to_string(), "a/+".to_string(), 0);
        });
        let thread_2 = thread::spawn(move || {
            let _ = topic_manager_t_3.write().unwrap().subscription("hello".to_string(), "clientF".to_string(), "a/+/c".to_string(), 0);
        });

        let _ = thread_1.join();
        let _ = thread_2.join();
        let _ = thread_3.join();

        let clients = topic_manager.write().unwrap().get_subscriptions("hello".to_string(), "a/b/c/d".to_string());
        let clients = clients.unwrap();
        assert_eq!(clients.len(), 2);
        assert_eq!(clients.clone()[0].client_identifier, "clientB");
        assert_eq!(clients.clone()[1].client_identifier, "clientD");
    }

    #[test]
    fn test_mutiple_subscription_the_same_topic_only_one_subscription() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        let _ = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
        let _ = topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 1);
    }

    #[test]
    fn test_retain_message() {
        let fix_header = FixHeader {
            packet_type: PacketType::PUBLISH,
            qos: Some(1),
            retain: Some(true),
            dup: Some(1),
            remaining_length: 8,
        };
        let variable_header = VariableHeader {
            topic_name: "a/b".to_string(),
            packet_identifier: Some(0x10),
        };

        let payload = Payload{
            payload: vec!(0x01)
        };

        let publish_packet = PublishPacket {
            fix_header,
            variable_header,
            payload
        };

        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        topic_manager.register_retain_publish_packet("hello".to_string(), "client_a".to_string(), &MqttPacketV3::Publish(publish_packet)).unwrap();
        let retain_packet = topic_manager.get_retain_publish_packet("hello".to_string(), "a/b".to_string());
        assert_eq!(1, retain_packet.unwrap().len());

        let retain_packet = topic_manager.get_retain_publish_packet("hello".to_string(), "a/+".to_string());
        assert_eq!(1, retain_packet.unwrap().len());

        let retain_packet = topic_manager.get_retain_publish_packet("hello".to_string(),  "a/#".to_string());
        assert_eq!(1, retain_packet.unwrap().len());

        let retain_packet = topic_manager.get_retain_publish_packet("hello".to_string(), "a".to_string());
        assert_eq!(0, retain_packet.unwrap().len());

    }

}