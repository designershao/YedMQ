use std::{collections::{HashMap, HashSet}, fmt, sync::{Arc, RwLock}};

use base64::{engine::general_purpose, Engine};
use log::warn;
use serde::{Deserialize, Serialize};
use yedmq_mqtt::MqttPacketV3;

use crate::raft::{Node, NodeId};

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


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Subscription {
    pub node_id: NodeId,
    pub client_identifier: String,
    pub qos: u8,
}

#[derive(Debug, Serialize, Deserialize)]
struct TopicStorageNode {

    pub topic_parttern: String,

    subscriptions: RwLock<HashMap<String, Arc<Subscription>>>,

    retain_publish_packet: Option<Arc<MqttPacketV3>>,

    leaves: Arc<RwLock<HashMap<String, Arc<RwLock<TopicStorageNode>>>>>,
}

impl TopicStorageNode {   
    fn to_serializable(&self) -> SerializableTopicStorageNode {
        SerializableTopicStorageNode {
            topic_parttern: self.topic_parttern.clone(),
            subscriptions: self
                .subscriptions
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), (**v).clone()))
                .collect(),
            retain_publish_packet: self.retain_publish_packet.as_ref().map(|p| (**p).clone()),
            leaves: self
                .leaves
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.read().unwrap().to_serializable()))
                .collect(),
        }
    }

    fn from_serializable(data: SerializableTopicStorageNode) -> Self {
        TopicStorageNode {
            topic_parttern: data.topic_parttern,
            subscriptions: RwLock::new(
                data.subscriptions
                    .into_iter()
                    .map(|(k, v)| (k, Arc::new(v)))
                    .collect(),
            ),
            retain_publish_packet: data.retain_publish_packet.map(Arc::new),
            leaves: Arc::new(RwLock::new(
                data.leaves
                    .into_iter()
                    .map(|(k, v)| (k, Arc::new(RwLock::new(TopicStorageNode::from_serializable(v)))))
                    .collect(),
            )),
        }
    }

    pub fn new(topic_pattern: String) -> Self {
        TopicStorageNode {
            topic_parttern: topic_pattern,
            subscriptions: RwLock::new(HashMap::new()),
            retain_publish_packet: None,
            leaves: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_subscription(&mut self, subscribtion:Subscription) {
        let client_existed = self.subscriptions.read().unwrap().contains_key(&subscribtion.client_identifier);
        if !client_existed {
            self.subscriptions.write().unwrap().insert(subscribtion.client_identifier.clone(), Arc::new(subscribtion));
        }
    }

    pub fn get_subscriptions(&self) -> Vec<Arc<Subscription>> {
        let mut out = Vec::with_capacity(self.subscriptions.read().unwrap().len());
        for (_, subscribtion) in self.subscriptions.read().unwrap().iter() {
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

    pub fn remove_subscription(&mut self, client_identifier:&String) {
        let client_existed = self.subscriptions.read().unwrap().contains_key(client_identifier);

        if client_existed  {
            self.subscriptions.write().unwrap().remove(client_identifier);
        }
    }

    fn get_leaf(&self, topic_pattern:String) -> Option<Arc<RwLock<TopicStorageNode>>> {
        let sub_leaf_existed = self.leaves.read().unwrap().contains_key(&topic_pattern);

        if sub_leaf_existed {
            return Some(Arc::clone(&self.leaves.read().unwrap()[&topic_pattern]));
        } else {
            None
        }
    }

    fn find_or_create_leaf(&mut self, topic_pattern: String) -> Arc<RwLock<TopicStorageNode>> {
        let sub_leaf_existed = self.leaves.read().unwrap().contains_key(&topic_pattern);

        if sub_leaf_existed {
            return Arc::clone(&self.leaves.read().unwrap()[&topic_pattern]);
        } else {
            let leaf = Arc::new(RwLock::new(TopicStorageNode {
                topic_parttern: topic_pattern.clone(),
                subscriptions: RwLock::new(HashMap::new()),
                leaves: Arc::new(RwLock::new(HashMap::new())),
                retain_publish_packet: None
            }));
            let topic_node_cloned = leaf.clone();
            self.leaves.write().unwrap().insert(topic_pattern, leaf.clone());
            topic_node_cloned
        }
    }


}

pub struct TopicStorage {

    retain_message_recorder: RwLock<HashMap<String, HashMap<String, (String, u8)>>>,

    topic_info_recorder: RwLock<HashMap<String, HashMap<String, u8>>>,

    topic_tree: Arc<RwLock<HashMap<String, Arc<RwLock<TopicStorageNode>>>>>,

    // Represent node has topic
    // Map<NodeId, Set<(topic, tenant_id, client_id)>
    topic_nodes: Arc<RwLock<HashMap<NodeId, HashSet<(String, String, String)>>>>
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


impl TopicStorage {

    fn to_serializable(&self) -> SerializableTopicStorage {
        SerializableTopicStorage {
            retain_message_recorder: self.retain_message_recorder.read().unwrap().clone(),
            topic_info_recorder: self.topic_info_recorder.read().unwrap().clone(),
            topic_tree: self
                .topic_tree
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.read().unwrap().to_serializable()))
                .collect(),
            topic_nodes: self.topic_nodes.read().unwrap().clone(),
        }
    }

    fn from_serializable(data: SerializableTopicStorage) -> Self {
        TopicStorage {
            retain_message_recorder: RwLock::new(data.retain_message_recorder),
            topic_info_recorder: RwLock::new(data.topic_info_recorder),
            topic_tree: Arc::new(RwLock::new(
                data.topic_tree
                    .into_iter()
                    .map(|(k, v)| (k, Arc::new(RwLock::new(TopicStorageNode::from_serializable(v)))))
                    .collect(),
            )),
            topic_nodes: Arc::new(RwLock::new(data.topic_nodes)),
        }
    }

    pub fn new() -> Self {
        Self {
            retain_message_recorder: RwLock::new(HashMap::new()),
            topic_info_recorder: RwLock::new(HashMap::new()),
            topic_tree: Arc::new(RwLock::new(HashMap::new())),
            topic_nodes: Arc::new(RwLock::new(HashMap::new()))
        }
    }

    pub fn create_tenant(&mut self, tenant:&String) {
        let topic_tree = self.topic_tree.clone();
        let mut topic_tree = topic_tree.write().unwrap();
        if !topic_tree.contains_key(tenant) {
            topic_tree.insert(tenant.clone(), Arc::new(RwLock::new(TopicStorageNode::new("/".to_string()))));
        }

        let mut topic_info_recorder = self.topic_info_recorder.write().unwrap();
        if !topic_info_recorder.contains_key(tenant) {
            topic_info_recorder.insert(tenant.clone(), HashMap::new());
        }

        let mut retain_message_recorder = self.retain_message_recorder.write().unwrap();
        if !retain_message_recorder.contains_key(tenant) {
            retain_message_recorder.insert(tenant.clone(), HashMap::new());
        }
    }

    pub fn subscription(&mut self, tenant_id:String, client_identifier:String, topic_filter: String, qos: u8, node_id:NodeId) -> Result<(), Error> {

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
            let result = Self::recursion_subscription(tenant_topic_root, topic_patterns, client_identifier.clone(), qos, node_id);
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

                // Add node_id to topic_nodes
                let mut topic_nodes = self.topic_nodes.write().unwrap();
                let topic_nodes_optional = topic_nodes.get_mut(&node_id);
                if topic_nodes_optional.is_none() {
                    topic_nodes.insert(node_id, HashSet::new());
                }
                let topic_nodes = topic_nodes.get_mut(&node_id).unwrap();
                topic_nodes.insert((topic_filter, tenant_id, client_identifier));
                //

                Ok(())
            }

        }
    }

    fn recursion_subscription(topic_node:Arc<RwLock<TopicStorageNode>>, mut topic_partterns: Vec<String>, client_identifier: String, qos: u8, node_id:NodeId) -> Result<(), Error> {
        if topic_partterns.len() > 0 {
            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node.write().unwrap().find_or_create_leaf(topic_pattern.to_string());
            let topic_patterns_rest = topic_partterns.drain(1..).collect();
            Self::recursion_subscription(topic_node_next, topic_patterns_rest, client_identifier, qos, node_id)
        } else {
            topic_node.write().unwrap().add_subscription(Subscription{client_identifier, qos, node_id});
            Ok(())
        }
    }

    pub fn unsubscription(&mut self, tenant_id:&String, client_identifier:&String, topic_filter: &String, node_id:&NodeId) -> Result<(), Error> {
        let topic_patterns:Vec<String> = topic_filter.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read().unwrap();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(Error::TenantNotFound(tenant_id.clone()))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            let result = Self::recursion_unsubscription(tenant_topic_root, topic_patterns, &client_identifier);
            if result.is_err() {
                return Err(result.err().unwrap());
            } else {
                // Update the topic_info_recorder
                let mut topic_info_recorder = self.topic_info_recorder.write().unwrap();
                let topic_info_state_optional = topic_info_recorder.get_mut(tenant_id);
                let key = generate_key(&client_identifier, &topic_filter);
                let topic_info_state_item = topic_info_state_optional.unwrap();
                if topic_info_state_item.contains_key(&key) {
                    topic_info_state_item.remove(&key);
                }
                //

                // Remove node_id from topic_nodes
                let mut topic_nodes = self.topic_nodes.write().unwrap();
                let topic_nodes_optional = topic_nodes.get_mut(&node_id);
                if !topic_nodes_optional.is_none() {
                    let topic_nodes = topic_nodes.get_mut(&node_id).unwrap();
                    topic_nodes.remove(&(topic_filter.to_string(), tenant_id.to_string(), client_identifier.to_string()));
                }
                //
                Ok(())
            }
        }
    }

    fn recursion_unsubscription(topic_node:Arc<RwLock<TopicStorageNode>>, mut topic_partterns: Vec<String>, client_identifier:&String) -> Result<(), Error> {
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

    fn recursion_get_subscriptions(topic_node:Arc<RwLock<TopicStorageNode>>, mut topic_partterns: Vec<String>) -> Vec<Arc<Subscription>> {
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

    fn recursion_retain_publish_packet(topic_node:Arc<RwLock<TopicStorageNode>>, mut topic_partterns: Vec<String>, publish_packet:MqttPacketV3) -> Result<(), Error> {
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

    fn recursion_clean_retain_publish_packet(topic_node:Arc<RwLock<TopicStorageNode>>,mut topic_partterns:Vec<String>)  -> Result<(), Error> {
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

    fn recursion_get_retain_packet(topic_node:Arc<RwLock<TopicStorageNode>>, mut topic_patterns:Vec<String>) -> Vec<Arc<MqttPacketV3>> {
        let mut result:Vec<Arc<MqttPacketV3>> = vec![];
        if topic_patterns.len() > 0 {
            let topic_pattern = &topic_patterns[0];
            if topic_pattern == &"+".to_string() || topic_pattern == &"#".to_string() {
                for sub in topic_node.write().unwrap().leaves.read().unwrap().iter() {
                    let topic_patterns_rest = topic_patterns.clone().drain(1..).collect();
                    result.append(&mut Self::recursion_get_retain_packet(sub.1.clone(), topic_patterns_rest));
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

    // Remove node all subscriptions
    pub fn remove_node(&mut self, node_id: NodeId) {
        let mut topic_items = vec![];
        {
            let topic_nodes = self.topic_nodes.write().unwrap();
            let topics = topic_nodes.get(&node_id);
            if topics.is_some() {
                let topics = topics.unwrap();
                for (topic_filter, tenant_id, client_identifier) in topics {
                    topic_items.push((topic_filter.clone(), tenant_id.clone(), client_identifier.clone()));
                }
            }
        }
        if topic_items.len() > 0 {
            for (topic_filter, tenant_id, client_identifier) in topic_items {
                let _ = self.unsubscription(&tenant_id, &client_identifier, &topic_filter, &node_id);
            }
            let mut topic_nodes = self.topic_nodes.write().unwrap();
            topic_nodes.remove(&node_id);
        }
    }

}



#[derive(Serialize, Deserialize)]
struct SerializableTopicStorageNode {
    topic_parttern: String,
    subscriptions: HashMap<String, Subscription>,
    retain_publish_packet: Option<MqttPacketV3>,
    leaves: HashMap<String, SerializableTopicStorageNode>,
}

#[derive(Serialize, Deserialize)]
struct SerializableTopicStorage {
    retain_message_recorder: HashMap<String, HashMap<String, (String, u8)>>,
    topic_info_recorder: HashMap<String, HashMap<String, u8>>,
    topic_tree: HashMap<String, SerializableTopicStorageNode>,
    topic_nodes: HashMap<NodeId, HashSet<(String, String, String)>>,
}