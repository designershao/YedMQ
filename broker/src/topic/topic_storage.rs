use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use base64::{engine::general_purpose, Engine};
use log::warn;
use serde::{Deserialize, Serialize};
use yedmq_mqtt::MqttPacketV3;

use crate::topic::TopicError;

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
    let binding = general_purpose::STANDARD
        .decode(&encoded_client_id)
        .unwrap();
    let client_id = std::str::from_utf8(&binding).unwrap();
    let binding = general_purpose::STANDARD.decode(&encoded_topic).unwrap();
    let topic = std::str::from_utf8(&binding).unwrap();
    (client_id.to_string(), topic.to_string())
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Subscription {
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
                .iter()
                .map(|(k, v)| (k.clone(), (**v).clone()))
                .collect(),
            retain_publish_packet: self.retain_publish_packet.as_ref().map(|p| (**p).clone()),
            leaves: self
                .leaves
                .read()
                .iter()
                .map(|(k, v)| (k.clone(), v.read().to_serializable()))
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
                    .map(|(k, v)| {
                        (
                            k,
                            Arc::new(RwLock::new(TopicStorageNode::from_serializable(v))),
                        )
                    })
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

    pub fn add_subscription(&self, subscribtion: Subscription) {
        /*
        let client_existed = self
            .subscriptions
            .read()
            .unwrap()
            .contains_key(&subscribtion.client_identifier);
        */
        self.subscriptions.write().insert(
            subscribtion.client_identifier.clone(),
            Arc::new(subscribtion),
        );
    }

    pub fn get_subscriptions(&self) -> Vec<Arc<Subscription>> {
        let mut out = Vec::with_capacity(self.subscriptions.read().len());
        for (_, subscribtion) in self.subscriptions.read().iter() {
            out.push(Arc::clone(subscribtion));
        }
        out
    }

    pub fn set_retain_publish_message(&mut self, publish_packet: MqttPacketV3) {
        self.retain_publish_packet = Some(Arc::new(publish_packet));
    }

    pub fn clean_retain_publish_message(&mut self) {
        self.retain_publish_packet = None;
    }

    pub fn remove_subscription(&self, client_identifier: &String) {
        let client_existed = self.subscriptions.read().contains_key(client_identifier);

        if client_existed {
            self.subscriptions.write().remove(client_identifier);
        }
    }

    fn get_leaf(&self, topic_pattern: String) -> Option<Arc<RwLock<TopicStorageNode>>> {
        let sub_leaf_existed = self.leaves.read().contains_key(&topic_pattern);

        if sub_leaf_existed {
            return Some(Arc::clone(&self.leaves.read()[&topic_pattern]));
        } else {
            None
        }
    }

    fn find_or_create_leaf(&self, topic_pattern: String) -> Arc<RwLock<TopicStorageNode>> {
        let sub_leaf_existed = self.leaves.read().contains_key(&topic_pattern);

        if sub_leaf_existed {
            return Arc::clone(&self.leaves.read()[&topic_pattern]);
        } else {
            let leaf = Arc::new(RwLock::new(TopicStorageNode {
                topic_parttern: topic_pattern.clone(),
                subscriptions: RwLock::new(HashMap::new()),
                leaves: Arc::new(RwLock::new(HashMap::new())),
                retain_publish_packet: None,
            }));
            let topic_node_cloned = leaf.clone();
            self.leaves.write().insert(topic_pattern, leaf.clone());
            topic_node_cloned
        }
    }
}

#[derive(Debug)]
pub struct TopicStorage {
    retain_message_recorder: RwLock<HashMap<String, HashMap<String, (String, u8)>>>,

    topic_info_recorder: RwLock<HashMap<String, HashMap<String, u8>>>,

    topic_tree: Arc<RwLock<HashMap<String, Arc<RwLock<TopicStorageNode>>>>>,
}

fn test_topic(topic: &String) -> bool {
    let i = topic.split("/");
    let mut index = 0;
    let length = i.clone().count();
    for s in i {
        index += 1;
        if s.len() > 1 {
            if s.contains('#') || s.contains('+') {
                return false;
            }
        } else if s.contains('#') && index != length {
            return false;
        }
    }
    true
}

impl Default for TopicStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicStorage {
    pub fn get_retain_message_list_with_pagination(
        &self,
        tenant_identifier: &str,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(u64, Vec<(String, String, u8)>)> {
        if !self
            .retain_message_recorder
            .read()
            .contains_key(tenant_identifier)
        {
            Err(anyhow::anyhow!(TopicError::TenantNotFound(
                tenant_identifier.to_string()
            )))
        } else {
            let retain_message_recorder = self.retain_message_recorder.read();
            let retain_message_list = retain_message_recorder.get(tenant_identifier).unwrap();
            let total = retain_message_list.len() as u64;
            let mut result = vec![];
            for (topic_filter, (client_id, qos)) in retain_message_list
                .iter()
                .skip(offset as usize)
                .take(limit as usize)
            {
                result.push((topic_filter.clone(), client_id.clone(), *qos));
            }
            Ok((total, result))
        }
    }

    pub fn get_topic_list_with_pagination(
        &self,
        tenant_id: &String,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(u64, Vec<(String, String, u8)>)> {
        let topic_info_recorder = self.topic_info_recorder.read();
        if topic_info_recorder.contains_key(tenant_id) {
            let items = topic_info_recorder.get(tenant_id).unwrap();
            let mut result_items = Vec::new();
            for (key, qos) in items.iter().skip(offset as usize).take(limit as usize) {
                let (client_id, topic) = extract_info_from_key(key);
                result_items.push((client_id, topic, *qos));
            }
            let total = items.len();
            Ok((total as u64, result_items))
        } else {
            Err(anyhow::anyhow!(TopicError::TenantNotFound(
                tenant_id.to_string()
            )))
        }
    }

    pub fn contains_tenant(&self, tenant_id: &String) -> bool {
        let topic_tree = self.topic_tree.clone();
        let topic_tree = topic_tree.read();
        topic_tree.contains_key(tenant_id)
    }

    pub fn to_snapshot(&self) -> Vec<u8> {
        let serializable = self.to_serializable();
        serde_json::to_vec(&serializable).unwrap()
    }

    pub fn from_snapshot(snapshot: Vec<u8>) -> Self {
        let serializable: SerializableTopicStorage = serde_json::from_slice(&snapshot).unwrap();
        Self::from_serializable(serializable)
    }

    fn to_serializable(&self) -> SerializableTopicStorage {
        SerializableTopicStorage {
            retain_message_recorder: self.retain_message_recorder.read().clone(),
            topic_info_recorder: self.topic_info_recorder.read().clone(),
            topic_tree: self
                .topic_tree
                .read()
                .iter()
                .map(|(k, v)| (k.clone(), v.read().to_serializable()))
                .collect(),
        }
    }

    fn from_serializable(data: SerializableTopicStorage) -> Self {
        TopicStorage {
            retain_message_recorder: RwLock::new(data.retain_message_recorder),
            topic_info_recorder: RwLock::new(data.topic_info_recorder),
            topic_tree: Arc::new(RwLock::new(
                data.topic_tree
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            Arc::new(RwLock::new(TopicStorageNode::from_serializable(v))),
                        )
                    })
                    .collect(),
            )),
        }
    }

    pub fn new() -> Self {
        let instance = Self {
            retain_message_recorder: RwLock::new(HashMap::new()),
            topic_info_recorder: RwLock::new(HashMap::new()),
            topic_tree: Arc::new(RwLock::new(HashMap::new())),
        };
        instance.create_tenant(&"public".to_string()); //create default tenant
        instance
    }

    pub fn create_tenant(&self, tenant: &String) {
        let topic_tree = self.topic_tree.clone();
        let mut topic_tree = topic_tree.write();
        if !topic_tree.contains_key(tenant) {
            topic_tree.insert(
                tenant.clone(),
                Arc::new(RwLock::new(TopicStorageNode::new("/".to_string()))),
            );
        }

        let mut topic_info_recorder = self.topic_info_recorder.write();
        if !topic_info_recorder.contains_key(tenant) {
            topic_info_recorder.insert(tenant.clone(), HashMap::new());
        }

        let mut retain_message_recorder = self.retain_message_recorder.write();
        if !retain_message_recorder.contains_key(tenant) {
            retain_message_recorder.insert(tenant.clone(), HashMap::new());
        }
    }

    pub fn subscribe(
        &self,
        tenant_id: String,
        client_identifier: String,
        topic_filter: String,
        qos: u8,
    ) -> Result<(), TopicError> {
        if !test_topic(&topic_filter) {
            return Err(TopicError::InvalidTopicFilter(topic_filter));
        }

        let topic_patterns: Vec<String> = topic_filter.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(TopicError::TenantNotFound(tenant_id))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            let result = Self::recursion_subscribe(
                tenant_topic_root,
                topic_patterns,
                client_identifier.clone(),
                qos,
            );
            if result.is_err() {
                Err(result.err().unwrap())
            } else {
                // Update the topic_info_recorder
                let mut topic_info_recorder = self.topic_info_recorder.write();
                let topic_info_recorder_optional = topic_info_recorder.get_mut(&tenant_id);

                let key = generate_key(&client_identifier, &topic_filter);

                let topic_info_state_item = topic_info_recorder_optional.unwrap();
                topic_info_state_item.entry(key).or_insert(qos);
                //

                Ok(())
            }
        }
    }

    fn recursion_subscribe(
        topic_node: Arc<RwLock<TopicStorageNode>>,
        mut topic_partterns: Vec<String>,
        client_identifier: String,
        qos: u8,
    ) -> Result<(), TopicError> {
        if !topic_partterns.is_empty() {
            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node
                .write()
                .find_or_create_leaf(topic_pattern.to_string());
            let topic_patterns_rest = topic_partterns.drain(1..).collect();
            Self::recursion_subscribe(topic_node_next, topic_patterns_rest, client_identifier, qos)
        } else {
            topic_node.write().add_subscription(Subscription {
                client_identifier,
                qos,
            });
            Ok(())
        }
    }

    pub fn unsubscribe(
        &self,
        tenant_id: &String,
        client_identifier: &String,
        topic_filter: &String,
    ) -> Result<(), TopicError> {
        let topic_patterns: Vec<String> = topic_filter.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(TopicError::TenantNotFound(tenant_id.clone()))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            let result = Self::recursion_unsubscription(
                tenant_topic_root,
                topic_patterns,
                client_identifier,
            );
            if result.is_err() {
                Err(result.err().unwrap())
            } else {
                // Update the topic_info_recorder
                let mut topic_info_recorder = self.topic_info_recorder.write();
                let topic_info_state_optional = topic_info_recorder.get_mut(tenant_id);
                let key = generate_key(client_identifier, topic_filter);
                let topic_info_state_item = topic_info_state_optional.unwrap();
                if topic_info_state_item.contains_key(&key) {
                    topic_info_state_item.remove(&key);
                }
                //

                Ok(())
            }
        }
    }

    fn recursion_unsubscription(
        topic_node: Arc<RwLock<TopicStorageNode>>,
        mut topic_partterns: Vec<String>,
        client_identifier: &String,
    ) -> Result<(), TopicError> {
        if !topic_partterns.is_empty() {
            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node.write().get_leaf(topic_pattern.to_string());
            if topic_node_next.is_none() {
                Err(TopicError::TopicNotFound(topic_pattern.to_string()))
            } else {
                let topic_patterns_rest = topic_partterns.drain(1..).collect();
                Self::recursion_unsubscription(
                    topic_node_next.unwrap(),
                    topic_patterns_rest,
                    client_identifier,
                )
            }
        } else {
            topic_node.write().remove_subscription(client_identifier);
            Ok(())
        }
    }

    pub fn get_subscriptions(
        &self,
        tenant_id: String,
        msg_topic: String,
    ) -> Result<Vec<Arc<Subscription>>, TopicError> {
        let topic_patterns: Vec<String> = msg_topic.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(TopicError::TenantNotFound(tenant_id))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            Ok(Self::recursion_get_subscriptions(
                tenant_topic_root,
                topic_patterns,
            ))
        }
    }

    fn recursion_get_subscriptions(
        topic_node: Arc<RwLock<TopicStorageNode>>,
        mut topic_partterns: Vec<String>,
    ) -> Vec<Arc<Subscription>> {
        if !topic_partterns.is_empty() {
            let mut result = vec![];
            // Get # wildcard subscriptions
            let topic_sharp_wildcard_option = topic_node.read().get_leaf("#".to_string());
            if topic_sharp_wildcard_option.is_some() {
                let sharp_wildcard_subscriptions = topic_sharp_wildcard_option
                    .unwrap()
                    .read()
                    .get_subscriptions();
                result.extend(sharp_wildcard_subscriptions);
            }
            // Get + wildcard subscriptions
            let topic_plus_wildcard_option = topic_node.read().get_leaf("+".to_string());
            if topic_plus_wildcard_option.is_some() {
                let mut topic_plus_left = topic_partterns.clone();
                let topic_plus_patterns_rest = topic_plus_left.drain(1..).collect();
                let topic_node_clone = topic_plus_wildcard_option.unwrap().clone();
                let plus_wildcard_subscriptions =
                    Self::recursion_get_subscriptions(topic_node_clone, topic_plus_patterns_rest);
                result.extend(plus_wildcard_subscriptions);
            }

            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node.read().get_leaf(topic_pattern.to_string());
            if topic_node_next.is_some() {
                let topic_patterns_rest = topic_partterns.drain(1..).collect();
                let subscriptions = Self::recursion_get_subscriptions(
                    topic_node_next.unwrap(),
                    topic_patterns_rest,
                );
                result.extend(subscriptions);
            }
            result
        } else {
            topic_node.read().get_subscriptions()
        }
    }

    fn recursion_retain_publish_packet(
        topic_node: Arc<RwLock<TopicStorageNode>>,
        mut topic_partterns: Vec<String>,
        publish_packet: MqttPacketV3,
    ) -> Result<(), TopicError> {
        if !topic_partterns.is_empty() {
            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node
                .write()
                .find_or_create_leaf(topic_pattern.to_string());
            let topic_patterns_rest = topic_partterns.drain(1..).collect();
            Self::recursion_retain_publish_packet(
                topic_node_next,
                topic_patterns_rest,
                publish_packet,
            )
        } else {
            topic_node
                .write()
                .set_retain_publish_message(publish_packet);
            Ok(())
        }
    }

    fn recursion_clean_retain_publish_packet(
        topic_node: Arc<RwLock<TopicStorageNode>>,
        mut topic_partterns: Vec<String>,
    ) -> Result<(), TopicError> {
        if !topic_partterns.is_empty() {
            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node.write().get_leaf(topic_pattern.to_string());
            if topic_node_next.is_none() {
                Err(TopicError::TopicNotFound(topic_pattern.to_string()))
            } else {
                let topic_patterns_rest = topic_partterns.drain(1..).collect();
                Self::recursion_clean_retain_publish_packet(
                    topic_node_next.unwrap(),
                    topic_patterns_rest,
                )
            }
        } else {
            topic_node.write().clean_retain_publish_message();
            Ok(())
        }
    }

    // clean retain publish packet from the topic tree
    pub fn clean_retain_publish_packet(
        &self,
        tenant_id: String,
        topic_filter: &String,
    ) -> Result<(), TopicError> {
        let topic_patterns: Vec<String> = topic_filter.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(TopicError::TenantNotFound(tenant_id))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            let result =
                Self::recursion_clean_retain_publish_packet(tenant_topic_root, topic_patterns);
            if let Err(err) = result {
                warn!(
                    "clean retain publish packet from the topic tree error: {}",
                    err
                );
                Err(err)
            } else {
                let mut retain_message_recorder = self.retain_message_recorder.write();
                let retain_message_recorder_optional = retain_message_recorder.get_mut(&tenant_id);
                retain_message_recorder_optional
                    .unwrap()
                    .remove(topic_filter);
                Ok(())
            }
        }
    }

    fn recursion_get_retain_packet(
        topic_node: Arc<RwLock<TopicStorageNode>>,
        mut topic_patterns: Vec<String>,
    ) -> Vec<Arc<MqttPacketV3>> {
        let mut result: Vec<Arc<MqttPacketV3>> = vec![];
        if !topic_patterns.is_empty() {
            let topic_pattern = &topic_patterns[0];
            if topic_pattern == &"+".to_string() || topic_pattern == &"#".to_string() {
                for sub in topic_node.write().leaves.read().iter() {
                    let topic_patterns_rest = topic_patterns.clone().drain(1..).collect();
                    result.append(&mut Self::recursion_get_retain_packet(
                        sub.1.clone(),
                        topic_patterns_rest,
                    ));
                }
            } else {
                let topic_node_next = topic_node.write().get_leaf(topic_pattern.to_string());
                if topic_node_next.is_some() {
                    let topic_patterns_rest = topic_patterns.drain(1..).collect();
                    result.append(&mut Self::recursion_get_retain_packet(
                        topic_node_next.unwrap(),
                        topic_patterns_rest,
                    ));
                }
            }
        } else {
            let topic_node = topic_node.read();
            if let Some(p) = &topic_node.retain_publish_packet {
                result.append(&mut vec![p.clone()])
            }
        }
        result
    }

    pub fn get_retain_publish_packet(
        &self,
        tenant_id: String,
        topic_filter: String,
    ) -> Result<Vec<Arc<MqttPacketV3>>, TopicError> {
        if !test_topic(&topic_filter) {
            return Err(TopicError::InvalidTopicFilter(topic_filter));
        }

        let topic_patterns: Vec<String> = topic_filter.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(TopicError::TenantNotFound(tenant_id))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            Ok(Self::recursion_get_retain_packet(
                tenant_topic_root,
                topic_patterns,
            ))
        }
    }

    // register retain publish packet to the topic tree
    pub fn register_retain_publish_packet(
        &self,
        tenant_id: String,
        source_client_identifier: String,
        publish_packet: &MqttPacketV3,
    ) -> Result<(), TopicError> {
        if let MqttPacketV3::Publish(publish_packet) = publish_packet {
            let topic_filter = publish_packet.variable_header.topic_name.clone();
            if !test_topic(&publish_packet.variable_header.topic_name) {
                return Err(TopicError::InvalidTopicFilter(topic_filter));
            }

            let topic_patterns: Vec<String> = topic_filter.split("/").map(String::from).collect();
            let map = self.topic_tree.clone();
            let tenant_topic_root_rwlock = map.read();
            let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
            if tenant_topic_root_optional.is_none() {
                Err(TopicError::TenantNotFound(tenant_id))
            } else {
                let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
                let result = Self::recursion_retain_publish_packet(
                    tenant_topic_root,
                    topic_patterns,
                    MqttPacketV3::Publish(publish_packet.clone()),
                );
                if let Err(e) = result {
                    Err(e)
                } else {
                    // Update retain recorder
                    let mut retain_message_recorder = self.retain_message_recorder.write();
                    let retain_message_recorder_optional =
                        retain_message_recorder.get_mut(&tenant_id);
                    let retain_message_recorder_item = retain_message_recorder_optional.unwrap();
                    let qos = publish_packet.fix_header.qos.unwrap_or(0);
                    retain_message_recorder_item.insert(
                        publish_packet.variable_header.topic_name.clone(),
                        (source_client_identifier, qos as u8),
                    );
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
        let tenant_topic_root_rwlock = map.read();
        tenant_topic_root_rwlock.keys().map(String::from).collect()
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
}

#[cfg(test)]
mod tests {
    use yedmq_mqtt::{
        v3::{
            fixed_header::FixHeader,
            publish::{Payload, PublishPacket, VariableHeader},
        },
        PacketType,
    };

    use super::*;
    use bytes::Bytes;
    use std::thread;

    #[test]
    fn when_subscribe_same_topic_from_other_node_should_update_subscription() {
        let topic_storage = TopicStorage::new();
        let tenant_name = "hello".to_string();
        topic_storage.create_tenant(&tenant_name);
        let _ = topic_storage.subscribe(
            tenant_name.clone(),
            "clientA".to_string(),
            "a/b/c".to_string(),
            0,
        );
        let _ = topic_storage.get_subscriptions("hello".to_string(), "a/b/c".to_string());

        let _ = topic_storage.subscribe(
            tenant_name.clone(),
            "clientA".to_string(),
            "a/b/c".to_string(),
            0,
        );
        let _ = topic_storage.get_subscriptions("hello".to_string(), "a/b/c".to_string());
    }

    #[test]
    fn test_add_subscription_and_get_subscriptions() {
        let topic_node = TopicStorageNode {
            topic_parttern: "a".to_string(),
            subscriptions: RwLock::new(HashMap::new()),
            leaves: Arc::new(RwLock::new(HashMap::new())),
            retain_publish_packet: None,
        };

        topic_node.add_subscription(Subscription {
            client_identifier: "1".to_string(),
            qos: 0,
        });
        topic_node.add_subscription(Subscription {
            client_identifier: "2".to_string(),
            qos: 0,
        });
        topic_node.add_subscription(Subscription {
            client_identifier: "3".to_string(),
            qos: 0,
        });

        let subscriptions = topic_node.get_subscriptions();
        assert_eq!(subscriptions.len(), 3);
    }

    #[test]
    fn test_find_or_create_leaf() {
        let topic_node = TopicStorageNode {
            topic_parttern: "a".to_string(),
            subscriptions: RwLock::new(HashMap::new()),
            leaves: Arc::new(RwLock::new(HashMap::new())),
            retain_publish_packet: None,
        };

        let new_topic_node = topic_node.find_or_create_leaf("b".to_string());
        assert_eq!(new_topic_node.read().topic_parttern, "b");

        assert!(topic_node.leaves.read().contains_key("b"));
    }

    #[test]
    fn test_multiple_thread() {
        let topic_node = TopicStorageNode {
            topic_parttern: "a".to_string(),
            subscriptions: RwLock::new(HashMap::new()),
            leaves: Arc::new(RwLock::new(HashMap::new())),
            retain_publish_packet: None,
        };

        let topic_node_arc = Arc::new(RwLock::new(topic_node));
        let topic_node_arc_clone = topic_node_arc.clone();
        let topic_node_arc_read_clone = topic_node_arc.clone();
        let thread_1 = thread::spawn(move || {
            let new_topic_node = topic_node_arc.write().find_or_create_leaf("b".to_string());
            assert_eq!(new_topic_node.read().topic_parttern, "b");
        });
        let thread_3 = thread::spawn(move || {
            let subscriptions = topic_node_arc_read_clone.read().get_subscriptions();
            assert_eq!(subscriptions.len(), 0);
        });
        let thread_2 = thread::spawn(move || {
            let new_topic_node = topic_node_arc_clone
                .write()
                .find_or_create_leaf("c".to_string());
            assert_eq!(new_topic_node.read().topic_parttern, "c");
        });

        let _ = thread_1.join();
        let _ = thread_2.join();
        let _ = thread_3.join();
    }

    #[test]
    fn test_subscribe_topic() {
        let topic_storage = TopicStorage::new();
        let tenant_name = "hello".to_string();
        topic_storage.create_tenant(&tenant_name);
        let _ = topic_storage.subscribe(tenant_name, "clientA".to_string(), "a/b/c".to_string(), 0);
        let clients = topic_storage.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 1);
        let topic_info_recorder = topic_storage.topic_info_recorder.read();
        assert_eq!(topic_info_recorder.len(), 1);

        assert_eq!(
            topic_info_recorder
                .get("hello")
                .unwrap()
                .contains_key(&generate_key(&"clientA".to_string(), "a/b/c")),
            true
        );
        assert_eq!(
            topic_info_recorder
                .get("hello")
                .unwrap()
                .get(&generate_key(&"clientA".to_string(), "a/b/c"))
                .unwrap(),
            &(0 as u8)
        );
    }

    #[test]
    fn test_unsubscribe_topic() {
        let topic_storage = TopicStorage::new();
        let tenant_name = "hello".to_string();
        topic_storage.create_tenant(&tenant_name);
        let _ = topic_storage.subscribe(
            tenant_name.clone(),
            "clientA".to_string(),
            "a/b/c".to_string(),
            0,
        );
        let _ =
            topic_storage.unsubscribe(&tenant_name, &"clientA".to_string(), &"a/b/c".to_string());
        let clients = topic_storage.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 0);

        let topic_info_recorder = topic_storage.topic_info_recorder.read();
        assert_eq!(topic_info_recorder.len(), 1);

        assert_eq!(
            topic_info_recorder
                .get("hello")
                .unwrap()
                .contains_key(&generate_key(&"clientA".to_string(), "a/b/c")),
            false
        );
    }

    #[test]
    fn test_sharp_wildcard_subscriptions() {
        let topic_storage = TopicStorage::new();
        let tenant_name = "hello".to_string();
        topic_storage.create_tenant(&tenant_name);
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientA".to_string(),
            "a/b/#".to_string(),
            0,
        );
        let clients = topic_storage.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 1);
    }

    #[test]
    fn test_plus_wildcard_subscriptions() {
        let topic_storage = TopicStorage::new();
        let tenant_name = "hello".to_string();
        topic_storage.create_tenant(&tenant_name);
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientA".to_string(),
            "a/+/c".to_string(),
            0,
        );
        let clients = topic_storage.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 1);
    }

    #[test]
    fn test_multiple_subscription() {
        let topic_storage = TopicStorage::new();
        let tenant_name = "hello".to_string();
        topic_storage.create_tenant(&tenant_name);
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientA".to_string(),
            "a/b/c".to_string(),
            0,
        );
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientB".to_string(),
            "a/b/#".to_string(),
            0,
        );
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientC".to_string(),
            "a/+/+".to_string(),
            0,
        );
        let clients = topic_storage.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 3);
    }

    #[test]
    fn test_mix_wildcard_subscription() {
        let topic_storage = TopicStorage::new();
        let tenant_name = "hello".to_string();
        topic_storage.create_tenant(&tenant_name);
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientA".to_string(),
            "a/b/c".to_string(),
            0,
        );
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientB".to_string(),
            "a/+/#".to_string(),
            0,
        );
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientC".to_string(),
            "a/+/+".to_string(),
            0,
        );
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientD".to_string(),
            "a/+/+/+".to_string(),
            0,
        );
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientE".to_string(),
            "a/+".to_string(),
            0,
        );
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientF".to_string(),
            "a/+/c".to_string(),
            0,
        );
        let clients = topic_storage.get_subscriptions("hello".to_string(), "a/b/c/d".to_string());
        let clients = clients.unwrap();
        assert_eq!(clients.len(), 2);
        assert_eq!(clients.clone()[0].client_identifier, "clientB");
        assert_eq!(clients.clone()[1].client_identifier, "clientD");
    }

    #[test]
    fn test_invalid_topic_filter() {
        let topic_storage = TopicStorage::new();
        let tenant_name = "hello".to_string();
        topic_storage.create_tenant(&tenant_name);
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientA".to_string(),
            "a/#/c".to_string(),
            0,
        );

        let result = topic_storage.subscribe(
            "hello".to_string(),
            "clientA".to_string(),
            "sport+".to_string(),
            0,
        );

        match result.unwrap_err() {
            TopicError::InvalidTopicFilter(msg) => {
                if msg != "sport+".to_string() {
                    assert!(false);
                } else {
                    assert!(true)
                }
            }
            _ => assert!(false),
        }

        let result = topic_storage.subscribe(
            "hello".to_string(),
            "clientA".to_string(),
            "sport+".to_string(),
            0,
        );

        match result.unwrap_err() {
            TopicError::InvalidTopicFilter(msg) => {
                if msg != "sport+".to_string() {
                    assert!(false);
                } else {
                    assert!(true)
                }
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn test_topic_subscription_multiple_thread() {
        let topic_storage = Arc::new(RwLock::new(TopicStorage::new()));
        let tenant_name = "hello".to_string();
        topic_storage.write().create_tenant(&tenant_name);
        let topic_storage_t_1 = topic_storage.clone();
        let topic_storage_t_2 = topic_storage.clone();
        let topic_storage_t_3 = topic_storage.clone();
        let thread_1 = thread::spawn(move || {
            let _ = topic_storage_t_1.write().subscribe(
                "hello".to_string(),
                "clientA".to_string(),
                "a/b/c".to_string(),
                0,
            );
            let _ = topic_storage_t_1.write().subscribe(
                "hello".to_string(),
                "clientB".to_string(),
                "a/+/#".to_string(),
                0,
            );
            let _ = topic_storage_t_1.write().subscribe(
                "hello".to_string(),
                "clientC".to_string(),
                "a/+/+".to_string(),
                0,
            );
        });
        let thread_3 = thread::spawn(move || {
            let _ = topic_storage_t_2.write().subscribe(
                "hello".to_string(),
                "clientD".to_string(),
                "a/+/+/+".to_string(),
                0,
            );
            let _ = topic_storage_t_2.write().subscribe(
                "hello".to_string(),
                "clientE".to_string(),
                "a/+".to_string(),
                0,
            );
        });
        let thread_2 = thread::spawn(move || {
            let _ = topic_storage_t_3.write().subscribe(
                "hello".to_string(),
                "clientF".to_string(),
                "a/+/c".to_string(),
                0,
            );
        });

        let _ = thread_1.join();
        let _ = thread_2.join();
        let _ = thread_3.join();

        let clients = topic_storage
            .write()
            .get_subscriptions("hello".to_string(), "a/b/c/d".to_string());
        let clients = clients.unwrap();
        assert_eq!(clients.len(), 2);
        assert_eq!(clients.clone()[0].client_identifier, "clientB");
        assert_eq!(clients.clone()[1].client_identifier, "clientD");
    }

    #[test]
    fn test_mutiple_subscription_the_same_topic_only_one_subscription() {
        let topic_storage = TopicStorage::new();
        let tenant_name = "hello".to_string();
        topic_storage.create_tenant(&tenant_name);
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientA".to_string(),
            "a/b/c".to_string(),
            0,
        );
        let _ = topic_storage.subscribe(
            "hello".to_string(),
            "clientA".to_string(),
            "a/b/c".to_string(),
            0,
        );
        let clients = topic_storage.get_subscriptions("hello".to_string(), "a/b/c".to_string());
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

        let payload = Payload {
            payload: Bytes::copy_from_slice(vec![0x01].as_slice()),
        };

        let publish_packet = PublishPacket {
            fix_header,
            variable_header,
            payload,
        };

        let topic_storage = TopicStorage::new();
        let tenant_name = "hello".to_string();
        topic_storage.create_tenant(&tenant_name);
        topic_storage
            .register_retain_publish_packet(
                "hello".to_string(),
                "client_a".to_string(),
                &MqttPacketV3::Publish(publish_packet),
            )
            .unwrap();
        let retain_packet =
            topic_storage.get_retain_publish_packet("hello".to_string(), "a/b".to_string());
        assert_eq!(1, retain_packet.unwrap().len());

        let retain_packet =
            topic_storage.get_retain_publish_packet("hello".to_string(), "a/+".to_string());
        assert_eq!(1, retain_packet.unwrap().len());

        let retain_packet =
            topic_storage.get_retain_publish_packet("hello".to_string(), "a/#".to_string());
        assert_eq!(1, retain_packet.unwrap().len());

        let retain_packet =
            topic_storage.get_retain_publish_packet("hello".to_string(), "a".to_string());
        assert_eq!(0, retain_packet.unwrap().len());
    }
}
