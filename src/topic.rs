use core::fmt;
use std::{string, sync::{Arc, Mutex, RwLock}, collections::HashMap};

use crate::protocol::{MqttPacketV3, v3::publish::PublishPacket};

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


pub struct TopicManager {

    topic_tree: Arc<RwLock<HashMap<String, Arc<RwLock<TopicNode>>>>>,

}

impl TopicManager {
    // generate mqtt topic test regex


    pub fn new() -> Self {
        Self {
            topic_tree: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_tenant(&mut self, tenant:String) {
        let topic_tree = self.topic_tree.clone();
        let mut topic_tree = topic_tree.write().unwrap();
        if !topic_tree.contains_key(&tenant) {
            topic_tree.insert(tenant, Arc::new(RwLock::new(TopicNode::new("/".to_string()))));
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
            Self::recursion_subscription(tenant_topic_root, topic_patterns, client_identifier, qos)
        }
    }

    fn recursion_subscription(topic_node:Arc<RwLock<TopicNode>>, mut topic_partterns: Vec<String>, client_identifier:String, qos: u8) -> Result<(), Error> {
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
            Self::recursion_unsubscription(tenant_topic_root, topic_patterns, client_identifier)
        }
    }

    fn recursion_unsubscription(topic_node:Arc<RwLock<TopicNode>>, mut topic_partterns: Vec<String>, client_identifier:String) -> Result<(), Error> {
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

    fn recursion_clean_retain_publish_packet(topic_node:Arc<RwLock<TopicNode>>,mut topic_partterns:Vec<String>, client_identifier:String)  -> Result<(), Error> {
        if topic_partterns.len() > 0 {
            let topic_pattern = &topic_partterns[0];
            let topic_node_next = topic_node.write().unwrap().get_leaf(topic_pattern.to_string());
            if topic_node_next.is_none() {
                Err(Error::TopicNotFound(topic_pattern.to_string()))
            } else {
                let topic_patterns_rest = topic_partterns.drain(1..).collect();
                Self::recursion_clean_retain_publish_packet(topic_node_next.unwrap(), topic_patterns_rest, client_identifier)
            }
        } else {
            topic_node.write().unwrap().clean_retain_publish_message();
            Ok(())
        }
    }

    // clean retain publish packet from the topic tree
    pub fn clean_retain_publish_packet(&mut self, tenant_id:String, client_identifier:String,topic_filter:&String) -> Result<(), Error> {
        let topic_patterns:Vec<String> = topic_filter.split("/").map(String::from).collect();
        let map = self.topic_tree.clone();
        let tenant_topic_root_rwlock = map.read().unwrap();
        let tenant_topic_root_optional = tenant_topic_root_rwlock.get(&tenant_id);
        if tenant_topic_root_optional.is_none() {
            Err(Error::TenantNotFound(tenant_id))
        } else {
            let tenant_topic_root = tenant_topic_root_optional.unwrap().clone();
            Self::recursion_clean_retain_publish_packet(tenant_topic_root, topic_patterns,client_identifier)
        }
    }

    fn recursion_get_retain_packet(topic_node:Arc<RwLock<TopicNode>>, mut topic_patterns:Vec<String>, client_identifier:String) -> Vec<Arc<MqttPacketV3>> {
        let mut result:Vec<Arc<MqttPacketV3>> = vec![];
        if topic_patterns.len() > 0 {
            let topic_pattern = &topic_patterns[0];
            if topic_pattern == &"+".to_string() || topic_pattern == &"#".to_string() {
                for sub in topic_node.write().unwrap().leaves.read().unwrap().iter() {
                    let topic_patterns_rest = topic_patterns.clone().drain(1..).collect();
                    result.append(&mut Self::recursion_get_retain_packet(sub.clone(), topic_patterns_rest, client_identifier.clone()));
                }
            } else {
                let topic_node_next = topic_node.write().unwrap().get_leaf(topic_pattern.to_string());
                if !topic_node_next.is_none() {
                    let topic_patterns_rest = topic_patterns.drain(1..).collect();
                    result.append(
                        &mut Self::recursion_get_retain_packet(topic_node_next.unwrap(), topic_patterns_rest, client_identifier)
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

    pub fn get_retain_publish_packet(&mut self, tenant_id:String, client_identifier:String, topic_filter: String) -> Result<Vec<Arc<MqttPacketV3>>, Error> {
        let map = self.topic_tree.clone();
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
            Ok(Self::recursion_get_retain_packet(tenant_topic_root, topic_patterns, client_identifier))
        }
    }

    // register retain publish packet to the topic tree
    pub fn register_retain_publish_packet(&mut self, tenant_id:String, publish_packet:&MqttPacketV3) -> Result<(), Error> {
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
                Self::recursion_retain_publish_packet(tenant_topic_root, topic_patterns,MqttPacketV3::Publish(publish_packet.clone()))
            }
        } else {
            Ok(())
        }
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
        self.subscriptions.write().unwrap().push(Arc::new(subscribtion));
        self.subscriptions.write().unwrap().sort_by(|sub_a, sub_b| {
            sub_a.client_identifier.cmp(&sub_b.client_identifier)
        });
    }

    pub fn remove_subscription(&mut self, client_identifier:String) {

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

#[derive(Debug)]
pub struct Subscription {
    pub client_identifier: String,
    pub qos: u8,
}

#[cfg(test)]
mod tests {

    use crate::protocol::{v3::{fixed_header::FixHeader, publish::{VariableHeader, Payload}}, PacketType};

    use super::*;
    use std::{thread, borrow::BorrowMut, cell::RefCell};

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

        thread_1.join();
        thread_2.join();
        thread_3.join();
    }

    #[test]
    fn test_subscribe_topic() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 1);
    }

    #[test]
    fn test_unsubscribe_topic() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
        topic_manager.unsubscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string());
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 0);
    }

    #[test]
    fn test_sharp_wildcard_subscriptions() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/#".to_string(), 0);
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 1);
    }

    #[test]
    fn test_plus_wildcard_subscriptions() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/+/c".to_string(), 0);
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 1);
    }

    #[test]
    fn test_multiple_subscription() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
        topic_manager.subscription("hello".to_string(), "clientB".to_string(), "a/b/#".to_string(), 0);
        topic_manager.subscription("hello".to_string(), "clientC".to_string(), "a/+/+".to_string(), 0);
        let clients = topic_manager.get_subscriptions("hello".to_string(), "a/b/c".to_string());
        assert_eq!(clients.unwrap().len(), 3);
    }

    #[test]
    fn test_mix_wildcard_subscription() {
        let mut topic_manager = TopicManager::new();
        topic_manager.create_tenant("hello".to_string());
        topic_manager.subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
        topic_manager.subscription("hello".to_string(), "clientB".to_string(), "a/+/#".to_string(), 0);
        topic_manager.subscription("hello".to_string(), "clientC".to_string(), "a/+/+".to_string(), 0);
        topic_manager.subscription("hello".to_string(), "clientD".to_string(), "a/+/+/+".to_string(), 0);
        topic_manager.subscription("hello".to_string(), "clientE".to_string(), "a/+".to_string(), 0);
        topic_manager.subscription("hello".to_string(), "clientF".to_string(), "a/+/c".to_string(), 0);
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
        let mut topic_manager_t_1 = topic_manager.clone();
        let mut topic_manager_t_2 = topic_manager.clone();
        let mut topic_manager_t_3 = topic_manager.clone();
        let thread_1 = thread::spawn(move || {
            print!("1");
            topic_manager_t_1.write().unwrap().subscription("hello".to_string(), "clientA".to_string(), "a/b/c".to_string(), 0);
            print!("2");
            topic_manager_t_1.write().unwrap().subscription("hello".to_string(), "clientB".to_string(), "a/+/#".to_string(), 0);
            print!("3");
            topic_manager_t_1.write().unwrap().subscription("hello".to_string(), "clientC".to_string(), "a/+/+".to_string(), 0);
            print!("4");
        });
        let thread_3 = thread::spawn(move || {
            print!("5");
            topic_manager_t_2.write().unwrap().subscription("hello".to_string(), "clientD".to_string(), "a/+/+/+".to_string(), 0);
            print!("6");
            topic_manager_t_2.write().unwrap().subscription("hello".to_string(), "clientE".to_string(), "a/+".to_string(), 0);
            print!("7");
        });
        let thread_2 = thread::spawn(move || {
            print!("8");
            topic_manager_t_3.write().unwrap().subscription("hello".to_string(), "clientF".to_string(), "a/+/c".to_string(), 0);
            print!("9");
        });

        thread_1.join();
        thread_2.join();
        thread_3.join();

        let clients = topic_manager.write().unwrap().get_subscriptions("hello".to_string(), "a/b/c/d".to_string());
        let clients = clients.unwrap();
        assert_eq!(clients.len(), 2);
        assert_eq!(clients.clone()[0].client_identifier, "clientB");
        assert_eq!(clients.clone()[1].client_identifier, "clientD");
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
        topic_manager.register_retain_publish_packet("hello".to_string(), &MqttPacketV3::Publish(publish_packet)).unwrap();
        let retain_packet = topic_manager.get_retain_publish_packet("hello".to_string(), "clientA".to_string(), "a/b".to_string());
        assert_eq!(1, retain_packet.unwrap().len());

        let retain_packet = topic_manager.get_retain_publish_packet("hello".to_string(), "clientA".to_string(), "a/+".to_string());
        assert_eq!(1, retain_packet.unwrap().len());

        let retain_packet = topic_manager.get_retain_publish_packet("hello".to_string(), "clientA".to_string(), "a/#".to_string());
        assert_eq!(1, retain_packet.unwrap().len());

        let retain_packet = topic_manager.get_retain_publish_packet("hello".to_string(), "clientA".to_string(), "a".to_string());
        assert_eq!(0, retain_packet.unwrap().len());

    }

}