use std::{collections::HashMap, sync::{Arc, RwLock}, time::Duration};

use serde::{Deserialize, Serialize};
use yedmq_mqtt::MqttPacketV3;

use crate::inflight::{self, Inflight};

use super::session_actor::QoS;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionState {
    pub pending_messages: Vec<MqttPacketV3>,

    pub inflight: Inflight,

    pub subscriptions: HashMap<String, QoS>,
}

impl SessionState {
    pub fn new(inflight_duration: Duration) -> Self {
        SessionState {
            pending_messages: Vec::new(),
            inflight: Inflight::new(inflight_duration),
            subscriptions: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct SessionStateStorage {
    inner: HashMap<String, HashMap<String, Arc<RwLock<SessionState>>>>,
}

impl SessionStateStorage {
    pub fn new () -> Self {
        SessionStateStorage {
            inner: HashMap::new(),
        }
    }

    fn to_serializable(&self) -> SerializableSessionStateStorage {
        SerializableSessionStateStorage {
            inner: self
                .inner
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.iter()
                            .map(|(k, v)| (k.clone(), v.read().unwrap().clone()))
                            .collect(),
                    )
                })
                .collect(),
        }
    }

    fn from_serializable(data: SerializableSessionStateStorage) -> Self {
        SessionStateStorage { inner: data.inner.iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.iter()
                        .map(|(k, v)| (k.clone(), Arc::new(RwLock::new(v.clone()))))
                        .collect(),
                )
            }).collect()
         }
    }

    pub fn to_snapshot(&self) -> Vec<u8> {
        let serializable = self.to_serializable();
        serde_json::to_vec(&serializable).unwrap()
    }

    pub fn from_snapshot(snapshot: Vec<u8>) -> Self {
        let serializable: SerializableSessionStateStorage =
            serde_json::from_slice(&snapshot).unwrap();
        Self::from_serializable(serializable)
    }
}

#[derive(Serialize, Deserialize)]
struct SerializableSessionStateStorage {
    inner: HashMap<String, HashMap<String, SessionState>>,
}
