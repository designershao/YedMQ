use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::raft::NodeId;

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionActorMapStorage {
    inner: HashMap<String,HashMap<String, NodeId>>
}

impl SessionActorMapStorage {
    pub fn new() -> Self {
        SessionActorMapStorage {
            inner: HashMap::new()
        }
    }

    pub fn register_session_actor(&mut self, tenant_id: String, session_id: String, node_id: NodeId) {
        self.inner.entry(tenant_id).or_insert(HashMap::new()).insert(session_id, node_id);
    }

    pub fn unregister_session_actor(&mut self, tenant_id: String, session_id: String) {
        self.inner.get_mut(&tenant_id).unwrap().remove(&session_id);
    }

    fn to_serializable(&self) -> SerializableSessionActorMapStorage {
        SerializableSessionActorMapStorage {
            inner: self.inner.clone()
        }
    }

    fn from_serializable(data: SerializableSessionActorMapStorage) -> Self {
        SessionActorMapStorage {
            inner: data.inner
        }
    }

    pub fn to_snapshot(&self) -> Vec<u8> {
        let serializable = self.to_serializable();
        serde_json::to_vec(&serializable).unwrap()
    }

    pub fn from_snapshot(snapshot: Vec<u8>) -> Self {
        let serializable: SerializableSessionActorMapStorage = serde_json::from_slice(&snapshot).unwrap();
        Self::from_serializable(serializable)
    }
}


#[derive(Serialize, Deserialize)]
struct SerializableSessionActorMapStorage {
    inner: HashMap<String, HashMap<String, NodeId>>
}