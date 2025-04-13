use std::{collections::HashMap, path::Display, sync::atomic::{AtomicU64, Ordering}};

use serde::{Deserialize, Serialize};

use crate::raft::{session_actor_map, NodeId};

#[derive(Debug,thiserror::Error)]
pub enum SessionActorMapError {

    #[error("session version rejected, current version {current_version} existing version {existing_version}")]
    SessionVersionRejected { current_version: SessionVersion, existing_version: SessionVersion },

}

pub struct SessionClock {
    counter: AtomicU64,
    node_id: u64,
}

impl SessionClock {
    pub fn new(node_id: u64) -> Self {
        Self {
            counter: AtomicU64::new(1),
            node_id,
        }
    }

    pub fn next(&self) -> SessionVersion {
        let next = self.counter.fetch_add(1, Ordering::SeqCst);
        SessionVersion::new(next, self.node_id)
    }

    pub fn bump(&self, remote: &SessionVersion) -> SessionVersion {
        let max_counter = self.counter.fetch_max(remote.counter + 1, Ordering::SeqCst);
        let adjusted = max_counter.max(remote.counter + 1);
        self.counter.store(adjusted, Ordering::SeqCst);
        SessionVersion::new(adjusted, self.node_id)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionVersion {

    pub counter: u64,

    pub node_id: u64

}

impl std::fmt::Display for SessionVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.counter, self.node_id)
    }
}

impl SessionVersion {
    pub fn new(counter: u64, node_id: u64) -> Self {
        SessionVersion { counter, node_id }
    }

    pub fn bump(&self, remote: &SessionVersion) -> SessionVersion {
        let max_counter = self.counter.max(remote.counter) + 1;
        SessionVersion::new(max_counter, self.node_id)
    }

    pub fn next_local(local_counter: &AtomicU64, node_id: u64) -> Self {
        let counter = local_counter.fetch_add(1, Ordering::SeqCst);
        SessionVersion::new(counter, node_id)
    }

    pub fn is_newer_than(&self, other: &Self) -> bool {
        self.counter > other.counter ||
        (self.counter == other.counter && self.node_id > other.node_id)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionActorMapEntry {

    pub node_id : NodeId,

    pub version: SessionVersion,

}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionActorMapStorage {
    inner: HashMap<String,HashMap<String, SessionActorMapEntry>>
}

impl SessionActorMapStorage {
    pub fn new() -> Self {
        SessionActorMapStorage {
            inner: HashMap::new()
        }
    }

    pub fn get_session_actor_map(&self, tenant_id: &str, client_id: &str) -> Option<SessionActorMapEntry> {
        self.inner
            .get(tenant_id)
            .and_then(|v| v.get(client_id))
            .cloned()   
    }

    pub fn register_session_actor(&mut self, tenant_id: String, session_id: String, node_id: NodeId, version: SessionVersion) -> Result<(), SessionActorMapError> {
        let session_tenant = self.inner.entry(tenant_id.clone()).or_insert(HashMap::new());
        if session_tenant.contains_key(&session_id) {
            let existing_entry = session_tenant.get(&session_id).unwrap();
            if existing_entry.version.is_newer_than(&version) {
                return Err(SessionActorMapError::SessionVersionRejected { current_version: version, existing_version: existing_entry.version.clone() });
            }
        }             
        let session_actor_map_entry = SessionActorMapEntry {
            node_id,
            version
        };
        session_tenant.insert(session_id.clone(), session_actor_map_entry);
        Ok(())
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
    inner: HashMap<String, HashMap<String, SessionActorMapEntry>>
}