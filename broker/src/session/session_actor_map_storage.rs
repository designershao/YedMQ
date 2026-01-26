use std::{
    collections::HashMap,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

use log::{info, warn};
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::raft::{
    session_actor_map::types::{ExpiredSession, RenewSession},
    NodeId,
};

#[derive(Debug, thiserror::Error)]
pub enum SessionActorMapError {
    #[error("session version rejected, current version {current_version} existing version {existing_version}")]
    SessionVersionRejected {
        current_version: SessionVersion,
        existing_version: SessionVersion,
    },
}

#[derive(Debug)]
pub struct SessionClock {
    counter: AtomicU64,
    node_id: u64,
    persist_path: String,
}

#[derive(Serialize, Deserialize)]
struct ClockSnapshot {
    counter: u64,
    node_id: u64,
}

impl SessionClock {
    pub fn get(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }

    /// Save clock to disk as JSON
    pub async fn persist(&self) -> std::io::Result<()> {
        let snapshot = ClockSnapshot {
            counter: self.get(),
            node_id: self.node_id,
        };

        let json = serde_json::to_string_pretty(&snapshot)?;
        fs::write(&self.persist_path, json).await
    }

    /// Restore from disk if exists, otherwise start fresh
    pub async fn restore(&self) -> std::io::Result<()> {
        if Path::new(&self.persist_path).exists() {
            let content = fs::read_to_string(&self.persist_path).await?;
            if let Ok(snapshot) = serde_json::from_str::<ClockSnapshot>(&content) {
                self.counter.store(snapshot.counter, Ordering::SeqCst);
                // Optional: verify snapshot.node_id == self.node_id
            }
        }
        Ok(())
    }

    pub fn new(node_id: u64, persist_path: impl Into<String>) -> Self {
        SessionClock {
            counter: AtomicU64::new(1),
            node_id,
            persist_path: persist_path.into(),
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

pub struct ClientListWithPagination {
    pub client_list: Vec<(String, NodeId)>,
    pub total: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionVersion {
    pub counter: u64,

    pub node_id: u64,
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
        self.counter > other.counter
            || (self.counter == other.counter && self.node_id > other.node_id)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionActorMapEntry {
    pub node_id: NodeId,

    pub version: SessionVersion,

    pub expiration_timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionActorMapStorage {
    inner: HashMap<String, HashMap<String, SessionActorMapEntry>>,
}

impl Default for SessionActorMapStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionActorMapStorage {
    pub fn new() -> Self {
        SessionActorMapStorage {
            inner: HashMap::new(),
        }
    }

    pub fn session_lease_renew(&mut self, sessions: Vec<RenewSession>, session_ttl: u64) {
        for renew in sessions {
            if let Some(session_tenant) = self.inner.get_mut(&renew.tenant_id) {
                if let Some(session_entry) = session_tenant.get_mut(&renew.session_id) {
                    session_entry.expiration_timestamp = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        + session_ttl;
                }
            }
        }
    }

    pub fn get_all_expired_sessions(&self) -> Vec<ExpiredSession> {
        let mut result = Vec::new();
        for (tenant_id, sessions) in self.inner.iter() {
            for (session_id, session) in sessions.iter() {
                if session.expiration_timestamp
                    < std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                {
                    result.push(ExpiredSession {
                        tenant_id: tenant_id.clone(),
                        session_id: session_id.clone(),
                        node_id: session.node_id,
                        session_version: session.version.clone(),
                    });
                }
            }
        }
        result
    }

    pub fn get_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Option<SessionActorMapEntry> {
        self.inner
            .get(tenant_id)
            .and_then(|v| v.get(client_id))
            .cloned()
    }

    pub fn register_session_actor(
        &mut self,
        tenant_id: String,
        session_id: String,
        node_id: NodeId,
        version: &SessionVersion,
        session_ttl: u64,
    ) -> Result<(), SessionActorMapError> {
        let session_tenant = self.inner.entry(tenant_id.clone()).or_default();
        if session_tenant.contains_key(&session_id) {
            let existing_entry = session_tenant.get(&session_id).unwrap();
            if existing_entry.version.is_newer_than(version) {
                return Err(SessionActorMapError::SessionVersionRejected {
                    current_version: version.clone(),
                    existing_version: existing_entry.version.clone(),
                });
            }
        }
        let session_actor_map_entry = SessionActorMapEntry {
            node_id,
            version: version.clone(),
            expiration_timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + session_ttl,
        };
        session_tenant.insert(session_id.clone(), session_actor_map_entry);
        Ok(())
    }

    pub fn unregister_session_actor(
        &mut self,
        tenant_id: String,
        session_id: String,
        version: &SessionVersion,
    ) {
        if let Some(tenant_map) = self.inner.get_mut(&tenant_id) {
            if let Some(existing) = tenant_map.get(&session_id) {
                if existing.version.counter == version.counter
                    && existing.version.node_id == version.node_id
                {
                    tenant_map.remove(&session_id);
                    info!(
                        "Session {} unregistered for tenant {} by version {}",
                        session_id, tenant_id, version
                    );
                } else {
                    warn!("Reject stale unregister for {}, current version is {}, request version is {}", session_id, existing.version, version);
                }
            }
        }
    }

    fn to_serializable(&self) -> SerializableSessionActorMapStorage {
        SerializableSessionActorMapStorage {
            inner: self.inner.clone(),
        }
    }

    fn from_serializable(data: SerializableSessionActorMapStorage) -> Self {
        SessionActorMapStorage { inner: data.inner }
    }

    pub fn to_snapshot(&self) -> Vec<u8> {
        let serializable = self.to_serializable();
        serde_json::to_vec(&serializable).unwrap()
    }

    pub fn from_snapshot(snapshot: Vec<u8>) -> Self {
        let serializable: SerializableSessionActorMapStorage =
            serde_json::from_slice(&snapshot).unwrap();
        Self::from_serializable(serializable)
    }

    pub fn get_client_id_list_with_pagination(
        &self,
        tenant_id: &str,
        offset: usize,
        limit: usize,
    ) -> Option<ClientListWithPagination> {
        if let Some(tenant_map) = self.inner.get(tenant_id) {
            let total = tenant_map.len();
            let clients: Vec<String> = tenant_map.keys().cloned().collect();
            let paginated_clients = clients
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|client_id| {
                    let node_id = tenant_map.get(&client_id).unwrap().node_id;
                    (client_id, node_id)
                }).collect::<Vec<(String, NodeId)>>();
            Some(
                ClientListWithPagination {
                    client_list: paginated_clients,
                    total
                }
            )
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SerializableSessionActorMapStorage {
    inner: HashMap<String, HashMap<String, SessionActorMapEntry>>,
}