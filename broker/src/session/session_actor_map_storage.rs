use std::{collections::HashMap, time::SystemTime};

use log::debug;
use serde::{Deserialize, Serialize};

use crate::raft::{
    session_actor_map::types::{ExpiredSession, RenewSession},
    NodeId,
};

pub struct ClientListWithPagination {
    pub client_list: Vec<(String, NodeId)>,
    pub total: usize,
}

/// The Raft log index of the registration that owns this session.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionInstanceId {
    pub log_index: u64,
}

impl std::fmt::Display for SessionInstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.log_index)
    }
}

impl SessionInstanceId {
    pub fn new(log_index: u64) -> Self {
        Self { log_index }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionActorMapEntry {
    pub node_id: NodeId,

    pub instance_id: SessionInstanceId,

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
                    if session_entry.instance_id != renew.session_instance_id {
                        continue;
                    }
                    session_entry.expiration_timestamp = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("system time is before unix epoch")
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
                        .expect("system time is before unix epoch")
                        .as_secs()
                {
                    result.push(ExpiredSession {
                        tenant_id: tenant_id.clone(),
                        session_id: session_id.clone(),
                        node_id: session.node_id,
                        session_instance_id: session.instance_id,
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
        instance_id: SessionInstanceId,
        session_ttl: u64,
    ) -> Option<SessionActorMapEntry> {
        let session_tenant = self.inner.entry(tenant_id.clone()).or_default();

        let session_actor_map_entry = SessionActorMapEntry {
            node_id,
            instance_id,
            expiration_timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("system time is before unix epoch")
                .as_secs()
                + session_ttl,
        };
        session_tenant.insert(session_id, session_actor_map_entry)
    }

    pub fn unregister_session_actor(
        &mut self,
        tenant_id: String,
        session_id: String,
        instance_id: SessionInstanceId,
    ) {
        if let Some(tenant_map) = self.inner.get_mut(&tenant_id) {
            if let Some(existing) = tenant_map.get(&session_id) {
                if existing.instance_id == instance_id {
                    tenant_map.remove(&session_id);
                    debug!(
                        "session {} unregistered for tenant {} by instance {}",
                        session_id, tenant_id, instance_id
                    );
                } else {
                    debug!(
                        "reject stale unregister for {}, current instance is {}, request instance is {}",
                        session_id, existing.instance_id, instance_id
                    );
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

    pub fn to_snapshot(&self) -> Result<Vec<u8>, serde_json::Error> {
        let serializable = self.to_serializable();
        serde_json::to_vec(&serializable)
    }

    pub fn from_snapshot(snapshot: Vec<u8>) -> Result<Self, serde_json::Error> {
        let serializable: SerializableSessionActorMapStorage = serde_json::from_slice(&snapshot)?;
        Ok(Self::from_serializable(serializable))
    }

    pub fn get_client_id_list_with_pagination(
        &self,
        tenant_id: &str,
        offset: usize,
        limit: usize,
    ) -> Option<ClientListWithPagination> {
        if let Some(tenant_map) = self.inner.get(tenant_id) {
            let total = tenant_map.len();
            let paginated_clients = tenant_map
                .iter()
                .skip(offset)
                .take(limit)
                .map(|(client_id, entry)| {
                    let node_id = entry.node_id;
                    (client_id.clone(), node_id)
                })
                .collect::<Vec<(String, NodeId)>>();
            Some(ClientListWithPagination {
                client_list: paginated_clients,
                total,
            })
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SerializableSessionActorMapStorage {
    inner: HashMap<String, HashMap<String, SessionActorMapEntry>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_registration_order_wins_and_stale_cleanup_is_ignored() {
        let mut storage = SessionActorMapStorage::new();
        let old = SessionInstanceId::new(1);
        let new = SessionInstanceId::new(2);

        assert!(storage
            .register_session_actor("tenant".into(), "client".into(), 10, old, 60)
            .is_none());
        let previous = storage
            .register_session_actor("tenant".into(), "client".into(), 20, new, 60)
            .unwrap();
        assert_eq!(previous.instance_id, old);

        storage.unregister_session_actor("tenant".into(), "client".into(), old);
        assert_eq!(
            storage
                .get_session_actor_map("tenant", "client")
                .unwrap()
                .instance_id,
            new
        );

        storage.unregister_session_actor("tenant".into(), "client".into(), new);
        assert!(storage.get_session_actor_map("tenant", "client").is_none());
    }

    #[test]
    fn stale_renew_does_not_extend_new_session_lease() {
        let mut storage = SessionActorMapStorage::new();
        let old = SessionInstanceId::new(1);
        let new = SessionInstanceId::new(2);
        storage.register_session_actor("tenant".into(), "client".into(), 20, new, 60);
        let before = storage
            .get_session_actor_map("tenant", "client")
            .unwrap()
            .expiration_timestamp;

        storage.session_lease_renew(
            vec![RenewSession {
                tenant_id: "tenant".into(),
                session_id: "client".into(),
                session_instance_id: old,
            }],
            600,
        );
        assert_eq!(
            storage
                .get_session_actor_map("tenant", "client")
                .unwrap()
                .expiration_timestamp,
            before
        );
    }
}
