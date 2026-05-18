use std::{collections::HashMap, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use yedmq_mqtt::packet::ProtocolVersion;

use crate::inflight::Inflight;

use super::session_actor::QoS;

#[derive(Debug, Error, Serialize, Deserialize, Clone)]
pub enum SessionStateStorageError {
    #[error("inflight error, details: {0}")]
    InflightError(#[from] crate::inflight::InflightError),

    #[error("session state not existed for client id: {client_id}")]
    SessionStateNotExisted { client_id: String },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionState {
    pub pending_messages: Vec<String>,

    pub inflight: Inflight,

    pub subscriptions: HashMap<String, QoS>,

    pub disconnected_at: Option<u64>,

    #[serde(default)]
    pub protocol_version: Option<ProtocolVersion>,

    #[serde(default)]
    pub session_expiry_interval: Option<u32>,

    #[serde(default)]
    pub expires_at: Option<u64>,
}

impl SessionState {
    pub fn new(inflight_duration: Duration) -> Self {
        Self::new_with_options(inflight_duration, None, None)
    }

    pub fn new_with_options(
        inflight_duration: Duration,
        protocol_version: Option<ProtocolVersion>,
        session_expiry_interval: Option<u32>,
    ) -> Self {
        SessionState {
            pending_messages: Vec::new(),
            inflight: Inflight::new(inflight_duration),
            subscriptions: HashMap::new(),
            disconnected_at: None,
            protocol_version,
            session_expiry_interval,
            expires_at: None,
        }
    }

    fn expiry_deadline(disconnected_at: u64, session_expiry_interval: u32) -> Option<u64> {
        if session_expiry_interval == u32::MAX {
            None
        } else {
            Some(disconnected_at.saturating_add(session_expiry_interval as u64))
        }
    }

    pub fn update_connection_state(
        &mut self,
        disconnected_at: Option<u64>,
        session_expiry_interval_update: Option<u32>,
    ) {
        if let Some(session_expiry_interval) = session_expiry_interval_update {
            self.protocol_version = Some(ProtocolVersion::V5_0);
            self.session_expiry_interval = Some(session_expiry_interval);
        }

        self.disconnected_at = disconnected_at;
        self.expires_at = match (self.disconnected_at, self.session_expiry_interval) {
            (Some(disconnected_at), Some(session_expiry_interval)) => {
                Self::expiry_deadline(disconnected_at, session_expiry_interval)
            }
            _ => None,
        };
    }

    pub fn expired_disconnected_at(&self, now: u64, legacy_ttl: u64) -> Option<u64> {
        let disconnected_at = self.disconnected_at?;

        if let Some(session_expiry_interval) = self.session_expiry_interval {
            if session_expiry_interval == u32::MAX {
                return None;
            }

            let expires_at = self
                .expires_at
                .unwrap_or_else(|| disconnected_at.saturating_add(session_expiry_interval as u64));
            if now >= expires_at {
                return Some(disconnected_at);
            }
            return None;
        }

        if now.saturating_sub(disconnected_at) > legacy_ttl {
            Some(disconnected_at)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct SessionStateStorage {
    inner: HashMap<String, HashMap<String, Arc<RwLock<SessionState>>>>,
    pub ref_counts: HashMap<String, u64>,
}

impl Default for SessionStateStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStateStorage {
    pub fn new() -> Self {
        SessionStateStorage {
            inner: HashMap::new(),
            ref_counts: HashMap::new(),
        }
    }

    fn inc_ref(&mut self, key: &str) {
        let count = self.ref_counts.entry(key.to_string()).or_insert(0);
        *count += 1;
    }

    fn dec_ref(&mut self, key: &str) -> Option<String> {
        if let Some(count) = self.ref_counts.get_mut(key) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                self.ref_counts.remove(key);
                return Some(key.to_string());
            }
        }
        None
    }

    pub async fn create_session_state(
        &mut self,
        tenant_id: &str,
        session_id: &str,
        inflight_duration: Duration,
        protocol_version: Option<ProtocolVersion>,
        session_expiry_interval: Option<u32>,
    ) {
        self.inner.entry(tenant_id.to_owned()).or_default().insert(
            session_id.to_owned(),
            Arc::new(RwLock::new(SessionState::new_with_options(
                inflight_duration,
                protocol_version,
                session_expiry_interval,
            ))),
        );
    }

    pub async fn delete_session_state(&mut self, tenant_id: &str, session_id: &str) -> Vec<String> {
        let mut freed_keys = Vec::new();
        if let Some(tenant_map) = self.inner.get_mut(tenant_id) {
            if let Some(state_arc) = tenant_map.remove(session_id) {
                let state = state_arc.write().await;
                for key in &state.pending_messages {
                    if let Some(k) = self.dec_ref(key) {
                        freed_keys.push(k);
                    }
                }
                for key in state.inflight.get_all_packet_keys() {
                    if let Some(k) = self.dec_ref(&key) {
                        freed_keys.push(k);
                    }
                }
            }
        }
        freed_keys
    }

    pub async fn get_session_state(
        &self,
        tenant_id: &str,
        session_id: &str,
    ) -> Option<Arc<RwLock<SessionState>>> {
        self.inner
            .get(tenant_id)
            .and_then(|v| v.get(session_id))
            .cloned()
    }

    pub async fn session_state_exists(&self, tenant_id: &str, session_id: &str) -> bool {
        self.inner
            .get(tenant_id)
            .and_then(|v| v.get(session_id))
            .is_some()
    }

    pub async fn inflight_register_tx_packet(
        &mut self,
        tenant_id: &str,
        client_id: &str,
        packet_id: u16,
        qos: u8,
        packet_key: &str,
    ) -> Result<Option<String>, SessionStateStorageError> {
        if !self.inner.contains_key(tenant_id) {
            self.inner.insert(tenant_id.to_string(), HashMap::new());
        }

        let state_arc = if let Some(tenant_sessions) = self.inner.get(tenant_id) {
            tenant_sessions.get(client_id).cloned()
        } else {
            None
        };

        if let Some(state_arc) = state_arc {
            self.inc_ref(packet_key);
            let mut state = state_arc.write().await;
            let old_key =
                state
                    .inflight
                    .register_with_tx_packet(packet_id, qos, packet_key.to_string())?;
            drop(state);
            let freed_key = old_key.and_then(|k| self.dec_ref(&k));
            Ok(freed_key)
        } else {
            Err(SessionStateStorageError::SessionStateNotExisted {
                client_id: client_id.to_string(),
            })
        }
    }
    pub async fn inflight_register_rx_packet(
        &mut self,
        tenant_id: &str,
        client_id: &str,
        packet_id: u16,
        qos: u8,
        packet_key: &str,
    ) -> Option<String> {
        if !self.inner.contains_key(tenant_id) {
            self.inner.insert(tenant_id.to_string(), HashMap::new());
        }

        let state_arc = if let Some(tenant_sessions) = self.inner.get(tenant_id) {
            tenant_sessions.get(client_id).cloned()
        } else {
            None
        };

        if let Some(state_arc) = state_arc {
            self.inc_ref(packet_key);
            let mut state = state_arc.write().await;
            let old_key =
                state
                    .inflight
                    .register_with_rx_packet(packet_id, qos, packet_key.to_string());
            drop(state);

            old_key.and_then(|k| self.dec_ref(&k))
        } else {
            None
        }
    }

    pub async fn inflight_get_next_state_packet_key(
        &self,
        tenant_id: String,
        client_id: String,
        packet_identifier: u16,
    ) -> Option<String> {
        self.inner.get(&tenant_id)?;

        if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            if let Some(session_arc) = tenant_sessions.get(&client_id) {
                let state = session_arc.write().await;
                return state.inflight.get_next_state_packet_key(packet_identifier);
            }
        }
        None
    }

    pub async fn inflight_get_packet_state(
        &self,
        tenant_id: String,
        client_id: String,
        packet_identifier: u16,
    ) -> Option<crate::inflight::InflightState> {
        self.inner.get(&tenant_id)?;

        if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            if let Some(session_arc) = tenant_sessions.get(&client_id) {
                let state = session_arc.write().await;
                return state.inflight.get_inflight_current_state(packet_identifier);
            }
        }
        None
    }

    pub async fn inflight_get_current_packet_key(
        &self,
        tenant_id: String,
        client_id: String,
        packet_identifier: u16,
    ) -> Option<String> {
        self.inner.get(&tenant_id)?;

        if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            if let Some(session_arc) = tenant_sessions.get(&client_id) {
                let state = session_arc.write().await;
                return state.inflight.get_current_packet_key(packet_identifier);
            }
        }
        None
    }

    pub async fn inflight_next_state(
        &mut self,
        tenant_id: String,
        client_id: String,
        packet_identifier: u16,
    ) -> Option<String> {
        let state_arc = if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            tenant_sessions.get(&client_id).cloned()
        } else {
            None
        };

        if let Some(state_arc) = state_arc {
            let mut state = state_arc.write().await;
            let _finished_key = state.inflight.next_state(packet_identifier);
            // We return None for now because Finish state doesn't mean removal.
            // Items are removed in clean_finished_items.
            return None;
        }
        None
    }

    pub async fn inflight_clean_finished_items(
        &mut self,
        tenant_id: String,
        client_id: String,
    ) -> Vec<String> {
        let state_arc = if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            tenant_sessions.get(&client_id).cloned()
        } else {
            None
        };

        if let Some(state_arc) = state_arc {
            let mut state = state_arc.write().await;
            let keys = state.inflight.clean_finished_items();
            drop(state);
            let mut freed_keys = Vec::new();
            for key in keys {
                if let Some(k) = self.dec_ref(&key) {
                    freed_keys.push(k);
                }
            }
            return freed_keys;
        }
        Vec::new()
    }
    pub async fn append_to_pending_queue(
        &mut self,
        tenant_id: &str,
        client_id: &str,
        packet_key: &str,
    ) {
        if !self.inner.contains_key(tenant_id) {
            self.inner.insert(tenant_id.to_string(), HashMap::new());
        }

        let state_arc = if let Some(tenant_sessions) = self.inner.get(tenant_id) {
            tenant_sessions.get(client_id).cloned()
        } else {
            None
        };

        if let Some(state_arc) = state_arc {
            self.inc_ref(packet_key);
            let mut state = state_arc.write().await;
            state.pending_messages.push(packet_key.to_string());
        }
    }

    pub async fn pop_from_pending_queue(
        &mut self,
        tenant_id: &str,
        client_id: &str,
    ) -> (Option<String>, Option<String>) {
        let state_arc = if let Some(tenant_sessions) = self.inner.get(tenant_id) {
            tenant_sessions.get(client_id).cloned()
        } else {
            None
        };

        if let Some(state_arc) = state_arc {
            let mut state = state_arc.write().await;

            let key = state.pending_messages.pop();

            drop(state);

            let freed_key = key.as_ref().and_then(|k| self.dec_ref(k));

            return (key, freed_key);
        }

        (None, None)
    }

    pub async fn subscribe_topic(
        &mut self,
        tenant_id: &str,
        client_id: &str,
        topic: &str,
        qos: QoS,
    ) {
        if !self.inner.contains_key(tenant_id) {
            self.inner.insert(tenant_id.to_string(), HashMap::new());
        }

        if let Some(tenant_sessions) = self.inner.get(tenant_id) {
            if let Some(session_arc) = tenant_sessions.get(client_id) {
                session_arc
                    .write()
                    .await
                    .subscriptions
                    .insert(topic.to_string(), qos);
            }
        }
    }

    pub async fn unsubscribe_topic(&mut self, tenant_id: String, client_id: String, topic: String) {
        if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            if let Some(session_arc) = tenant_sessions.get(&client_id) {
                let mut session = session_arc.write().await;
                session.subscriptions.remove(&topic);
            }
        }
    }

    pub async fn scan_expired_sessions(&self, now: u64, ttl: u64) -> Vec<(String, String, u64)> {
        let mut expired_sessions = Vec::new();
        for (tenant_id, sessions) in self.inner.iter() {
            for (client_id, session_arc) in sessions.iter() {
                let session = session_arc.read().await;
                if let Some(disconnected_at) = session.expired_disconnected_at(now, ttl) {
                    expired_sessions.push((tenant_id.clone(), client_id.clone(), disconnected_at));
                }
            }
        }
        expired_sessions
    }

    pub async fn update_connection_state(
        &mut self,
        tenant_id: String,
        client_id: String,
        disconnected_at: Option<u64>,
        session_expiry_interval_update: Option<u32>,
    ) {
        if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            if let Some(session_arc) = tenant_sessions.get(&client_id) {
                let mut session = session_arc.write().await;
                session.update_connection_state(disconnected_at, session_expiry_interval_update);
            }
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
                            .map(|(k, v)| (k.clone(), v.blocking_read().clone()))
                            .collect(),
                    )
                })
                .collect(),
            ref_counts: self.ref_counts.clone(),
        }
    }

    fn from_serializable(data: SerializableSessionStateStorage) -> Self {
        SessionStateStorage {
            inner: data
                .inner
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.iter()
                            .map(|(k, v)| (k.clone(), Arc::new(RwLock::new(v.clone()))))
                            .collect(),
                    )
                })
                .collect(),
            ref_counts: data.ref_counts,
        }
    }

    pub fn to_snapshot(&self) -> Result<Vec<u8>, serde_json::Error> {
        let serializable = self.to_serializable();
        serde_json::to_vec(&serializable)
    }

    pub fn from_snapshot(snapshot: Vec<u8>) -> Result<Self, serde_json::Error> {
        let serializable: SerializableSessionStateStorage = serde_json::from_slice(&snapshot)?;
        Ok(Self::from_serializable(serializable))
    }
}

#[derive(Serialize, Deserialize)]
pub struct SerializableSessionStateStorage {
    pub inner: HashMap<String, HashMap<String, SessionState>>,
    pub ref_counts: HashMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn legacy_session_expiry_uses_global_ttl() {
        let mut storage = SessionStateStorage::new();
        storage
            .create_session_state("tenant", "legacy", Duration::from_secs(1), None, None)
            .await;
        storage
            .update_connection_state("tenant".into(), "legacy".into(), Some(100), None)
            .await;

        assert!(storage.scan_expired_sessions(110, 20).await.is_empty());
        assert_eq!(
            storage.scan_expired_sessions(121, 20).await,
            vec![("tenant".to_string(), "legacy".to_string(), 100)]
        );
    }

    #[tokio::test]
    async fn mqtt5_session_expiry_uses_per_session_deadline() {
        let mut storage = SessionStateStorage::new();
        storage
            .create_session_state(
                "tenant",
                "mqtt5",
                Duration::from_secs(1),
                Some(ProtocolVersion::V5_0),
                Some(5),
            )
            .await;
        storage
            .update_connection_state("tenant".into(), "mqtt5".into(), Some(100), None)
            .await;

        let state = storage
            .get_session_state("tenant", "mqtt5")
            .await
            .expect("session state");
        assert_eq!(state.read().await.expires_at, Some(105));
        assert!(storage.scan_expired_sessions(104, 999).await.is_empty());
        assert_eq!(
            storage.scan_expired_sessions(105, 999).await,
            vec![("tenant".to_string(), "mqtt5".to_string(), 100)]
        );
    }

    #[tokio::test]
    async fn mqtt5_never_expiring_session_is_not_scanned() {
        let mut storage = SessionStateStorage::new();
        storage
            .create_session_state(
                "tenant",
                "mqtt5-never",
                Duration::from_secs(1),
                Some(ProtocolVersion::V5_0),
                Some(u32::MAX),
            )
            .await;
        storage
            .update_connection_state("tenant".into(), "mqtt5-never".into(), Some(100), None)
            .await;

        let state = storage
            .get_session_state("tenant", "mqtt5-never")
            .await
            .expect("session state");
        assert_eq!(state.read().await.expires_at, None);
        assert!(storage.scan_expired_sessions(100_000, 1).await.is_empty());
    }
}
