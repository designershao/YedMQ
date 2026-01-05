use std::{collections::HashMap, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;

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
    ) {
        self.inner.entry(tenant_id.to_owned()).or_default().insert(
            session_id.to_owned(),
            Arc::new(RwLock::new(SessionState::new(inflight_duration))),
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
        tenant_id: String,
        client_id: String,
        packet_id: u16,
        qos: u8,
        packet_key: String,
    ) -> Result<Option<String>, SessionStateStorageError> {
        if self.inner.get(&tenant_id).is_none() {
            self.inner.insert(tenant_id.clone(), HashMap::new());
        }

        let state_arc = if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            tenant_sessions.get(&client_id).cloned()
        } else {
            None
        };

        if let Some(state_arc) = state_arc {
            self.inc_ref(&packet_key);
            let mut state = state_arc.write().await;
            let old_key = state
                .inflight
                .register_with_tx_packet(packet_id, qos, packet_key)?;
            drop(state);
            let freed_key = old_key.and_then(|k| self.dec_ref(&k));
            Ok(freed_key)
        } else {
            Err(SessionStateStorageError::SessionStateNotExisted { client_id })
        }
    }
    pub async fn inflight_register_rx_packet(
        &mut self,
        tenant_id: String,
        client_id: String,
        packet_id: u16,
        qos: u8,
        packet_key: String,
    ) -> Option<String> {
        if self.inner.get(&tenant_id).is_none() {
            self.inner.insert(tenant_id.clone(), HashMap::new());
        }

        let state_arc = if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            tenant_sessions.get(&client_id).cloned()
        } else {
            None
        };

        if let Some(state_arc) = state_arc {
            self.inc_ref(&packet_key);
            let mut state = state_arc.write().await;
            let old_key = state
                .inflight
                .register_with_rx_packet(packet_id, qos, packet_key);
            drop(state);
            let freed_key = old_key.and_then(|k| self.dec_ref(&k));
            freed_key
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
        tenant_id: String,
        client_id: String,
        packet_key: String,
    ) {
        if self.inner.get(&tenant_id).is_none() {
            self.inner.insert(tenant_id.clone(), HashMap::new());
        }

        let state_arc = if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            tenant_sessions.get(&client_id).cloned()
        } else {
            None
        };

        if let Some(state_arc) = state_arc {
            self.inc_ref(&packet_key);
            let mut state = state_arc.write().await;
            state.pending_messages.push(packet_key);
        }
    }

    pub async fn pop_from_pending_queue(
        &mut self,
        tenant_id: String,
        client_id: String,
    ) -> (Option<String>, Option<String>) {
        let state_arc = if let Some(tenant_sessions) = self.inner.get(&tenant_id) {
            tenant_sessions.get(&client_id).cloned()
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
        tenant_id: String,
        client_id: String,
        topic: String,
        qos: QoS,
    ) {
        if self.inner.get(&tenant_id).is_none() {
            self.inner.insert(tenant_id.clone(), HashMap::new());
        }

        if self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_some()
        {
            self.inner
                .get_mut(&tenant_id)
                .unwrap()
                .get_mut(&client_id)
                .unwrap()
                .write()
                .await
                .subscriptions
                .insert(topic, qos);
        }
    }

    pub async fn unsubscribe_topic(&mut self, tenant_id: String, client_id: String, topic: String) {
        if self.inner.get(&tenant_id).is_none() {
            return;
        }

        if self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_some()
        {
            self.inner
                .get_mut(&tenant_id)
                .unwrap()
                .get_mut(&client_id)
                .unwrap()
                .write()
                .await
                .subscriptions
                .remove(&topic);
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
pub struct SerializableSessionStateStorage {
    pub inner: HashMap<String, HashMap<String, SessionState>>,
    pub ref_counts: HashMap<String, u64>,
}
