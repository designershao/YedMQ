use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use yedmq_mqtt::MqttPacketV3;

use crate::inflight::Inflight;

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
    pub fn new() -> Self {
        SessionStateStorage {
            inner: HashMap::new(),
        }
    }

    pub async fn create_session_state(&mut self, tenant_id: &str, session_id: &str, inflight_duration: Duration) {
        self.inner
            .entry(tenant_id.to_owned())
            .or_insert(HashMap::new())
            .insert(session_id.to_owned(), Arc::new(RwLock::new(SessionState::new(inflight_duration))));
    }

    pub async fn delete_session_state(&mut self, tenant_id: &str, session_id: &str) {
        self.inner.get_mut(tenant_id).unwrap().remove(session_id);
    }

    pub async fn get_session_state(&self, tenant_id: &str, session_id: &str) -> Option<Arc<RwLock<SessionState>>> {
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

    pub async fn inflight_register_tx_packet(&mut self, tenant_id: String, client_id: String, packet: MqttPacketV3) {
        if self.inner.get(&tenant_id).is_none() {
            self.inner.insert(tenant_id.clone(), HashMap::new());
        }

        if !self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_none()
        {
            let mut state = self
                .inner
                .get_mut(&tenant_id)
                .unwrap()
                .get_mut(&client_id)
                .unwrap()
                .write()
                .await;
            state.inflight.register_with_tx_packet(&packet).await.unwrap();
        }
    }

    pub async fn inflight_register_rx_packet(&mut self, tenant_id: String, client_id: String, packet: MqttPacketV3) {
        if self.inner.get(&tenant_id).is_none() {
            self.inner.insert(tenant_id.clone(), HashMap::new());
        }

        if !self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_none()
        {
            let mut state = self
                .inner
                .get_mut(&tenant_id)
                .unwrap()
                .get_mut(&client_id)
                .unwrap()
                .write()
                .await;
            state.inflight.register_with_rx_packet(&packet).await;
        }
    }

    pub async fn inflight_get_current_packet(&mut self, tenant_id: String, client_id: String, packet_identifier: u16) -> Option<MqttPacketV3> {
        if self.inner.get(&tenant_id).is_none() {
            return None;
        }

        if !self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_none()
        {
            let state = self
                .inner
                .get_mut(&tenant_id)
                .unwrap()
                .get_mut(&client_id)
                .unwrap()
                .write()
                .await;
            state.inflight.get_current_packet(packet_identifier).await
        } else {
            None
        }
    }

    pub async fn inflight_next_state(&mut self, tenant_id: String, client_id: String, packet_identifier: u16) {
        if self.inner.get(&tenant_id).is_none() {
            return;
        }

        if !self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_none()
        {
            let mut state = self
                .inner
                .get_mut(&tenant_id)
                .unwrap()
                .get_mut(&client_id)
                .unwrap()
                .write()
                .await;
            state.inflight.next_state(packet_identifier).await;
        }
    }

    pub async fn inflight_clean_finished_items(&mut self, tenant_id: String, client_id: String) {
        if self.inner.get(&tenant_id).is_none() {
            return;
        }

        if !self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_none()
        {
            let mut state = self
                .inner
                .get_mut(&tenant_id)
                .unwrap()
                .get_mut(&client_id)
                .unwrap()
                .write()
                .await;
            state.inflight.clean_finished_items().await;
        }
    }

    pub async fn append_to_pending_queue(&mut self, tenant_id: String, client_id: String, packet: MqttPacketV3) {
        if self.inner.get(&tenant_id).is_none() {
            self.inner.insert(tenant_id.clone(), HashMap::new());
        }

        if !self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_none()
        {
            let mut state = self
                .inner
                .get_mut(&tenant_id)
                .unwrap()
                .get_mut(&client_id)
                .unwrap()
                .write()
                .await;
            state.pending_messages.push(packet);
        }
    }

    pub async fn pop_from_pending_queue(&mut self, tenant_id: String, client_id: String) -> Option<MqttPacketV3> {
        if self.inner.get(&tenant_id).is_none() {
            return None;
        }

        if !self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_none()
        {
            let mut state = self
                .inner
                .get_mut(&tenant_id)
                .unwrap()
                .get_mut(&client_id)
                .unwrap()
                .write()
                .await;
            let packet = state.pending_messages.pop();
            Some(packet.unwrap())
        } else {
            None
        }   
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

        if !self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_none()
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

    pub async fn unsubscribe_topic(
        &mut self,
        tenant_id: String,
        client_id: String,
        topic: String,
    ) {
        if self.inner.get(&tenant_id).is_none() {
            return;
        }

        if !self
            .inner
            .get(&tenant_id)
            .unwrap()
            .get(&client_id)
            .is_none()
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
