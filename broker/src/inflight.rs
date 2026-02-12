use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Serialize, Deserialize, Clone)]
pub enum InflightError {
    #[error("packet identifier has existed")]
    PacketIdentifierHasExisted,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Inflight {
    pub inner: HashMap<u16, InflightItem>,
    expired_duration: Duration,
}

impl Inflight {
    pub fn new(expired_duration: Duration) -> Inflight {
        Inflight {
            inner: HashMap::new(),
            expired_duration,
        }
    }

    pub fn get_all_packet_keys(&self) -> Vec<String> {
        self.inner
            .values()
            .filter_map(|item| item.packet_key.clone())
            .collect()
    }

    pub fn register_with_rx_packet(
        &mut self,
        packet_identifier: u16,
        qos: u8,
        packet_key: String,
    ) -> Option<String> {
        let mut old_key = None;
        if qos == 2 {
            let item = InflightItem {
                packet_identifier,
                state: InflightState::WaitPubrel,
                packet_key: Some(packet_key.clone()),
                last_modified: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time is before unix epoch")
                    .as_secs(),
            };
            if let Some(old_item) = self.inner.insert(packet_identifier, item) {
                old_key = old_item.packet_key;
            }
        }
        if qos == 1 {
            let item = InflightItem {
                packet_identifier,
                state: InflightState::Finish,
                packet_key: Some(packet_key),
                last_modified: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time is before unix epoch")
                    .as_secs(),
            };
            if let Some(old_item) = self.inner.insert(packet_identifier, item) {
                old_key = old_item.packet_key;
            }
        }
        old_key
    }

    fn packet_id_exists(&self, packet_identifier: u16) -> bool {
        self.inner.contains_key(&packet_identifier)
    }

    pub fn register_with_tx_packet(
        &mut self,
        packet_identifier: u16,
        qos: u8,
        packet_key: String,
    ) -> Result<Option<String>, InflightError> {
        // NOTE: The original code allowed overwriting if packet_id_exists was false.
        // Actually, register_with_tx_packet has an explicit check.
        if self.packet_id_exists(packet_identifier) {
            return Err(InflightError::PacketIdentifierHasExisted);
        }

        let mut old_key = None;
        if qos == 2 {
            let item = InflightItem {
                packet_identifier,
                state: InflightState::WaitPubrec,
                packet_key: Some(packet_key.clone()),
                last_modified: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time is before unix epoch")
                    .as_secs(),
            };
            if let Some(old_item) = self.inner.insert(packet_identifier, item) {
                old_key = old_item.packet_key;
            }
        }
        if qos == 1 {
            let item = InflightItem {
                packet_identifier,
                state: InflightState::WaitPuback,
                packet_key: Some(packet_key),
                last_modified: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time is before unix epoch")
                    .as_secs(),
            };
            if let Some(old_item) = self.inner.insert(packet_identifier, item) {
                old_key = old_item.packet_key;
            }
        }
        Ok(old_key)
    }

    pub fn get_all_packet_keys_and_refresh_expired_time(&mut self) -> Vec<(u16, String)> {
        let mut result_vec: Vec<(u16, String)> = vec![];
        //let mut inner = self.inner.write().await;
        for item in self.inner.values_mut() {
            if let Some(packet_key) = &item.packet_key {
                result_vec.push((item.packet_identifier, packet_key.clone()));
            }
            item.last_modified = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time is before unix epoch")
                .as_secs();
        }
        result_vec
    }

    // Get all packet keys which should be resend to the client and refresh expired time
    pub fn get_all_expired_packet_keys_and_refresh_expired_time(&mut self) -> Vec<(u16, String)> {
        let mut result_vec: Vec<(u16, String)> = vec![];
        //let mut inner = self.inner.write().await;
        for item in self.inner.values_mut() {
            if item.last_modified + self.expired_duration.as_secs()
                < std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time is before unix epoch")
                    .as_secs()
            {
                if let Some(packet_key) = &item.packet_key {
                    result_vec.push((item.packet_identifier, packet_key.clone()));
                }
                item.last_modified = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time is before unix epoch")
                    .as_secs();
            }
        }
        result_vec
    }

    pub fn get_next_state_packet_key(&self, packet_identifier: u16) -> Option<String> {
        let ctx = self.inner.get(&packet_identifier);
        if let Some(ctx_item) = ctx {
            ctx_item.packet_key.clone()
        } else {
            None
        }
    }

    pub fn allocate_packet_id(&mut self) -> Option<u16> {
        (1..=65535).find(|&id| !self.inner.contains_key(&id))
    }

    pub fn get_current_packet_key(&self, packet_identifier: u16) -> Option<String> {
        let ctx = self.inner.get(&packet_identifier);
        if let Some(ctx_item) = ctx {
            ctx_item.packet_key.clone()
        } else {
            None
        }
    }

    pub fn get_inflight_current_state(&self, packet_identifier: u16) -> Option<InflightState> {
        let ctx = self.inner.get(&packet_identifier);
        ctx.map(|ctx_item| ctx_item.state)
    }

    pub fn next_state(&mut self, packet_identifier: u16) -> Option<String> {
        let ctx = self.inner.get_mut(&packet_identifier);
        if let Some(ctx_item) = ctx {
            ctx_item.to_next();
            if ctx_item.state == InflightState::Finish {
                return ctx_item.packet_key.clone();
            }
        }
        None
    }

    // Clean all finished qos packet identifier and return their keys
    pub fn clean_finished_items(&mut self) -> Vec<String> {
        let mut keys = Vec::new();
        self.inner.retain(|_, v| {
            if v.state == InflightState::Finish {
                if let Some(k) = &v.packet_key {
                    keys.push(k.clone());
                }
                false
            } else {
                true
            }
        });
        keys
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum InflightState {
    WaitPubrel,
    WaitPubcomp,
    WaitPubrec,
    WaitPuback,
    Finish,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InflightItem {
    pub packet_identifier: u16,
    pub state: InflightState,
    pub packet_key: Option<String>,
    pub last_modified: u64,
}

impl InflightItem {
    // According current state , set the next state
    pub fn to_next(&mut self) {
        let next_state = match self.state {
            InflightState::WaitPubrel => InflightState::Finish,
            InflightState::WaitPubcomp => InflightState::Finish,
            InflightState::WaitPubrec => InflightState::WaitPubcomp,
            InflightState::WaitPuback => InflightState::Finish,
            InflightState::Finish => InflightState::Finish,
        };
        self.state = next_state;
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time is before unix epoch")
            .as_secs();
    }
}