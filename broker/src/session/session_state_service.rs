use actix::{Addr, SystemService};

use crate::raft::session_state::session_state_raft_actor::{
    AdvanceInflightState, GetSessionStateEnsureLinearizable, InflightCleanFinishedItems,
    PopOfflineMessage, RegisterInflightRxPacket, RegisterInflightTxPacket, ScanExpiredSessions,
    SessionStateRaftActor, SessionStateRaftError, StoreOfflineMessage, SubscribeTopic,
    UnsubscribeTopic, UpdateSessionConnectionState,
};
use crate::session::session_state_storage::SessionState;
use yedmq_mqtt::packet::ProtocolVersion;

#[derive(Clone)]
pub struct SessionStateService {
    session_state_raft_actor: Addr<SessionStateRaftActor>,
}

impl SessionStateService {
    pub fn new(session_state_raft_actor: Addr<SessionStateRaftActor>) -> Self {
        Self {
            session_state_raft_actor,
        }
    }

    pub fn from_registry() -> Self {
        Self::new(SessionStateRaftActor::from_registry())
    }

    pub async fn store_offline_message(
        &self,
        tenant_id: String,
        client_id: String,
        packet_key: String,
    ) -> Result<(), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(StoreOfflineMessage {
                tenant_id,
                client_id,
                packet_key,
            })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn pop_offline_message(
        &self,
        tenant_id: String,
        client_id: String,
    ) -> Result<(Option<String>, Option<Vec<u8>>), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(PopOfflineMessage {
                tenant_id,
                client_id,
            })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn get_session_state_linearizable(
        &self,
        tenant_id: String,
        client_id: String,
    ) -> Result<Option<SessionState>, SessionStateRaftError> {
        self.session_state_raft_actor
            .send(GetSessionStateEnsureLinearizable {
                tenant_id,
                client_id,
            })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn create_session_state(
        &self,
        tenant_id: String,
        client_id: String,
        protocol_version: Option<ProtocolVersion>,
        session_expiry_interval: Option<u32>,
    ) -> Result<(), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(
                crate::raft::session_state::session_state_raft_actor::CreateSessionState {
                    tenant_id,
                    client_id,
                    protocol_version,
                    session_expiry_interval,
                },
            )
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn delete_session_state(
        &self,
        tenant_id: String,
        client_id: String,
        expected_disconnected_at: Option<u64>,
    ) -> Result<(), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(
                crate::raft::session_state::session_state_raft_actor::DeleteSessionState {
                    tenant_id,
                    client_id,
                    expected_disconnected_at,
                },
            )
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn update_session_connection_state(
        &self,
        tenant_id: String,
        client_id: String,
        disconnected_at: Option<u64>,
        session_expiry_interval_update: Option<u32>,
    ) -> Result<(), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(UpdateSessionConnectionState {
                tenant_id,
                client_id,
                disconnected_at,
                session_expiry_interval_update,
            })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn scan_expired_sessions(
        &self,
        now: u64,
        ttl: u64,
    ) -> Result<Vec<(String, String, u64)>, SessionStateRaftError> {
        self.session_state_raft_actor
            .send(ScanExpiredSessions { now, ttl })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn subscribe_topic(
        &self,
        tenant_id: String,
        client_id: String,
        topic: String,
        qos: u8,
    ) -> Result<(), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(SubscribeTopic {
                tenant_id,
                client_id,
                topic,
                qos,
            })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn unsubscribe_topic(
        &self,
        tenant_id: String,
        client_id: String,
        topic: String,
    ) -> Result<(), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(UnsubscribeTopic {
                tenant_id,
                client_id,
                topic,
            })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn register_inflight_rx_packet(
        &self,
        tenant_id: String,
        client_id: String,
        packet_id: u16,
        qos: u8,
        packet_key: String,
    ) -> Result<(), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(RegisterInflightRxPacket {
                tenant_id,
                client_id,
                packet_id,
                qos,
                packet_key,
            })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn register_inflight_tx_packet(
        &self,
        tenant_id: String,
        client_id: String,
        packet_id: u16,
        qos: u8,
        packet_key: String,
    ) -> Result<(), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(RegisterInflightTxPacket {
                tenant_id,
                client_id,
                packet_id,
                qos,
                packet_key,
            })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn advance_inflight_state(
        &self,
        tenant_id: String,
        client_id: String,
        packet_id: u16,
    ) -> Result<(), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(AdvanceInflightState {
                tenant_id,
                client_id,
                packet_id,
            })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn clean_finished_inflight_items(
        &self,
        tenant_id: String,
        client_id: String,
    ) -> Result<(), SessionStateRaftError> {
        self.session_state_raft_actor
            .send(InflightCleanFinishedItems {
                tenant_id,
                client_id,
            })
            .await
            .map_err(|e| SessionStateRaftError::ServiceUnavailable(e.to_string()))?
    }
}
