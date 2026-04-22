use actix::{Addr, SystemService};

use crate::raft::session_actor_map::session_actor_map_raft_actor::{
    GetSessionActorMap, GetSessionActorMapLinearizable, RegisterSessionActorMap, RenewSession,
    SessionActorMapRaftActor, SessionActorMapRaftError, UnregisterSessionActorMap,
};
use crate::session::session_actor_map_storage::{SessionActorMapEntry, SessionVersion};

#[derive(Clone)]
pub struct SessionActorMapService {
    session_actor_map_raft_actor: Addr<SessionActorMapRaftActor>,
}

impl SessionActorMapService {
    pub fn new(session_actor_map_raft_actor: Addr<SessionActorMapRaftActor>) -> Self {
        Self {
            session_actor_map_raft_actor,
        }
    }

    pub fn from_registry() -> Self {
        Self::new(SessionActorMapRaftActor::from_registry())
    }

    pub async fn renew_session(
        &self,
        tenant_id: String,
        client_id: String,
    ) -> Result<(), SessionActorMapRaftError> {
        self.session_actor_map_raft_actor
            .send(RenewSession {
                tenant_id,
                client_id,
            })
            .await
            .map_err(|e| SessionActorMapRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn register_session_actor_map(
        &self,
        tenant_id: String,
        client_id: String,
        node_id: u64,
        version: SessionVersion,
    ) -> Result<(), SessionActorMapRaftError> {
        self.session_actor_map_raft_actor
            .send(RegisterSessionActorMap {
                tenant_id,
                client_id,
                node_id,
                version,
            })
            .await
            .map_err(|e| SessionActorMapRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn unregister_session_actor_map(
        &self,
        tenant_id: String,
        client_id: String,
        version: SessionVersion,
    ) -> Result<(), SessionActorMapRaftError> {
        self.session_actor_map_raft_actor
            .send(UnregisterSessionActorMap {
                tenant_id,
                client_id,
                version,
            })
            .await
            .map_err(|e| SessionActorMapRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn get_session_actor_map(
        &self,
        tenant_id: String,
        client_id: String,
    ) -> Result<Option<SessionActorMapEntry>, SessionActorMapRaftError> {
        self.session_actor_map_raft_actor
            .send(GetSessionActorMap {
                tenant_id,
                client_id,
            })
            .await
            .map_err(|e| SessionActorMapRaftError::ServiceUnavailable(e.to_string()))?
    }

    pub async fn get_session_actor_map_linearizable(
        &self,
        tenant_id: String,
        client_id: String,
    ) -> Result<Option<SessionActorMapEntry>, SessionActorMapRaftError> {
        self.session_actor_map_raft_actor
            .send(GetSessionActorMapLinearizable {
                tenant_id,
                client_id,
            })
            .await
            .map_err(|e| SessionActorMapRaftError::ServiceUnavailable(e.to_string()))?
    }
}
