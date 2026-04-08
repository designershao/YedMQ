use crate::session::session_actor::{AcceptRoutedPublish, GetSessionInfo, SessionActorMessage};
use crate::session::session_actor_map_storage::SessionVersion;
use actix::Recipient;
use dashmap::DashMap;
use std::sync::Arc;

pub struct SessionActorRecipientWrapper {
    pub session_actor_message_recipient: Recipient<SessionActorMessage>,
    pub accept_routed_publish_recipient: Recipient<AcceptRoutedPublish>,
    pub get_session_info_recipient: Recipient<GetSessionInfo>,
    pub session_version: SessionVersion,
}

#[derive(Clone)]
pub struct SessionRegistry {
    // Outer map key: Tenant ID
    // Inner map key: Client ID
    sessions: Arc<DashMap<String, Arc<DashMap<String, SessionActorRecipientWrapper>>>>,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
        }
    }
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_session(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Option<Recipient<SessionActorMessage>> {
        if let Some(tenant_sessions) = self.sessions.get(tenant_id) {
            if let Some(wrapper) = tenant_sessions.get(client_id) {
                return Some(wrapper.session_actor_message_recipient.clone());
            }
        }
        None
    }

    pub fn get_accept_routed_publish(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Option<Recipient<AcceptRoutedPublish>> {
        if let Some(tenant_sessions) = self.sessions.get(tenant_id) {
            if let Some(wrapper) = tenant_sessions.get(client_id) {
                return Some(wrapper.accept_routed_publish_recipient.clone());
            }
        }
        None
    }

    // This exposes the inner structure for SessionManagerActor to manipulate directly.
    // In a cleaner design we would wrap all operations, but for refactoring speed we expose the inner map.
    pub fn get_inner(
        &self,
    ) -> Arc<DashMap<String, Arc<DashMap<String, SessionActorRecipientWrapper>>>> {
        self.sessions.clone()
    }
}
