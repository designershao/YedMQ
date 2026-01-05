use types::SessionStateTypeConfig;

pub mod raft_network_impl;
pub mod session_state_raft_actor;
pub mod store;
pub mod types;

pub type SessionStateRaft = openraft::Raft<SessionStateTypeConfig>;
