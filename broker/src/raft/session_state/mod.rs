use types::SessionStateTypeConfig;


pub mod raft_network_impl;
pub mod store;
pub mod types;
pub mod session_state_raft_actor;

pub type SessionStateRaft = openraft::Raft<SessionStateTypeConfig>;
