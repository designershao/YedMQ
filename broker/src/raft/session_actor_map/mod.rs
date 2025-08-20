use types::SessionActorMapTypeConfig;

use super::NodeId;

pub mod raft_network_impl;
pub mod session_actor_map_raft_actor;
pub mod store;
pub mod types;

pub type SessionActorMapRaft = openraft::Raft<SessionActorMapTypeConfig>;
