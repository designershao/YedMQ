use std::sync::Arc;

use openraft::Config;

use super::{NodeId, YedMQRaft};

pub struct App {
    pub id: NodeId,
    pub api_addr: String,
    pub rpc_addr: String,
    pub raft: YedMQRaft,
    pub config: Arc<Config>,
}