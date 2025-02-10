use std::sync::Arc;

use openraft::LogId;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StoredMembership;
use rocksdb::DB;
use serde::Serialize;
use serde::Deserialize;
use yedmq::topic::Subscription;

use super::TypeConfig;
use super::{NodeId, Node};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    // Subscribe topic 
    SubscribeTopic {
        tenant_name: String,
        topic_name: String,
        qos: u8
    },
    // Unsubscribe topic
    UnsubscribeTopic {
        tenant_name: String,
        topic_name: String
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    get_topic_subscriptions_response {
        subscriptions: Vec<Subscription>
    },
    get_topic_router_response {
        nodes: Vec<Node>
    }
}

#[derive(Debug, Clone)]
pub struct StoredSnapshot {

    pub meta: SnapshotMeta<NodeId, Node>,

    pub data: Vec<u8>,

}

#[derive(Debug, Clone)]
pub struct StateMachineData {

    pub last_applied_log_id: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, Node>,

    // TODO: Topic Manager Inner Data

    // TODO: Topic Route Table Data
}

#[derive(Debug, Clone)]
pub struct StateMachineStore {

    pub data: StateMachineData,

    snapshot_idx: u64,

    db: Arc<DB>

}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    #[doc = " Build snapshot"]
#[doc = ""]
#[doc = " A snapshot has to contain state of all applied log, including membership. Usually it is just"]
#[doc = " a serialized state machine."]
#[doc = ""]
#[doc = " Building snapshot can be done by:"]
#[doc = " - Performing log compaction, e.g. merge log entries that operates on the same key, like a"]
#[doc = "   LSM-tree does,"]
#[doc = " - or by fetching a snapshot from the state machine."]
async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig> ,StorageError<NodeId>> {
        todo!()
    }
}