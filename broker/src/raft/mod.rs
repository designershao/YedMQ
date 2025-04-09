use backoff::{backoff::Backoff, ExponentialBackoff};
use log::warn;
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;
use std::{fmt::Display, io::Cursor, time::Duration};

use crate::protobuf::raft_service_client::RaftServiceClient;

pub mod raft_manager;
pub mod service;
pub mod session_actor_map;
pub mod session_state;
pub mod topic;

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Node {
    pub rpc_addr: String,
    pub api_addr: String,
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node {{ rpc_addr: {}, api_addr: {} }}",
            self.rpc_addr, self.api_addr
        )
    }
}

pub type SnapshotData = Cursor<Vec<u8>>;

pub enum RaftManagerError {
    Raft(openraft::AnyError),
}

async fn create_rpc_client_with_retry(addr: String) -> anyhow::Result<RaftServiceClient<Channel>> {
    let mut backoff = ExponentialBackoff {
        initial_interval: Duration::from_millis(100),
        max_interval: Duration::from_secs(10),
        multiplier: 2.0,
        max_elapsed_time: Some(Duration::from_secs(60)),
        ..ExponentialBackoff::default()
    };

    let channel = loop {
        match tonic::transport::Endpoint::from_shared(addr.clone())?
            .connect()
            .await
        {
            Ok(channel) => break channel,
            Err(e) => {
                if let Some(duration) = backoff.next_backoff() {
                    warn!(
                        "RPC client connection failed: {}. Retrying in {:?}...",
                        e, duration
                    );
                    tokio::time::sleep(duration).await;
                } else {
                    return Err(anyhow::anyhow!(format!(
                        "Failed to connect after retries: {}",
                        e
                    )));
                }
            }
        }
    };

    Ok(RaftServiceClient::new(channel))
}
