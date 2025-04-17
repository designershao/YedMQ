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

#[async_trait::async_trait]
pub trait RaftCommandExecutor<Command, Response> {

    // Check if the current node is the leader
    async fn is_leader(&self) -> bool;
    
    // Get the current leader node information
    async fn get_leader(&self) -> Option<Node>;

    // Execute command as leader
    async fn execute_as_leader(&self, command: Command) -> Result<Response, raft_manager::RaftManagerError>;

    // Create an RPC client for communication with a target node
    async fn create_rpc_client(&self, addr: String) -> Result<RaftServiceClient<Channel>, raft_manager::RaftManagerError>;

    // Send command to another node
    async fn send_command_to_node(&self, mut client: RaftServiceClient<Channel>, command: Command) -> Result<Response, raft_manager::RaftManagerError>;

}

pub async fn execute_raft_command<T, C, Response>(
    executor: &T,
    command: C,
    max_retries: u32,
) -> Result<Response, raft_manager::RaftManagerError>
where
    T: RaftCommandExecutor<C, Response>,
    C: Clone,
{
    // Try to execute command as leader first
    if executor.is_leader().await {
        return executor.execute_as_leader(command).await;
    }
    
    // If not leader, try to find the leader and forward the command
    for attempt in 0..max_retries {
        if attempt > 0 {
            // Add exponential backoff delay
            let delay = tokio::time::Duration::from_millis(100 * 2u64.pow(attempt));
            tokio::time::sleep(delay).await;
        }
        
        // Get the leader node
        if let Some(leader_node) = executor.get_leader().await {
            let addr = format!("http://{}", leader_node.rpc_addr);
            
            // Create an RPC client
            match executor.create_rpc_client(addr).await {
                Ok(client) => {
                    // Send the command to the leader
                    match executor.send_command_to_node(client, command.clone()).await {
                        Ok(res) => return Ok(res),
                        Err(e) => {
                            warn!("Failed to execute raft command {} , retrying...", e);
                            continue // Try the next retry
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to create RPC client {}, retrying...", e);
                    continue // Try the next retry
                }
            }
        }
    }
    
    Err(raft_manager::RaftManagerError::InternalError("Failed to execute raft command".into()))
}
