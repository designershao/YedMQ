use std::{sync::Arc, time::Duration};

use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

use crate::{
    protobuf::{
        raft_service_client::RaftServiceClient, AppendEntriesRequest, ErrorCode, ErrorDetail,
        InstallSnapshotRequest, RaftType, VoteRequest,
    }, raft::NodeTrait, session::session_state_storage::SessionStateStorageError
};

#[derive(Debug, thiserror::Error)]
pub enum RaftClientError {
    #[error("No leader available")]
    NoLeader,
    #[error("Network error: {0}")]
    NetworkError(#[from] tonic::transport::Error),
    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),
    #[error("Timeout error")]
    Timeout,
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Service error: {code:?} - {message}")]
    ServiceError {
        code: ErrorCode,
        message: String,
        node: String,
    },
    #[error("SessionStateStorage error: {0}")]
    SessionStateStorageError(#[from] SessionStateStorageError),

    #[error("Failed to serialize data: {0}")]
    SerializationError(String),
    
    #[error("Failed to deserialize data: {0}")]
    DeserializationError(String),
}

pub type Result<T> = std::result::Result<T, RaftClientError>;

#[async_trait::async_trait]
pub trait RaftClient<R>: Send + Sync 
where
    R: openraft::RaftTypeConfig,
    R::Node: NodeTrait,
{
    fn get_raft_type(&self) -> RaftType;

    async fn get_client(&self) -> Result<RaftServiceClient<Channel>>;

    async fn append_entries(&self, data: String) -> Result<String>;

    async fn vote(&self, data: String) -> Result<String>;

    async fn install_snapshot(&self, data: String) -> Result<String>;

    async fn reconnect(&self) -> Result<()>;

    async fn get_leader(&self) -> Option<R::Node>;

    async fn get_current_node_id(&self) -> R::NodeId;
}

pub struct BaseRaftClient<R>
where
    R: openraft::RaftTypeConfig,
    R::Node: NodeTrait,
{
    raft: Arc<openraft::Raft<R>>,

    current_connection: Arc<Mutex<Option<(String, RaftServiceClient<Channel>)>>>,

    max_retries: usize,

    retry_interval: Duration,

    raft_type: RaftType,
}

impl<R> BaseRaftClient<R>
where
    R: openraft::RaftTypeConfig,
    R::Node: NodeTrait,
{
    pub fn new(raft_type: RaftType, raft: Arc<openraft::Raft<R>>) -> Self {
        Self {
            raft,
            current_connection: Arc::new(Mutex::new(None)),
            max_retries: 3,
            retry_interval: Duration::from_millis(500),
            raft_type,
        }
    }

    async fn connect_to_node(&self, node_addr: &str) -> Result<RaftServiceClient<Channel>> {
        let endpoint = Endpoint::from_shared(format!("http://{}", node_addr))?;
        let channel = endpoint.connect().await?;
        Ok(RaftServiceClient::new(channel))
    }

    fn handle_error(&self, error: Option<ErrorDetail>) -> Result<()> {
        if let Some(error) = error {
            return Err(RaftClientError::ServiceError {
                code: ErrorCode::try_from(error.code).unwrap_or(ErrorCode::Unknown),
                message: error.message,
                node: error.node,
            });
        }
        Ok(())
    }

    async fn is_connected_to_leader(&self, current_addr: &str) -> bool {
        if let Some(current_leader) = self.get_leader().await {
            current_addr == current_leader.rpc_addr()
        } else {
            false
        }
    }

}

#[async_trait::async_trait]
impl<R> RaftClient<R> for BaseRaftClient<R>
where
    R: openraft::RaftTypeConfig,
    R::Node: NodeTrait,
{
    async fn get_leader(&self) -> Option<R::Node> {
        self.raft.current_leader().await.and_then(|id| {
            self.raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .find(|x| *x.0 == id)
                .and_then(|x| Some(x.1.clone()))
        })
    }

    async fn get_current_node_id(&self) -> R::NodeId {
        self.raft.metrics().borrow().id.clone()
    }

    async fn reconnect(&self) -> Result<()> {
        let mut conn_guard = self.current_connection.lock().await;
        *conn_guard = None;

        if let Some(leader_node) = self.get_leader().await { 
            match self.connect_to_node(leader_node.rpc_addr()).await {
                Ok(client) => {
                    *conn_guard = Some((leader_node.rpc_addr().clone(), client));
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn get_raft_type(&self) -> RaftType {
        self.raft_type
    }

    async fn install_snapshot(&self, data: String) -> Result<String> {
        let request = InstallSnapshotRequest {
            data,
            raft_type: self.raft_type.into(),
        };

        let mut retries = 0;

        while retries < self.max_retries {
            let mut client = self.get_client().await?;

            match client
                .install_snapshot(tonic::Request::new(request.clone()))
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    if !response.success {
                        self.handle_error(response.error)?;
                        return Err(RaftClientError::InternalError("vote failed".to_string()));
                    }

                    return Ok(response.data);
                }
                Err(_e) => {
                    self.reconnect().await?;
                    retries += 1;
                    tokio::time::sleep(self.retry_interval).await;
                    continue;
                }
            }
        }

        Err(RaftClientError::NoLeader)
    }

    async fn get_client(&self) -> Result<RaftServiceClient<Channel>> {
        let mut conn_guard = self.current_connection.lock().await;

        if let Some((current_addr, _)) = &*conn_guard {
            if !self.is_connected_to_leader(current_addr).await {
                *conn_guard = None;
            }
        }

        if conn_guard.is_none() {
            drop(conn_guard);
            self.reconnect().await?;
            conn_guard = self.current_connection.lock().await;
        }

        if let Some((_, ref client)) = *conn_guard {
            Ok(client.clone())
        } else {
            Err(RaftClientError::NoLeader)
        }
    }

    async fn vote(&self, data: String) -> Result<String> {
        let request = VoteRequest {
            data,
            raft_type: self.raft_type.into(),
        };

        let mut retries = 0;

        while retries < self.max_retries {
            let mut client = self.get_client().await?;

            match client.vote(tonic::Request::new(request.clone())).await {
                Ok(response) => {
                    let response = response.into_inner();
                    if !response.success {
                        self.handle_error(response.error)?;
                        return Err(RaftClientError::InternalError("vote failed".to_string()));
                    }

                    return Ok(response.data);
                }
                Err(_e) => {
                    self.reconnect().await?;
                    retries += 1;
                    tokio::time::sleep(self.retry_interval).await;
                    continue;
                }
            }
        }

        Err(RaftClientError::NoLeader)
    }

    async fn append_entries(&self, data: String) -> Result<String> {
        let request = AppendEntriesRequest {
            data,
            raft_type: self.raft_type.into(),
        };

        let mut retries = 0;

        let mut last_status = None;

        while retries < self.max_retries {
            let mut client = self.get_client().await?;

            match client
                .append_entries(tonic::Request::new(request.clone()))
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    if !response.success {
                        self.handle_error(response.error)?;
                        return Err(RaftClientError::InternalError(
                            "AppendEntries failed".to_string(),
                        ));
                    }

                    return Ok(response.data);
                }
                Err(_e) => {
                    self.reconnect().await?;
                    retries += 1;
                    tokio::time::sleep(self.retry_interval).await;
                    last_status = Some(_e);
                    continue;
                }
            }
        }

        Err(RaftClientError::GrpcError(last_status.unwrap()))
    }
}
