pub mod topic_manager;
pub mod topic_storage;

#[derive(Debug, thiserror::Error)]
pub enum TopicError {
    #[error("topic {0} not found")]
    TopicNotFound(String),

    #[error("tenant {0} not found")]
    TenantNotFound(String),

    #[error("invalid topic filter {0}")]
    InvalidTopicFilter(String),

    #[error("network error: {0}")]
    NetworkError(#[from] NetworkError),

    #[error("raft error {0}")]
    RaftError(#[from] openraft::error::RaftError<crate::raft::NodeId>),

    #[error("raft client error: {0}")]
    RaftClientError(#[from] crate::raft::client::base::RaftClientError),

    #[error("internal error {0}")]
    InternalError(String),
}

#[derive(Debug,  thiserror::Error)]
pub enum NetworkError {
    #[error("network timeout")]
    Timeout,

    #[error("connection closed")]
    ConnectionClosed,

    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),

    #[error("DNS error: {0}")]
    DnsError(String),
}
