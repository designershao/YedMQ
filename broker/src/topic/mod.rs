pub mod topic_storage;

#[derive(Debug, thiserror::Error, Clone)]
pub enum TopicError {
    #[error("topic {0} not found")]
    TopicNotFound(String),

    #[error("tenant {0} not found")]
    TenantNotFound(String),

    #[error("invalid topic filter {0}")]
    InvalidTopicFilter(String),

    #[error("internal error {0}")]
    InternalError(String),
}
