use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub type PayloadKey = String; // Use String for convenience, or Uuid

#[derive(Error, Debug)]
pub enum PayloadError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Payload not found: {0}")]
    NotFound(PayloadKey),
}

pub type Result<T> = std::result::Result<T, PayloadError>;

use std::fmt::Debug;

#[async_trait]
pub trait PayloadStore: Send + Sync + Debug + 'static {
    /// Write Payload
    ///
    /// It is recommended to include fsync logic in the implementation to ensure durability
    async fn put(&self, key: &PayloadKey, data: Bytes) -> Result<()>;

    /// Read Payload
    async fn get(&self, key: &PayloadKey) -> Result<Option<Bytes>>;

    /// Delete Payload.
    /// This operation should be idempotent: deleting a missing key should return `Ok(())`.
    async fn delete(&self, key: &PayloadKey) -> Result<()>;

    /// Check if payload exists
    async fn contains(&self, key: &PayloadKey) -> Result<bool>;

    /// Batch write (used for BulkSync receiver side optimization)
    async fn put_batch(&self, entries: Vec<(PayloadKey, Bytes)>) -> Result<()>;
}
