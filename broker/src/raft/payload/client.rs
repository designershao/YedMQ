use super::transfer::{chunks, ChunkAssembler, RPC_SIZE_LIMIT};
use crate::protobuf::raft_payload::payload_service_client::PayloadServiceClient;
use crate::protobuf::raft_payload::{BulkSyncDataRequest, ReplicateBatchRequest, ReplicateRequest};
use crate::raft::payload::store::{PayloadKey, PayloadStore};
use crate::raft::GRPCBusinessError;
use bytes::Bytes;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;

#[derive(Clone, Debug, thiserror::Error)]
pub enum PayloadClientError {
    #[error("gRPC error: {0}")]
    GRPC(#[from] tonic::Status),

    #[error("gRPC connect error: {0}")]
    GRPCConnect(String),

    #[error("gRPC business error: {0}")]
    GRPCBusiness(GRPCBusinessError),

    #[error("local payload error: {0}")]
    PayloadError(String),

    #[error("payload not found on the remote node")]
    PayloadNotFound,
}

pub struct PayloadClient {
    store: Arc<dyn PayloadStore>,
}

impl std::fmt::Debug for PayloadClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadClient").finish()
    }
}

impl PayloadClient {
    pub fn new(store: Arc<dyn PayloadStore>) -> Self {
        Self { store }
    }

    pub async fn fetch_and_store(
        &self,
        addr: &str,
        key: PayloadKey,
    ) -> Result<(), PayloadClientError> {
        self.bulk_sync(addr, vec![(key, 0, 0)])
            .await
            .map_err(|e| PayloadClientError::PayloadError(e.to_string()))
    }

    pub async fn replicate(
        &self,
        addr: &str,
        key: PayloadKey,
        term: u64,
    ) -> Result<(), PayloadClientError> {
        let data = self
            .store
            .get(&key)
            .await
            .map_err(|e| PayloadClientError::PayloadError(e.to_string()))?
            .ok_or_else(|| {
                PayloadClientError::PayloadError(format!("Payload not found locally: {}", key))
            })?;
        Self::replicate_entries(
            addr,
            vec![ReplicateRequest {
                key,
                data: data.to_vec(),
                term,
            }],
        )
        .await
    }

    /// Split by protobuf encoded size. A single oversized value uses bounded chunks.
    pub async fn replicate_entries(
        addr: &str,
        entries: Vec<ReplicateRequest>,
    ) -> Result<(), PayloadClientError> {
        let mut client = PayloadServiceClient::new(
            crate::rpc::grpc_client::lazy_channel(addr)
                .map_err(|e| PayloadClientError::GRPCConnect(e.to_string()))?,
        );
        let mut batch = ReplicateBatchRequest {
            entries: Vec::new(),
        };
        let mut batch_size = 0;
        for entry in entries {
            let len = entry.encoded_len();
            let encoded_size = 1 + prost::length_delimiter_len(len) + len;
            if batch_size + encoded_size > RPC_SIZE_LIMIT && !batch.entries.is_empty() {
                let response = client.replicate_batch(std::mem::take(&mut batch)).await?;
                if !response.into_inner().success {
                    return Err(PayloadClientError::PayloadError("batch rejected".into()));
                }
                batch_size = 0;
            }
            if encoded_size > RPC_SIZE_LIMIT {
                let response = client
                    .replicate_stream(tokio_stream::iter(chunks(
                        entry.key,
                        Bytes::from(entry.data),
                    )))
                    .await?;
                if !response.into_inner().success {
                    return Err(PayloadClientError::PayloadError("upload rejected".into()));
                }
            } else {
                batch_size += encoded_size;
                batch.entries.push(entry);
            }
        }
        if !batch.entries.is_empty() {
            let response = client.replicate_batch(batch).await?;
            if !response.into_inner().success {
                return Err(PayloadClientError::PayloadError("batch rejected".into()));
            }
        }
        Ok(())
    }

    pub async fn bulk_sync(
        &self,
        addr: &str,
        manifest: Vec<(PayloadKey, u64, u32)>,
    ) -> anyhow::Result<()> {
        // This is a pull: the requested keys are missing HERE. StartBulkSync is
        // a receiver-side diff for pushes and must not decide what we fetch.
        let mut pending = HashMap::new();
        for (key, size, checksum) in manifest {
            if !self.store.contains(&key).await? {
                pending.insert(key, (size, checksum));
            }
        }
        let mut client = PayloadServiceClient::new(crate::rpc::grpc_client::lazy_channel(addr)?);
        let mut requests = Vec::new();
        let session_id = uuid::Uuid::new_v4().to_string();
        let mut request = BulkSyncDataRequest {
            session_id: session_id.clone(),
            keys_to_fetch: Vec::new(),
        };
        let mut request_size = request.encoded_len();
        for key in pending.keys() {
            let size = 1 + prost::length_delimiter_len(key.len()) + key.len();
            anyhow::ensure!(
                size + session_id.len() + 2 <= RPC_SIZE_LIMIT,
                "payload key too large"
            );
            if request_size + size > RPC_SIZE_LIMIT {
                requests.push(request);
                request = BulkSyncDataRequest {
                    session_id: session_id.clone(),
                    keys_to_fetch: Vec::new(),
                };
                request_size = request.encoded_len();
            }
            request.keys_to_fetch.push(key.clone());
            request_size += size;
        }
        if !request.keys_to_fetch.is_empty() {
            requests.push(request);
        }
        for request in requests {
            let mut stream = client.stream_bulk_data(request).await?.into_inner();
            let mut assembler = ChunkAssembler::default();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                anyhow::ensure!(
                    pending.contains_key(&chunk.key),
                    "unexpected or duplicate payload key"
                );
                if let Some((key, data)) = assembler.push(chunk)? {
                    let (size, checksum) = pending[&key];
                    anyhow::ensure!(
                        size == 0 || size == data.len() as u64,
                        "manifest size mismatch"
                    );
                    anyhow::ensure!(
                        checksum == 0 || checksum == crc32fast::hash(&data),
                        "manifest checksum mismatch"
                    );
                    self.store.put(&key, data).await?;
                    pending.remove(&key);
                }
            }
            assembler.finish()?;
        }
        anyhow::ensure!(
            pending.is_empty(),
            "peer omitted {} requested payloads",
            pending.len()
        );
        Ok(())
    }
}
