use std::sync::Arc;
use crate::protobuf::raft_payload::payload_service_client::PayloadServiceClient;
use crate::protobuf::raft_payload::{FetchRequest, BulkSyncStartRequest, BulkSyncDataRequest, PayloadManifestItem};
use crate::raft::payload::store::{PayloadStore, PayloadKey};
use bytes::Bytes;
use log::{info, warn};
use tokio_stream::StreamExt;

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

    pub async fn fetch_and_store(&self, addr: &str, key: PayloadKey) -> anyhow::Result<()> {
        let mut client = PayloadServiceClient::connect(format!("http://{}", addr)).await?;
        let resp = client.fetch_payload(FetchRequest {
            key: key.clone(),
        }).await?;
        
        let inner = resp.into_inner();
        if inner.found {
            self.store.put(&key, Bytes::from(inner.data)).await?;
            Ok(())
        } else {
            anyhow::bail!("Payload not found on remote: {}", key)
        }
    }

    pub async fn replicate(&self, addr: &str, key: PayloadKey, term: u64) -> anyhow::Result<()> {
        let data = self.store.get(&key).await?.ok_or_else(|| anyhow::anyhow!("Payload not found locally: {}", key))?;
        let mut client = PayloadServiceClient::connect(format!("http://{}", addr)).await?;
        let resp = client.replicate_payload(crate::protobuf::raft_payload::ReplicateRequest {
            key,
            data: data.to_vec(),
            term,
        }).await?;
        if resp.into_inner().success {
            Ok(())
        } else {
            anyhow::bail!("Replication failed on remote")
        }
    }

    pub async fn bulk_sync(&self, addr: &str, manifest: Vec<(PayloadKey, u64, u32)>) -> anyhow::Result<()> {
        let mut client = PayloadServiceClient::connect(format!("http://{}", addr)).await?;
        
        let proto_manifest = manifest.into_iter().map(|(key, size, checksum)| {
            PayloadManifestItem {
                key,
                size,
                checksum,
            }
        }).collect();

        let start_resp = client.start_bulk_sync(BulkSyncStartRequest {
            session_id: uuid::Uuid::new_v4().to_string(),
            manifest: proto_manifest,
        }).await?.into_inner();

        if !start_resp.accepted {
            anyhow::bail!("BulkSync rejected by remote");
        }

        if start_resp.missing_keys.is_empty() {
            return Ok(());
        }

        info!("BulkSync: fetching {} missing keys from {}", start_resp.missing_keys.len(), addr);

        let mut stream = client.stream_bulk_data(BulkSyncDataRequest {
            session_id: uuid::Uuid::new_v4().to_string(),
            keys_to_fetch: start_resp.missing_keys,
        }).await?.into_inner();

        while let Some(chunk_res) = stream.next().await {
            let chunk = chunk_res?;
            // NOTE: This simple implementation assumes chunks arrive in order for each key
            // and we just append or write. For now, since we use RocksDB and put_batch or simple put,
            // we'll just handle it. 
            // In a more robust version, we'd buffer chunks and verify checksums.
            if chunk.is_last_chunk {
                // For now, our proto StreamBulkData is simple. 
                // Let's just store the data.
                self.store.put(&chunk.key, Bytes::from(chunk.data)).await?;
            } else {
                // TODO: Handle multi-chunk payloads if they exceed 1MB
                warn!("Multi-chunk payloads not fully supported in this sync helper yet");
            }
        }

        Ok(())
    }
}
