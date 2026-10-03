use super::transfer::{chunks, ChunkAssembler};
use crate::protobuf::raft_payload::payload_service_server::PayloadService;
use crate::protobuf::raft_payload::{
    BulkSyncChunk, BulkSyncDataRequest, BulkSyncStartRequest, BulkSyncStartResponse, FetchRequest,
    FetchResponse, ReplicateBatchRequest, ReplicateBatchResponse, ReplicateRequest,
    ReplicateResponse,
};
use crate::raft::payload::store::PayloadStore;
use bytes::Bytes;
use log::{debug, error};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct PayloadServiceImpl {
    store: Arc<dyn PayloadStore>,
}

impl std::fmt::Debug for PayloadServiceImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadServiceImpl").finish()
    }
}

impl PayloadServiceImpl {
    pub fn new(store: Arc<dyn PayloadStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl PayloadService for PayloadServiceImpl {
    async fn replicate_payload(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let req = request.into_inner();
        // info!("Received payload replication for key: {}", req.key);

        self.store
            .put(&req.key, Bytes::from(req.data))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ReplicateResponse { success: true }))
    }

    async fn replicate_batch(
        &self,
        request: Request<ReplicateBatchRequest>,
    ) -> Result<Response<ReplicateBatchResponse>, Status> {
        let req = request.into_inner();
        // debug!("Received batch payload replication with {} entries", req.entries.len());

        // Prepare vector for batch put
        let entries: Vec<(String, Bytes)> = req
            .entries
            .into_iter()
            .map(|e| (e.key, Bytes::from(e.data)))
            .collect();

        if let Err(e) = self.store.put_batch(entries).await {
            error!("Failed to batch write payloads: {}", e);
            return Err(Status::internal(e.to_string()));
        }

        Ok(Response::new(ReplicateBatchResponse { success: true }))
    }

    async fn replicate_stream(
        &self,
        request: Request<tonic::Streaming<BulkSyncChunk>>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let mut stream = request.into_inner();
        let mut assembler = ChunkAssembler::default();
        let mut completed = None;
        while let Some(chunk) = stream.message().await? {
            if completed.is_some() {
                return Err(Status::invalid_argument("expected exactly one payload"));
            }
            completed = assembler
                .push(chunk)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        assembler
            .finish()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let (key, data) = completed.ok_or_else(|| Status::invalid_argument("empty upload"))?;
        self.store
            .put(&key, data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ReplicateResponse { success: true }))
    }

    async fn fetch_payload(
        &self,
        request: Request<FetchRequest>,
    ) -> Result<Response<FetchResponse>, Status> {
        let req = request.into_inner();
        debug!("Received payload fetch for key: {}", req.key);

        match self.store.get(&req.key).await {
            Ok(Some(data)) => Ok(Response::new(FetchResponse {
                found: true,
                data: data.to_vec(),
            })),
            Ok(None) => Ok(Response::new(FetchResponse {
                found: false,
                data: Vec::new(),
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn start_bulk_sync(
        &self,
        request: Request<BulkSyncStartRequest>,
    ) -> Result<Response<BulkSyncStartResponse>, Status> {
        let req = request.into_inner();
        let mut missing_keys = Vec::new();

        for manifest_item in req.manifest {
            match self.store.contains(&manifest_item.key).await {
                Ok(exists) => {
                    if !exists {
                        missing_keys.push(manifest_item.key);
                    }
                }
                Err(e) => {
                    error!("Error checking payload existence: {}", e);
                    return Err(Status::internal(e.to_string()));
                }
            }
        }

        Ok(Response::new(BulkSyncStartResponse {
            accepted: true,
            missing_keys,
        }))
    }

    type StreamBulkDataStream =
        tokio_stream::wrappers::ReceiverStream<Result<BulkSyncChunk, Status>>;

    async fn stream_bulk_data(
        &self,
        request: Request<BulkSyncDataRequest>,
    ) -> Result<Response<Self::StreamBulkDataStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let store = self.store.clone();

        tokio::spawn(async move {
            for key in req.keys_to_fetch {
                match store.get(&key).await {
                    Ok(Some(data)) => {
                        for chunk in chunks(key, data) {
                            if tx.send(Ok(chunk)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Ok(None) => {
                        let _ = tx
                            .send(Err(Status::not_found(format!("payload {key} not found"))))
                            .await;
                        return;
                    }
                    Err(e) => {
                        error!("BulkSync: Error reading key {}: {}", key, e);
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}
