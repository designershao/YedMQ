use bytes::Bytes;
use std::sync::Arc;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};
use yedmq::protobuf::raft_payload::{
    payload_service_client::PayloadServiceClient,
    payload_service_server::{PayloadService, PayloadServiceServer},
    *,
};
use yedmq::raft::payload::{PayloadClient, PayloadServiceImpl, PayloadStore, RocksDBPayloadStore};

struct TestServer {
    addr: String,
    store: Arc<RocksDBPayloadStore>,
    task: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TestServer {
    async fn start(mode: Option<&'static str>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RocksDBPayloadStore::new(dir.path().join("remote")).unwrap());
        let service = FaultyService {
            inner: PayloadServiceImpl::new(store.clone()),
            mode,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(PayloadServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            addr,
            store,
            task,
            _dir: dir,
        }
    }
}

struct FaultyService {
    inner: PayloadServiceImpl,
    mode: Option<&'static str>,
}

#[tonic::async_trait]
impl PayloadService for FaultyService {
    async fn replicate_payload(
        &self,
        r: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        self.inner.replicate_payload(r).await
    }
    async fn replicate_batch(
        &self,
        r: Request<ReplicateBatchRequest>,
    ) -> Result<Response<ReplicateBatchResponse>, Status> {
        self.inner.replicate_batch(r).await
    }
    async fn replicate_stream(
        &self,
        r: Request<tonic::Streaming<BulkSyncChunk>>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        self.inner.replicate_stream(r).await
    }
    async fn fetch_payload(
        &self,
        r: Request<FetchRequest>,
    ) -> Result<Response<FetchResponse>, Status> {
        self.inner.fetch_payload(r).await
    }
    async fn start_bulk_sync(
        &self,
        r: Request<BulkSyncStartRequest>,
    ) -> Result<Response<BulkSyncStartResponse>, Status> {
        self.inner.start_bulk_sync(r).await
    }
    type StreamBulkDataStream =
        tokio_stream::wrappers::ReceiverStream<Result<BulkSyncChunk, Status>>;
    async fn stream_bulk_data(
        &self,
        r: Request<BulkSyncDataRequest>,
    ) -> Result<Response<Self::StreamBulkDataStream>, Status> {
        let Some(mode) = self.mode else {
            return self.inner.stream_bulk_data(r).await;
        };
        let key = r.into_inner().keys_to_fetch[0].clone();
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        if mode != "omitted" {
            tx.send(Ok(BulkSyncChunk {
                key,
                data: b"abc".to_vec(),
                offset: if mode == "offset" { 1 } else { 0 },
                is_last_chunk: mode != "truncated",
                total_size: if mode == "size" { 4 } else { 3 },
                checksum: if mode == "checksum" {
                    1
                } else {
                    crc32fast::hash(b"abc")
                },
            }))
            .await
            .unwrap();
        }
        drop(tx);
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

#[tokio::test]
async fn pulls_missing_payloads_including_empty_and_multiple_chunks() {
    let server = TestServer::start(None).await;
    let dir = tempfile::tempdir().unwrap();
    let local = Arc::new(RocksDBPayloadStore::new(dir.path().join("local")).unwrap());
    let values = [
        Bytes::new(),
        Bytes::from_static(b"small"),
        Bytes::from(vec![42; 2 * 1024 * 1024 + 7]),
    ];
    for (i, data) in values.iter().enumerate() {
        server
            .store
            .put(&i.to_string(), data.clone())
            .await
            .unwrap();
    }
    PayloadClient::new(local.clone())
        .bulk_sync(
            &server.addr,
            (0..3).map(|i| (i.to_string(), 0, 0)).collect(),
        )
        .await
        .unwrap();
    for (i, data) in values.iter().enumerate() {
        assert_eq!(
            local.get(&i.to_string()).await.unwrap().as_ref(),
            Some(data)
        );
    }
    // A subsequent sync is idempotent and does not require the remote copy.
    server.store.delete(&"1".into()).await.unwrap();
    PayloadClient::new(local)
        .bulk_sync(&server.addr, vec![("1".into(), 0, 0)])
        .await
        .unwrap();
}

#[tokio::test]
async fn missing_remote_payload_is_an_error() {
    let server = TestServer::start(None).await;
    let dir = tempfile::tempdir().unwrap();
    let local = Arc::new(RocksDBPayloadStore::new(dir.path()).unwrap());
    assert!(PayloadClient::new(local.clone())
        .bulk_sync(&server.addr, vec![("missing".into(), 0, 0)])
        .await
        .is_err());
    assert!(!local.contains(&"missing".into()).await.unwrap());
}

#[tokio::test]
async fn invalid_downloads_never_commit_a_payload() {
    for mode in ["omitted", "truncated", "offset", "checksum", "size"] {
        let server = TestServer::start(Some(mode)).await;
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(RocksDBPayloadStore::new(dir.path()).unwrap());
        assert!(
            PayloadClient::new(local.clone())
                .bulk_sync(&server.addr, vec![("key".into(), 0, 0)])
                .await
                .is_err(),
            "{mode}"
        );
        assert!(!local.contains(&"key".into()).await.unwrap(), "{mode}");
    }
}

#[tokio::test]
async fn replication_splits_batches_and_streams_oversized_values() {
    let server = TestServer::start(None).await;
    let values = (0..7)
        .map(|i| {
            vec![
                i;
                if i == 5 {
                    5 * 1024 * 1024 + 13
                } else {
                    1024 * 1024
                }
            ]
        })
        .collect::<Vec<_>>();
    let entries = values
        .iter()
        .enumerate()
        .map(|(i, data)| ReplicateRequest {
            key: i.to_string(),
            data: data.clone(),
            term: 1,
        })
        .collect();
    PayloadClient::replicate_entries(&server.addr, entries)
        .await
        .unwrap();
    for (i, data) in values.iter().enumerate() {
        assert_eq!(
            server
                .store
                .get(&i.to_string())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            data
        );
    }
}

#[tokio::test]
async fn failed_stream_upload_preserves_previous_value() {
    let server = TestServer::start(None).await;
    let key = "key".to_string();
    server
        .store
        .put(&key, Bytes::from_static(b"old"))
        .await
        .unwrap();
    let mut client = PayloadServiceClient::connect(format!("http://{}", server.addr))
        .await
        .unwrap();
    for mode in ["truncated", "offset", "checksum", "trailing"] {
        let mut chunks = vec![BulkSyncChunk {
            key: key.clone(),
            data: b"new".to_vec(),
            offset: if mode == "offset" { 1 } else { 0 },
            is_last_chunk: mode != "truncated",
            total_size: 3,
            checksum: if mode == "checksum" {
                1
            } else {
                crc32fast::hash(b"new")
            },
        }];
        if mode == "trailing" {
            chunks.push(chunks[0].clone());
        }
        assert!(
            client
                .replicate_stream(tokio_stream::iter(chunks))
                .await
                .is_err(),
            "{mode}"
        );
        assert_eq!(
            server.store.get(&key).await.unwrap().unwrap().as_ref(),
            b"old"
        );
    }
}

#[tokio::test]
async fn acknowledged_writes_and_deletes_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let key = "single".to_string();
    {
        let store = RocksDBPayloadStore::new(dir.path()).unwrap();
        store.put(&key, Bytes::from_static(b"one")).await.unwrap();
        store
            .put_batch(vec![("batch".into(), Bytes::from_static(b"two"))])
            .await
            .unwrap();
    }
    {
        let store = RocksDBPayloadStore::new(dir.path()).unwrap();
        assert_eq!(store.get(&key).await.unwrap().unwrap().as_ref(), b"one");
        assert_eq!(
            store.get(&"batch".into()).await.unwrap().unwrap().as_ref(),
            b"two"
        );
        store.delete(&key).await.unwrap();
        store.delete(&key).await.unwrap();
    }
    assert!(!RocksDBPayloadStore::new(dir.path())
        .unwrap()
        .contains(&key)
        .await
        .unwrap());
}
