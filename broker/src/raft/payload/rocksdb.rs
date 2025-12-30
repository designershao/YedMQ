use std::path::Path;
use std::sync::Arc;
use crate::raft::payload::store::{PayloadStore, PayloadKey, PayloadError, Result};
use async_trait::async_trait;
use bytes::Bytes;
use rocksdb::{DB, Options, WriteBatch};
use log::info;

#[derive(Clone)]
pub struct RocksDBPayloadStore {
    db: Arc<DB>,
}

impl std::fmt::Debug for RocksDBPayloadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDBPayloadStore").finish()
    }
}

impl RocksDBPayloadStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Optimization for Blobs/large values (optional)
        // opts.set_enable_blob_files(true); // If supported by rust-rocksdb
        // opts.set_min_blob_size(1024);

        let db = DB::open(&opts, path).map_err(|e| PayloadError::Storage(e.to_string()))?;
        info!("RocksDBPayloadStore initialized.");
        Ok(Self {
            db: Arc::new(db),
        })
    }
}

#[async_trait]
impl PayloadStore for RocksDBPayloadStore {
    async fn put(&self, key: &PayloadKey, data: Bytes) -> Result<()> {
        let db = self.db.clone();
        let key = key.clone();
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            db.put(key.as_bytes(), data).map_err(|e| PayloadError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| PayloadError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))??;

        Ok(())
    }

    async fn get(&self, key: &PayloadKey) -> Result<Option<Bytes>> {
        let db = self.db.clone();
        let key = key.clone();

        let result = tokio::task::spawn_blocking(move || {
            db.get(key.as_bytes()).map_err(|e| PayloadError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| PayloadError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))??;

        Ok(result.map(Bytes::from))
    }

    async fn delete(&self, key: &PayloadKey) -> Result<()> {
        let db = self.db.clone();
        let key = key.clone();

        tokio::task::spawn_blocking(move || {
            db.delete(key.as_bytes()).map_err(|e| PayloadError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| PayloadError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))??;

        Ok(())
    }

    async fn contains(&self, key: &PayloadKey) -> Result<bool> {
        let db = self.db.clone();
        let key = key.clone();
        
        let result = tokio::task::spawn_blocking(move || {
             db.get(key.as_bytes()).map_err(|e| PayloadError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| PayloadError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))??;
        
        Ok(result.is_some())
    }

    async fn put_batch(&self, entries: Vec<(PayloadKey, Bytes)>) -> Result<()> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            for (key, val) in entries {
                batch.put(key.as_bytes(), val.as_ref());
            }
            db.write(batch).map_err(|e| PayloadError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| PayloadError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))??;

        Ok(())
    }
}
