use rocksdb::{IteratorMode, Options, DB};
use serde::{de::DeserializeOwned, Serialize};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum RouteStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, RouteStoreError>;

#[derive(Clone)]
pub struct JsonRocksDBStore {
    db: Arc<DB>,
}

impl std::fmt::Debug for JsonRocksDBStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonRocksDBStore").finish()
    }
}

impl JsonRocksDBStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path).map_err(|e| RouteStoreError::Storage(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    pub async fn put<T: Serialize + Send + Sync>(&self, key: &str, value: &T) -> Result<()> {
        let db = self.db.clone();
        let key = key.to_string();
        let bytes =
            serde_json::to_vec(value).map_err(|e| RouteStoreError::Serialization(e.to_string()))?;

        tokio::task::spawn_blocking(move || {
            db.put(key.as_bytes(), bytes)
                .map_err(|e| RouteStoreError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| RouteStoreError::Io(std::io::Error::other(e)))??;

        Ok(())
    }

    pub async fn get<T: DeserializeOwned + Send + 'static>(&self, key: &str) -> Result<Option<T>> {
        let db = self.db.clone();
        let key = key.to_string();

        let value = tokio::task::spawn_blocking(move || {
            db.get(key.as_bytes())
                .map_err(|e| RouteStoreError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| RouteStoreError::Io(std::io::Error::other(e)))??;

        value
            .map(|raw| {
                serde_json::from_slice(&raw)
                    .map_err(|e| RouteStoreError::Serialization(e.to_string()))
            })
            .transpose()
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let db = self.db.clone();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || {
            db.delete(key.as_bytes())
                .map_err(|e| RouteStoreError::Storage(e.to_string()))
        })
        .await
        .map_err(|e| RouteStoreError::Io(std::io::Error::other(e)))??;

        Ok(())
    }

    pub async fn scan<T: DeserializeOwned + Send + 'static>(&self) -> Result<Vec<(String, T)>> {
        let db = self.db.clone();

        let rows = tokio::task::spawn_blocking(move || {
            let iter = db.iterator(IteratorMode::Start);
            iter.map(|item| {
                item.map_err(|e| RouteStoreError::Storage(e.to_string()))
                    .and_then(|(key, value)| {
                        let key = String::from_utf8(key.to_vec())
                            .map_err(|e| RouteStoreError::Serialization(e.to_string()))?;
                        let value = serde_json::from_slice::<T>(&value)
                            .map_err(|e| RouteStoreError::Serialization(e.to_string()))?;
                        Ok((key, value))
                    })
            })
            .collect::<Result<Vec<_>>>()
        })
        .await
        .map_err(|e| RouteStoreError::Io(std::io::Error::other(e)))??;

        Ok(rows)
    }
}
