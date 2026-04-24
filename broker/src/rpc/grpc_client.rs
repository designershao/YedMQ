use std::sync::OnceLock;
use std::time::Duration;

use dashmap::{mapref::entry::Entry, DashMap};
use tonic::transport::{Channel, Endpoint};

#[derive(Default)]
struct GrpcChannelPool {
    channels: DashMap<String, Channel>,
}

impl GrpcChannelPool {
    fn channel(&self, addr: &str) -> Result<Channel, tonic::transport::Error> {
        self.channel_with_connect_timeout(addr, None)
    }

    fn channel_with_connect_timeout(
        &self,
        addr: &str,
        timeout: Option<Duration>,
    ) -> Result<Channel, tonic::transport::Error> {
        let key = pool_key(addr, timeout);
        if let Some(channel) = self.channels.get(&key) {
            return Ok(channel.clone());
        }

        let channel = build_channel(addr, timeout)?;
        match self.channels.entry(key) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                entry.insert(channel.clone());
                Ok(channel)
            }
        }
    }

    #[cfg(test)]
    fn clear(&self) {
        self.channels.clear();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.channels.len()
    }
}

fn pool_key(addr: &str, timeout: Option<Duration>) -> String {
    match timeout {
        Some(timeout) => format!("{addr}#{}", timeout.as_nanos()),
        None => addr.to_string(),
    }
}

fn build_channel(
    addr: &str,
    timeout: Option<Duration>,
) -> Result<Channel, tonic::transport::Error> {
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))?;
    let endpoint = if let Some(timeout) = timeout {
        endpoint.connect_timeout(timeout)
    } else {
        endpoint
    };

    Ok(endpoint.connect_lazy())
}

fn global_pool() -> &'static GrpcChannelPool {
    static POOL: OnceLock<GrpcChannelPool> = OnceLock::new();
    POOL.get_or_init(GrpcChannelPool::default)
}

pub fn lazy_channel(addr: &str) -> Result<Channel, tonic::transport::Error> {
    global_pool().channel(addr)
}

pub fn lazy_channel_with_connect_timeout(
    addr: &str,
    timeout: Duration,
) -> Result<Channel, tonic::transport::Error> {
    global_pool().channel_with_connect_timeout(addr, Some(timeout))
}

pub async fn connected_channel(
    addr: &str,
    timeout: Duration,
) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_shared(format!("http://{addr}"))?
        .connect_timeout(timeout)
        .connect()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn reuses_cached_channel_for_same_addr() {
        let pool = global_pool();
        pool.clear();

        let _first = lazy_channel("127.0.0.1:9080").expect("channel should be created");
        let _second = lazy_channel("127.0.0.1:9080").expect("channel should be reused");

        assert_eq!(pool.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn keeps_separate_entries_for_different_timeouts() {
        let pool = global_pool();
        pool.clear();

        let _default = lazy_channel("127.0.0.1:9080").expect("default channel should be created");
        let _timed = lazy_channel_with_connect_timeout("127.0.0.1:9080", Duration::from_secs(5))
            .expect("timed channel should be created");

        assert_eq!(pool.len(), 2);
    }
}
