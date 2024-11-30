use std::{sync::atomic::AtomicU64, time::Instant};

pub struct Metric {

    clients_connected: AtomicU64,

    bytes_received: AtomicU64,

    bytes_sent: AtomicU64,

    start_time: Instant,
}

impl Metric {

    pub fn new() -> Metric {
        Metric {

            clients_connected: AtomicU64::new(0),

            bytes_received: AtomicU64::new(0),

            bytes_sent: AtomicU64::new(0),

            start_time: Instant::now()
        }
    }

    pub fn get_uptime(&self) -> u64 {  
        self.start_time.elapsed().as_secs()
    }

    pub fn increase_clients_connected(&self) {
        self.clients_connected.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn decrease_clients_connected(&self) {
        self.clients_connected.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn increase_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn increase_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
    }

}