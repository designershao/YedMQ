use std::{
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};

use actix::Addr;
use bytes::Bytes;
use yedmq_mqtt::packet::{Packet, Properties, ProtocolVersion, Publish};

use crate::router_actor::{self, RouterActor};

pub struct Metric {
    pub clients_connected: AtomicU64,

    pub bytes_received: AtomicU64,

    pub bytes_sent: AtomicU64,

    pub packets_received: AtomicU64,

    pub packets_sent: AtomicU64,

    pub messages_received: AtomicU64,

    pub messages_sent: AtomicU64,

    pub messages_dropped: AtomicU64,

    pub subscriptions_count: AtomicU64,

    pub start_time: Instant,
}

impl Default for Metric {
    fn default() -> Self {
        Self::new()
    }
}

impl Metric {
    pub fn new() -> Metric {
        Metric {
            clients_connected: AtomicU64::new(0),

            bytes_received: AtomicU64::new(0),

            bytes_sent: AtomicU64::new(0),

            packets_received: AtomicU64::new(0),

            packets_sent: AtomicU64::new(0),

            messages_received: AtomicU64::new(0),

            messages_sent: AtomicU64::new(0),

            messages_dropped: AtomicU64::new(0),

            subscriptions_count: AtomicU64::new(0),

            start_time: Instant::now(),
        }
    }

    pub fn get_uptime(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn increase_clients_connected(&self) {
        self.clients_connected
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn decrease_clients_connected(&self) {
        self.clients_connected
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn increase_bytes_received(&self, bytes: u64) {
        self.bytes_received
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn increase_bytes_sent(&self, bytes: u64) {
        self.bytes_sent
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn increase_packets_received(&self) {
        self.packets_received
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn increase_packets_sent(&self) {
        self.packets_sent
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn increase_messages_received(&self) {
        self.messages_received
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn increase_messages_sent(&self) {
        self.messages_sent
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn increase_messages_dropped(&self) {
        self.messages_dropped
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn increase_subscriptions_count(&self) {
        self.subscriptions_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn decrease_subscriptions_count(&self) {
        self.subscriptions_count
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

pub struct SysTopicTask {
    metric: Arc<Metric>,

    interval_secs: u64,

    router_actor: Option<Addr<RouterActor>>,
}

impl SysTopicTask {
    pub fn new(metric: Arc<Metric>, interval_secs: u64) -> SysTopicTask {
        SysTopicTask {
            metric,
            interval_secs,
            router_actor: None,
        }
    }

    pub fn set_router_actor(&mut self, router_actor: Addr<RouterActor>) {
        self.router_actor = Some(router_actor);
    }

    fn sys_publish(topic_name: String, payload: Bytes) -> Packet {
        Packet::Publish(Publish {
            protocol_version: ProtocolVersion::V3_1_1,
            topic_name,
            payload,
            qos: 0,
            retain: false,
            dup: false,
            packet_identifier: None,
            properties: Properties::default(),
            expires_at_unix_secs: None,
        })
    }

    pub async fn run(&self) {
        let clients_connected_topic = "$SYS/broker/clients/connected".to_string();
        let broker_bytes_sent_topic = "$SYS/broker/bytes/sent".to_string();
        let broker_bytes_received_topic = "$SYS/broker/bytes/received".to_string();
        let broker_uptime_topic = "$SYS/broker/uptime".to_string();
        let packets_received_topic = "$SYS/broker/packets/received".to_string();
        let packets_sent_topic = "$SYS/broker/packets/sent".to_string();
        let messages_received_topic = "$SYS/broker/messages/received".to_string();
        let messages_sent_topic = "$SYS/broker/messages/sent".to_string();
        let messages_dropped_topic = "$SYS/broker/messages/dropped".to_string();
        let subscriptions_count_topic = "$SYS/broker/subscriptions/count".to_string();

        let mut sys_topic_interval = tokio::time::interval(Duration::from_secs(self.interval_secs));
        let metric = self.metric.clone();

        loop {
            sys_topic_interval.tick().await;
            let clients_connected = metric
                .clients_connected
                .load(std::sync::atomic::Ordering::SeqCst);
            let bytes_received = metric
                .bytes_received
                .load(std::sync::atomic::Ordering::SeqCst);
            let bytes_sent = metric.bytes_sent.load(std::sync::atomic::Ordering::SeqCst);
            let packets_received = metric
                .packets_received
                .load(std::sync::atomic::Ordering::SeqCst);
            let packets_sent = metric
                .packets_sent
                .load(std::sync::atomic::Ordering::SeqCst);
            let messages_received = metric
                .messages_received
                .load(std::sync::atomic::Ordering::SeqCst);
            let messages_sent = metric
                .messages_sent
                .load(std::sync::atomic::Ordering::SeqCst);
            let messages_dropped = metric
                .messages_dropped
                .load(std::sync::atomic::Ordering::SeqCst);
            let subscriptions_count = metric
                .subscriptions_count
                .load(std::sync::atomic::Ordering::SeqCst);

            let clients_connected_packet = Self::sys_publish(
                clients_connected_topic.clone(),
                Bytes::copy_from_slice(vec![clients_connected.to_le_bytes()[0]].as_slice()),
            );
            let bytes_received_packet = Self::sys_publish(
                broker_bytes_received_topic.clone(),
                Bytes::copy_from_slice(vec![bytes_received.to_le_bytes()[0]].as_slice()),
            );
            let bytes_sent_packet = Self::sys_publish(
                broker_bytes_sent_topic.clone(),
                Bytes::copy_from_slice(vec![bytes_sent.to_le_bytes()[0]].as_slice()),
            );
            let uptime_packet = Self::sys_publish(
                broker_uptime_topic.clone(),
                Bytes::copy_from_slice(vec![metric.get_uptime().to_le_bytes()[0]].as_slice()),
            );

            let packets_received_packet = Self::sys_publish(
                packets_received_topic.clone(),
                Bytes::copy_from_slice(vec![packets_received.to_le_bytes()[0]].as_slice()),
            );
            let packets_sent_packet = Self::sys_publish(
                packets_sent_topic.clone(),
                Bytes::copy_from_slice(vec![packets_sent.to_le_bytes()[0]].as_slice()),
            );
            let messages_received_packet = Self::sys_publish(
                messages_received_topic.clone(),
                Bytes::copy_from_slice(vec![messages_received.to_le_bytes()[0]].as_slice()),
            );
            let messages_sent_packet = Self::sys_publish(
                messages_sent_topic.clone(),
                Bytes::copy_from_slice(vec![messages_sent.to_le_bytes()[0]].as_slice()),
            );
            let messages_dropped_packet = Self::sys_publish(
                messages_dropped_topic.clone(),
                Bytes::copy_from_slice(vec![messages_dropped.to_le_bytes()[0]].as_slice()),
            );
            let subscriptions_count_packet = Self::sys_publish(
                subscriptions_count_topic.clone(),
                Bytes::copy_from_slice(vec![subscriptions_count.to_le_bytes()[0]].as_slice()),
            );

            let router_actor_addr = self
                .router_actor
                .as_ref()
                .expect("Router actor not initialized")
                .clone();

            let packets = vec![
                clients_connected_packet,
                bytes_received_packet,
                bytes_sent_packet,
                uptime_packet,
                packets_received_packet,
                packets_sent_packet,
                messages_received_packet,
                messages_sent_packet,
                messages_dropped_packet,
                subscriptions_count_packet,
            ];

            for packet in packets {
                router_actor_addr.do_send(router_actor::RoutePacketToAllTenants { packet });
            }
        }
    }
}
