use std::{sync::{atomic::AtomicU64, Arc}, time::{Duration, Instant}};

use log::warn;
use yedmq_mqtt::v3::publish::PublishPacketBuilder;

pub struct Metric {

    pub clients_connected: AtomicU64,

    pub bytes_received: AtomicU64,

    pub bytes_sent: AtomicU64,

    pub start_time: Instant,
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

pub struct SysTopicTask{

    metric: Arc<Metric>,

    interval_secs: u64,

}


impl SysTopicTask {

    pub fn new(metric: Arc<Metric>, interval_secs: u64) -> SysTopicTask {
        SysTopicTask { metric, interval_secs }
    }

    pub async fn run(&self) {

        let clients_connected_topic = "$SYS/broker/clients/connected".to_string();
        let broker_bytes_sent_topic = "$SYS/broker/bytes/sent".to_string();
        let broker_bytes_received_topic = "$SYS/broker/bytes/received".to_string();
        let broker_uptime_topic = "$SYS/broker/uptime".to_string();

        let mut sys_topic_interval = tokio::time::interval(Duration::from_secs(self.interval_secs));
        let metric = self.metric.clone();

        loop {

            sys_topic_interval.tick().await;
            let clients_connected = metric.clients_connected.load(std::sync::atomic::Ordering::SeqCst);
            let bytes_received = metric.bytes_received.load(std::sync::atomic::Ordering::SeqCst);
            let bytes_sent = metric.bytes_sent.load(std::sync::atomic::Ordering::SeqCst);

            let clients_connected_packet = PublishPacketBuilder::new(clients_connected_topic.clone(), vec![clients_connected.to_le_bytes()[0]]).build();
            let bytes_received_packet = PublishPacketBuilder::new(broker_bytes_received_topic.clone(), vec![bytes_received.to_le_bytes()[0]]).build();
            let bytes_sent_packet = PublishPacketBuilder::new(broker_bytes_sent_topic.clone(), vec![bytes_sent.to_le_bytes()[0]]).build();
            let uptime_packet = PublishPacketBuilder::new(broker_uptime_topic.clone(), vec![metric.get_uptime().to_le_bytes()[0]]).build();

                
            if let Err(e) = self.router_sender.send(RouterCmd::RoutePacketToAllTenants(yedmq_mqtt::MqttPacketV3::Publish(clients_connected_packet))).await {
                warn!("Failed to send packet to all tenants, error: {}", e);
            }
            if let Err(e) = self.router_sender.send(RouterCmd::RoutePacketToAllTenants(yedmq_mqtt::MqttPacketV3::Publish(bytes_received_packet))).await {
                warn!("Failed to send packet to all tenants, error: {}", e);
            }
            if let Err(e) = self.router_sender.send(RouterCmd::RoutePacketToAllTenants(yedmq_mqtt::MqttPacketV3::Publish(bytes_sent_packet))).await {
                warn!("Failed to send packet to all tenants, error: {}", e);
            }
            if let Err(e) = self.router_sender.send(RouterCmd::RoutePacketToAllTenants(yedmq_mqtt::MqttPacketV3::Publish(uptime_packet))).await {
                warn!("Failed to send packet to all tenants, error: {}", e);
            }
        }
    }
}