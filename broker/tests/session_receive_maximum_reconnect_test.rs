use std::{net::SocketAddr, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpStream};

mod cluster_setup;
mod common;

async fn connect(
    addr: SocketAddr,
    client_id: &str,
    clean_start: bool,
    properties: &[u8],
) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&common::mqtt5::connect_packet_with_properties(
            client_id,
            clean_start,
            60,
            properties,
        ))
        .await
        .unwrap();
    let packet = common::mqtt5::read_packet_from_reader(&mut stream).await;
    let connack = common::mqtt5::parse_connack(&packet).unwrap();
    assert_eq!(connack.reason_code, 0);
    assert_eq!(connack.session_present, !clean_start);
    stream
}

async fn acknowledge_publish(stream: &mut TcpStream, packet_id: u16, qos: u8) {
    if qos == 1 {
        stream
            .write_all(&common::mqtt5::puback_packet(packet_id))
            .await
            .unwrap();
    } else {
        stream
            .write_all(&common::mqtt5::pubrec_packet(packet_id))
            .await
            .unwrap();
        let packet = common::mqtt5::read_packet_from_reader(stream).await;
        let pubrel = common::mqtt5::parse_pubrel(&packet).unwrap();
        assert_eq!(pubrel.packet_id, packet_id);
        stream
            .write_all(&common::mqtt5::pubcomp_packet(packet_id))
            .await
            .unwrap();
    }
}

async fn deferred_publish_survives_reconnect(qos: u8, target_node: usize) {
    let context = cluster_setup::setup_cluster().await;
    let addr: SocketAddr = context.nodes[0].listener.tcp.external.parse().unwrap();
    let reconnect_addr: SocketAddr = context.nodes[target_node]
        .listener
        .tcp
        .external
        .parse()
        .unwrap();
    let mut properties = common::mqtt5::session_expiry_interval_property(60);
    properties.extend(common::mqtt5::receive_maximum_property(1));
    let mut subscriber = connect(addr, "deferred-subscriber", true, &properties).await;
    subscriber
        .write_all(&common::mqtt5::subscribe_packet(1, "deferred/topic", qos))
        .await
        .unwrap();
    let suback = common::mqtt5::read_packet_from_reader(&mut subscriber).await;
    assert_eq!(
        common::mqtt5::parse_suback(&suback).unwrap().reason_codes,
        vec![qos]
    );
    let mut publisher = connect(addr, "deferred-publisher", true, &[]).await;
    for (packet_id, payload) in [(2, b"first".as_slice()), (3, b"second".as_slice())] {
        publisher
            .write_all(&common::mqtt5::publish_packet(
                "deferred/topic",
                payload,
                qos,
                false,
                Some(packet_id),
            ))
            .await
            .unwrap();
        let ack = common::mqtt5::read_packet_from_reader(&mut publisher).await;
        if qos == 1 {
            assert_eq!(
                common::mqtt5::parse_puback(&ack).unwrap().packet_id,
                packet_id
            );
        } else {
            assert_eq!(
                common::mqtt5::parse_pubrec(&ack).unwrap().packet_id,
                packet_id
            );
            publisher
                .write_all(&common::mqtt5::pubrel_packet(packet_id))
                .await
                .unwrap();
            let ack = common::mqtt5::read_packet_from_reader(&mut publisher).await;
            assert_eq!(
                common::mqtt5::parse_pubcomp(&ack).unwrap().packet_id,
                packet_id
            );
        }
    }
    let first = common::mqtt5::read_packet_from_reader(&mut subscriber).await;
    let first = common::mqtt5::parse_publish(&first).unwrap();
    assert_eq!(first.payload, b"first");
    assert!(tokio::time::timeout(
        Duration::from_millis(500),
        common::mqtt5::read_packet_from_reader(&mut subscriber),
    )
    .await
    .is_err());

    if target_node == 0 {
        subscriber
            .write_all(&common::mqtt5::disconnect_packet())
            .await
            .unwrap();
        drop(subscriber);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let mut resumed = connect(reconnect_addr, "deferred-subscriber", false, &properties).await;
    let retry = common::mqtt5::read_packet_from_reader(&mut resumed).await;
    let retry = common::mqtt5::parse_publish(&retry).unwrap();
    assert_eq!(retry.payload, b"first");
    assert_eq!(retry.packet_id, first.packet_id);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            common::mqtt5::read_packet_from_reader(&mut resumed),
        )
        .await
        .is_err(),
        "recovered publishes exceeded Receive Maximum"
    );
    acknowledge_publish(&mut resumed, retry.packet_id.unwrap(), qos).await;
    let second = tokio::time::timeout(
        Duration::from_secs(3),
        common::mqtt5::read_packet_from_reader(&mut resumed),
    )
    .await
    .expect("deferred publish was lost when the session actor was replaced");
    let second = common::mqtt5::parse_publish(&second).unwrap();
    assert_eq!(second.payload, b"second");
    assert_eq!(second.qos, qos);
    acknowledge_publish(&mut resumed, second.packet_id.unwrap(), qos).await;
}

#[actix::test]
async fn mqtt5_deferred_qos1_survives_same_node_reconnect() {
    deferred_publish_survives_reconnect(1, 0).await;
}

#[actix::test]
async fn mqtt5_deferred_qos2_survives_same_node_reconnect() {
    deferred_publish_survives_reconnect(2, 0).await;
}

#[actix::test]
async fn mqtt5_deferred_qos1_survives_cross_node_takeover() {
    deferred_publish_survives_reconnect(1, 1).await;
}

#[actix::test]
async fn mqtt5_deferred_qos2_survives_cross_node_takeover() {
    deferred_publish_survives_reconnect(2, 1).await;
}
