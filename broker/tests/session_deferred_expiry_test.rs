use bytes::Bytes;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use yedmq::{
    protobuf::{
        cluster_service_client::ClusterServiceClient, GetSessionStateRequest, RoutePacketRequest,
    },
    session::session_state_storage::SessionState,
    stored_packet::serialize_stored_packet_to_string,
};
use yedmq_mqtt::{packet::Packet, v3::publish::PublishPacketBuilder, MqttPacketV3};

mod cluster_setup;
mod common;

async fn connect(addr: &str, clean: bool) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut properties = common::mqtt5::session_expiry_interval_property(60);
    properties.extend(common::mqtt5::receive_maximum_property(1));
    stream
        .write_all(&common::mqtt5::connect_packet_with_properties(
            "expiry-subscriber",
            clean,
            60,
            &properties,
        ))
        .await
        .unwrap();
    let reply = common::mqtt5::read_packet_from_reader(&mut stream).await;
    let connack = common::mqtt5::parse_connack(&reply).unwrap();
    assert_eq!(connack.reason_code, 0);
    assert_eq!(connack.session_present, !clean);
    stream
}

async fn state(client: &mut ClusterServiceClient<tonic::transport::Channel>) -> SessionState {
    let reply = client
        .get_session_state(GetSessionStateRequest {
            tenant_id: "public".into(),
            client_id: "expiry-subscriber".into(),
        })
        .await
        .unwrap()
        .into_inner();
    serde_json::from_str::<Option<SessionState>>(&reply.payload.unwrap())
        .unwrap()
        .unwrap()
}

async fn wait_for_count(
    client: &mut ClusterServiceClient<tonic::transport::Channel>,
    count: usize,
) -> SessionState {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = state(client).await;
            if state.inflight.inner.len() == count {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("inflight count did not converge")
}

async fn route(
    client: &mut ClusterServiceClient<tonic::transport::Channel>,
    id: u16,
    expiry: Option<u64>,
) {
    let mut packet = Packet::from(MqttPacketV3::Publish(
        PublishPacketBuilder::new("expiry/topic".into(), Bytes::from(vec![id as u8]))
            .qos(1)
            .packet_identifier(id)
            .build(),
    ));
    if let Packet::Publish(publish) = &mut packet {
        publish.expires_at_unix_secs = expiry;
    }
    client
        .route_packet(RoutePacketRequest {
            tenant_id: "public".into(),
            payload: serialize_stored_packet_to_string(&packet).unwrap(),
            route_id: uuid::Uuid::new_v4().to_string(),
            source_node_id: 999,
            target_client_id: "expiry-subscriber".into(),
            target_qos: 1,
            expiry_at: 0,
        })
        .await
        .unwrap();
}

async fn expired_deferred_is_removed(reconnect: bool) {
    let cluster = cluster_setup::setup_cluster().await;
    let mut subscriber = connect(&cluster.nodes[0].listener.tcp.external, true).await;
    let mut rpc =
        ClusterServiceClient::connect(format!("http://{}", cluster.nodes[0].cluster.rpc.external))
            .await
            .unwrap();
    route(&mut rpc, 1, None).await;
    let first = common::mqtt5::read_packet_from_reader(&mut subscriber).await;
    let first = common::mqtt5::parse_publish(&first).unwrap();
    let expires_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3;
    route(&mut rpc, 2, Some(expires_at)).await;
    let before = wait_for_count(&mut rpc, 2).await;
    let deferred = before
        .inflight
        .inner
        .values()
        .find(|item| !item.delivery_started)
        .expect("pending first-send state must be persisted")
        .clone();
    if reconnect {
        subscriber
            .write_all(&common::mqtt5::disconnect_packet())
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(4)).await;
    if reconnect {
        subscriber = connect(&cluster.nodes[1].listener.tcp.external, false).await;
        rpc = ClusterServiceClient::connect(format!(
            "http://{}",
            cluster.nodes[1].cluster.rpc.external
        ))
        .await
        .unwrap();
        let retry = common::mqtt5::read_packet_from_reader(&mut subscriber).await;
        assert_eq!(
            common::mqtt5::parse_publish(&retry).unwrap().payload,
            first.payload
        );
    }
    subscriber
        .write_all(&common::mqtt5::puback_packet(first.packet_id.unwrap()))
        .await
        .unwrap();
    let after = wait_for_count(&mut rpc, 0).await;
    assert!(!after
        .inflight
        .inner
        .contains_key(&deferred.packet_identifier));
    // A new delivery still succeeds after cancellation and does not replay expired data.
    route(&mut rpc, 3, None).await;
    let packet = tokio::time::timeout(
        Duration::from_secs(5),
        common::mqtt5::read_packet_from_reader(&mut subscriber),
    )
    .await
    .unwrap();
    let publish = common::mqtt5::parse_publish(&packet).unwrap();
    assert_eq!(publish.payload, vec![3]);
    subscriber
        .write_all(&common::mqtt5::puback_packet(publish.packet_id.unwrap()))
        .await
        .unwrap();
}

#[actix::test]
async fn expiry_cancels_persistent_deferred_delivery_on_drain() {
    expired_deferred_is_removed(false).await;
}

#[actix::test]
async fn expiry_cancels_persistent_deferred_delivery_after_cross_node_reconnect() {
    expired_deferred_is_removed(true).await;
}
