use rumqttc::tokio_rustls::rustls::{ClientConfig, RootCertStore};
use rumqttc::{AsyncClient, Event, LastWill, MqttOptions, Packet, QoS, Transport};
use rustls_pemfile as pemfile;
use std::io::BufReader;
use std::{env, fs, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream, rustls as direct_rustls, TlsConnector};
use tokio_tungstenite::{client_async, tungstenite::client::IntoClientRequest, WebSocketStream};

mod cluster_setup;
#[allow(dead_code)]
mod common;
use cluster_setup::setup_cluster;

fn configure_wss() -> Transport {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let certs_path = PathBuf::from(crate_root_path).join("tests").join("certs");
    let ca_file_path = certs_path.join("ca.crt");

    let mut root_cert_store = RootCertStore::empty();
    let ca_file = fs::File::open(ca_file_path).expect("Failed to open CA file");
    let mut reader = BufReader::new(ca_file);
    for cert_result in pemfile::certs(&mut reader) {
        root_cert_store.add(cert_result.unwrap()).unwrap();
    }

    let client_config = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    Transport::wss_with_config(client_config.into())
}

fn raw_wss_client_config() -> Arc<direct_rustls::ClientConfig> {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let certs_path = PathBuf::from(crate_root_path).join("tests").join("certs");
    let ca_file_path = certs_path.join("ca.crt");

    let mut root_cert_store = direct_rustls::RootCertStore::empty();
    let ca_file = fs::File::open(ca_file_path).expect("Failed to open CA file");
    let mut reader = BufReader::new(ca_file);
    for cert_result in pemfile::certs(&mut reader) {
        root_cert_store.add(cert_result.unwrap()).unwrap();
    }

    Arc::new(
        direct_rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth(),
    )
}

async fn connect_mqtt5_wss_client(
    broker_addr: SocketAddr,
    client_id: &str,
) -> WebSocketStream<TlsStream<TcpStream>> {
    let stream = TcpStream::connect(("127.0.0.1", broker_addr.port()))
        .await
        .unwrap();
    let connector = TlsConnector::from(raw_wss_client_config());
    let server_name = direct_rustls::pki_types::ServerName::try_from("localhost")
        .unwrap()
        .to_owned();
    let stream = connector.connect(server_name, stream).await.unwrap();
    let url = format!("wss://localhost:{}/mqtt", broker_addr.port());
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());
    let (mut stream, _) = client_async(request, stream).await.unwrap();

    common::mqtt5::write_websocket_packet(
        &mut stream,
        common::mqtt5::connect_packet(client_id, true, 5),
    )
    .await;

    let connack = common::mqtt5::read_websocket_packet(&mut stream).await;
    let connack = common::mqtt5::parse_connack(&connack).expect("parse MQTT 5 CONNACK");
    assert_eq!(connack.reason_code, 0x00);
    stream
}

#[actix::test]
pub async fn test_wss_listener_connect() {
    let context = setup_cluster().await;

    let settings = &context.nodes[0];
    let broker_addr: SocketAddr = settings.listener.wss.external.as_str().parse().unwrap();

    let mut options = MqttOptions::new(
        "test_cluster_wss_connect",
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    options.set_keep_alive(Duration::from_secs(5));
    options.set_transport(configure_wss());

    let (client, mut eventloop) = AsyncClient::new(options, 10);

    let connection_handle = tokio::spawn(async move {
        let mut _connected = false;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    _connected = true;
                    client.disconnect().await.unwrap();
                    return Ok(ack.code);
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    return Ok(rumqttc::ConnectReturnCode::Success);
                }
                Ok(_) => continue,
                Err(e) => {
                    if _connected {
                        break;
                    }
                    return Err(e);
                }
            }
        }
        Ok(rumqttc::ConnectReturnCode::Success)
    });

    let result = tokio::time::timeout(Duration::from_secs(15), connection_handle).await;

    assert!(result.is_ok());
    let connect_result = result.unwrap().unwrap();
    assert!(connect_result.is_ok());
    assert_eq!(connect_result.unwrap(), rumqttc::ConnectReturnCode::Success);
}

#[actix::test]
pub async fn test_wss_listener_mqtt5_publish_subscribe_cross_node_smoke() {
    let context = setup_cluster().await;

    let pub_addr: SocketAddr = context.nodes[0]
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();
    let sub_addr: SocketAddr = context.nodes[1]
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();

    let suffix = uuid::Uuid::new_v4();
    let topic = format!("test/cluster/mqtt5/wss/{suffix}");
    let payload = b"hello cluster mqtt5 wss";

    let mut subscriber = connect_mqtt5_wss_client(sub_addr, "cluster-mqtt5-wss-sub").await;
    common::mqtt5::write_websocket_packet(
        &mut subscriber,
        common::mqtt5::subscribe_packet(1, &topic, 1),
    )
    .await;
    let suback = common::mqtt5::read_websocket_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.reason_codes, vec![0x01]);

    let mut publisher = connect_mqtt5_wss_client(pub_addr, "cluster-mqtt5-wss-pub").await;
    common::mqtt5::write_websocket_packet(
        &mut publisher,
        common::mqtt5::publish_packet(&topic, payload, 1, false, Some(7)),
    )
    .await;
    let puback = common::mqtt5::read_websocket_packet(&mut publisher).await;
    let puback = common::mqtt5::parse_puback(&puback).expect("parse MQTT 5 PUBACK");
    assert_eq!(puback.packet_id, 7);
    assert_eq!(puback.reason_code, 0x00);

    let publish = common::mqtt5::read_websocket_packet(&mut subscriber).await;
    let publish = common::mqtt5::parse_publish(&publish).expect("parse MQTT 5 PUBLISH");
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert_eq!(publish.qos, 1);
    let subscriber_packet_id = publish.packet_id.expect("QoS 1 packet id");
    common::mqtt5::write_websocket_packet(
        &mut subscriber,
        common::mqtt5::puback_packet(subscriber_packet_id),
    )
    .await;

    common::mqtt5::write_websocket_packet(&mut subscriber, common::mqtt5::disconnect_packet())
        .await;
    common::mqtt5::write_websocket_packet(&mut publisher, common::mqtt5::disconnect_packet()).await;
}

async fn test_wss_publish_subscribe_cross_node(qos: QoS) {
    let context = setup_cluster().await;

    // Publisher -> Node 1
    let pub_node = &context.nodes[0];
    let pub_addr: SocketAddr = pub_node.listener.wss.external.as_str().parse().unwrap();

    // Subscriber -> Node 2
    let sub_node = &context.nodes[1];
    let sub_addr: SocketAddr = sub_node.listener.wss.external.as_str().parse().unwrap();

    let topic = format!("test/cluster/wss/{:?}", qos);
    let payload = format!("hello cluster wss {:?}", qos).into_bytes();

    // Subscriber Setup
    let mut sub_opts = MqttOptions::new(
        format!("sub-wss-{:?}", qos),
        format!("wss://localhost:{}/mqtt", sub_addr.port()),
        sub_addr.port(),
    );
    sub_opts.set_keep_alive(Duration::from_secs(5));
    sub_opts.set_transport(configure_wss());
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 10);

    let payload_clone = payload.clone();
    let topic_clone = topic.clone();

    let sub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match sub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    sub_client
                        .subscribe(topic_clone.clone(), qos)
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    // Ready
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, topic_clone);
                    assert_eq!(publish.payload.as_ref(), payload_clone.as_slice());
                    assert_eq!(publish.qos, qos);
                    sub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Sub Eventloop error: {:?}", e);
                }
                _ => {}
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publisher Setup
    let mut pub_opts = MqttOptions::new(
        format!("pub-wss-{:?}", qos),
        format!("wss://localhost:{}/mqtt", pub_addr.port()),
        pub_addr.port(),
    );
    pub_opts.set_keep_alive(Duration::from_secs(5));
    pub_opts.set_transport(configure_wss());
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_opts, 10);

    let _pub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match pub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    pub_client
                        .publish(topic.clone(), qos, false, payload.clone())
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::PubAck(_)))
                | Ok(Event::Incoming(Packet::PubComp(_))) => {
                    pub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Pub Eventloop error: {:?}", e);
                }
                _ => {}
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(10), sub_task)
        .await
        .expect("Sub task timed out")
        .unwrap();
}

#[actix::test]
async fn test_wss_publish_subscribe_qos0() {
    test_wss_publish_subscribe_cross_node(QoS::AtMostOnce).await;
}

#[actix::test]
async fn test_wss_publish_subscribe_qos1() {
    test_wss_publish_subscribe_cross_node(QoS::AtLeastOnce).await;
}

#[actix::test]
async fn test_wss_publish_subscribe_qos2() {
    test_wss_publish_subscribe_cross_node(QoS::ExactlyOnce).await;
}

#[actix::test]
async fn test_wss_retained_message_cross_node() {
    let context = setup_cluster().await;
    let pub_node = &context.nodes[0];
    let sub_node = &context.nodes[1];

    let pub_addr: SocketAddr = pub_node.listener.wss.external.as_str().parse().unwrap();
    let sub_addr: SocketAddr = sub_node.listener.wss.external.as_str().parse().unwrap();

    let topic = "test/cluster/wss/retained";
    let payload = b"cluster wss retained message";

    // Client 1 publishes retained to Node 1
    let mut mqtt_options1 = MqttOptions::new(
        "retained-wss-pub",
        format!("wss://localhost:{}/mqtt", pub_addr.port()),
        pub_addr.port(),
    );
    mqtt_options1.set_keep_alive(Duration::from_secs(5));
    mqtt_options1.set_transport(configure_wss());
    let (client1, mut eventloop1) = AsyncClient::new(mqtt_options1, 10);

    let task1 = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop1.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client1
                        .publish(topic, QoS::AtLeastOnce, true, payload.to_vec())
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::PubAck(_))) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    client1.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task1)
        .await
        .expect("Pub task timed out")
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client 2 connects to Node 2 and subscribes
    let mut mqtt_options2 = MqttOptions::new(
        "retained-wss-sub",
        format!("wss://localhost:{}/mqtt", sub_addr.port()),
        sub_addr.port(),
    );
    mqtt_options2.set_keep_alive(Duration::from_secs(5));
    mqtt_options2.set_transport(configure_wss());
    let (client2, mut eventloop2) = AsyncClient::new(mqtt_options2, 10);

    let task2 = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop2.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client2.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, topic);
                    assert_eq!(publish.payload.as_ref(), payload);
                    assert!(publish.retain);
                    client2.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task2)
        .await
        .expect("Sub task timed out")
        .unwrap();
}

#[actix::test]
async fn test_wss_last_will_message_cross_node() {
    let context = setup_cluster().await;

    let node1 = &context.nodes[0];
    let addr1: SocketAddr = node1.listener.wss.external.as_str().parse().unwrap();

    let node2 = &context.nodes[1];
    let addr2: SocketAddr = node2.listener.wss.external.as_str().parse().unwrap();

    let will_topic = "test/cluster/wss/will";
    let will_message = b"cluster wss unclean disconnect";
    let last_will = LastWill::new(will_topic, will_message, QoS::AtLeastOnce, false);

    // Subscriber
    let mut sub_opts = MqttOptions::new(
        "will-wss-sub",
        format!("wss://localhost:{}/mqtt", addr2.port()),
        addr2.port(),
    );
    sub_opts.set_keep_alive(Duration::from_secs(5));
    sub_opts.set_transport(configure_wss());
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 10);

    let (notify_sub_succeed_sender, mut notify_sub_succeed_receiver) =
        tokio::sync::broadcast::channel(1);

    let sub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match sub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    sub_client
                        .subscribe(will_topic, QoS::AtLeastOnce)
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    notify_sub_succeed_sender.send(()).unwrap();
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, will_topic);
                    assert_eq!(publish.payload.as_ref(), will_message);
                    sub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Sub Error: {:?}", e)
                }
                _ => {}
            }
        }
    });

    notify_sub_succeed_receiver.recv().await.unwrap();

    // Will Client
    let mut will_opts = MqttOptions::new(
        "will-wss-client",
        format!("wss://localhost:{}/mqtt", addr1.port()),
        addr1.port(),
    );
    will_opts
        .set_keep_alive(Duration::from_secs(2))
        .set_last_will(last_will);
    will_opts.set_transport(configure_wss());
    let (_will_client, mut will_eventloop) = AsyncClient::new(will_opts, 10);

    let will_task = tokio::spawn(async move {
        loop {
            match will_eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                    // Drop the client without sending MQTT DISCONNECT to trigger the will.
                    return;
                }
                Ok(_) => {} // Continue processing other initialization events (such as SubAck, etc.)
                Err(_e) => {
                    return;
                }
            }
        }
    });

    // Wait for the will client to connect
    tokio::time::timeout(Duration::from_secs(10), will_task)
        .await
        .expect("Will client task timed out")
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), sub_task)
        .await
        .expect("Sub task timed out")
        .unwrap();
}

#[actix::test]
async fn test_persistent_session_cross_node() {
    let context = setup_cluster().await;

    // Node 1 (Pub)
    let pub_node = &context.nodes[0];
    let pub_addr: SocketAddr = pub_node.listener.wss.external.as_str().parse().unwrap();

    // Node 2 (Sub, persistent)
    let sub_node = &context.nodes[1];
    let sub_addr: SocketAddr = sub_node.listener.wss.external.as_str().parse().unwrap();

    let topic = "test/cluster/wss/persistent";
    let payload = b"cluster wss persistent message";

    // Sub Client (Persistent)
    let mut sub_opts = MqttOptions::new(
        "cluster-persistent-wss-sub",
        format!("wss://localhost:{}/mqtt", sub_addr.port()),
        sub_addr.port(),
    );
    sub_opts.set_keep_alive(Duration::from_secs(5));
    sub_opts.set_transport(configure_wss());
    sub_opts.set_clean_session(false);
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_opts, 10);

    let sub_setup_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match sub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    sub_client.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    sub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Sub Setup Error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), sub_setup_task)
        .await
        .expect("Sub Setup timed out")
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Pub Client
    let mut pub_opts = MqttOptions::new(
        "cluster-persistent-wss-pub",
        format!("wss://localhost:{}/mqtt", pub_addr.port()),
        pub_addr.port(),
    );
    pub_opts.set_keep_alive(Duration::from_secs(5));
    pub_opts.set_transport(configure_wss());
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_opts, 10);

    let pub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match pub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    pub_client
                        .publish(topic, QoS::AtLeastOnce, false, payload.to_vec())
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::PubAck(_))) => {
                    pub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Pub Error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), pub_task)
        .await
        .expect("Pub timed out")
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Sub Client Reconnect
    let mut sub_opts2 = MqttOptions::new(
        "cluster-persistent-wss-sub",
        format!("wss://localhost:{}/mqtt", sub_addr.port()),
        sub_addr.port(),
    );
    sub_opts2.set_keep_alive(Duration::from_secs(5));
    sub_opts2.set_transport(configure_wss());
    sub_opts2.set_clean_session(false);
    let (sub_client2, mut sub_eventloop2) = AsyncClient::new(sub_opts2, 10);

    let sub_verify_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match sub_eventloop2.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    connected = true;
                    assert!(ack.session_present);
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, topic);
                    assert_eq!(publish.payload.as_ref(), payload);
                    sub_client2.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Sub Verify Error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), sub_verify_task)
        .await
        .expect("Sub Verify timed out")
        .unwrap();
}
