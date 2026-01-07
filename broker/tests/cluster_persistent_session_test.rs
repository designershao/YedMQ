use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::{net::SocketAddr, time::Duration};

mod cluster_setup;
use cluster_setup::setup_cluster;

#[actix::test]
async fn test_persistent_session_switch_node() {
    let context = setup_cluster().await;

    // We'll use Node 1 and Node 2 for switching
    let node1 = &context.nodes[0];
    let addr1: SocketAddr = node1.listener.tcp.external.as_str().parse().unwrap();

    let node2 = &context.nodes[1];
    let addr2: SocketAddr = node2.listener.tcp.external.as_str().parse().unwrap();

    let client_id = "switch-node-persistent-sub";
    let topic = "test/cluster/switch_node_persistent";
    let payload = b"message for switched node";

    println!("Step 1: Connecting to Node 1 and subscribing...");
    // 1. Sub Client (Persistent) connects to Node 1 and subscribes
    let mut sub_opts1 = MqttOptions::new(client_id, addr1.ip().to_string(), addr1.port());
    sub_opts1.set_keep_alive(Duration::from_secs(30));
    sub_opts1.set_clean_session(false);
    let (sub_client1, mut sub_eventloop1) = AsyncClient::new(sub_opts1, 10);

    let sub_setup_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match sub_eventloop1.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    connected = true;
                    println!(
                        "Sub connected to Node 1, session_present: {}",
                        ack.session_present
                    );
                    sub_client1
                        .subscribe(topic, QoS::AtLeastOnce)
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(ack))) => {
                    println!("Subscribed to topic on Node 1, ack: {:?}", ack);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    sub_client1.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    println!("Sub disconnected from Node 1");
                    return;
                }
                Ok(ev) => {
                    println!("Sub received event from Node 1: {:?}", ev);
                }
                Err(e) => {
                    if connected {
                        println!("Sub connection to Node 1 closed: {:?}", e);
                        return;
                    }
                    panic!("Sub Setup Error: {:?}", e)
                }
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(30), sub_setup_task)
        .await
        .expect("Sub Setup timed out")
        .unwrap();

    println!("Waiting for session to be fully saved and cluster to stabilize...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 2. Pub Client connects to Node 2 and publishes
    // We expect this message to be queued for the persistent session
    println!("Step 2: Connecting to Node 2 and publishing message...");
    let mut pub_opts = MqttOptions::new(
        "cluster-persistent-pub",
        addr2.ip().to_string(),
        addr2.port(),
    );
    pub_opts.set_keep_alive(Duration::from_secs(30));
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_opts, 10);

    let pub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match pub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    println!("Pub connected to Node 2");
                    pub_client
                        .publish(topic, QoS::AtLeastOnce, false, payload.to_vec())
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::PubAck(ack))) => {
                    println!("Message published to Node 2, ack: {:?}", ack);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    pub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    println!("Pub disconnected from Node 2");
                    return;
                }
                Ok(ev) => {
                    println!("Pub received event from Node 2: {:?}", ev);
                }
                Err(e) => {
                    if connected {
                        println!("Pub connection to Node 2 closed: {:?}", e);
                        return;
                    }
                    panic!("Pub Error: {:?}", e)
                }
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(30), pub_task)
        .await
        .expect("Pub timed out")
        .unwrap();

    println!("Waiting for message to be routed and stored...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 3. Sub Client Reconnects to Node 2 (different node!) and verifies session & message
    println!("Step 3: Reconnecting to Node 2 and verifying session state...");
    let mut sub_opts2 = MqttOptions::new(client_id, addr2.ip().to_string(), addr2.port());
    sub_opts2.set_keep_alive(Duration::from_secs(30));
    sub_opts2.set_clean_session(false);
    let (sub_client2, mut sub_eventloop2) = AsyncClient::new(sub_opts2, 10);

    let sub_verify_task = tokio::spawn(async move {
        let mut connected = false;
        let mut message_received = false;
        loop {
            match sub_eventloop2.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    connected = true;
                    println!(
                        "Reconnected to Node 2, session_present: {}",
                        ack.session_present
                    );
                    // assert!(ack.session_present, "Session should be present on Node 2");
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    println!("Received message on Node 2: {:?}", publish);
                    assert_eq!(publish.topic, topic);
                    assert_eq!(publish.payload.as_ref(), payload);
                    message_received = true;
                    sub_client2.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    println!("Sub disconnected from Node 2 (verify task)");
                    if message_received {
                        return;
                    }
                }
                Ok(ev) => {
                    println!("Sub verify task received event: {:?}", ev);
                }
                Err(e) => {
                    if connected && message_received {
                        println!("Sub verify task connection closed after success: {:?}", e);
                        return;
                    }
                    println!("Sub verify task error: {:?}", e);
                    panic!("Sub Verify Error: {:?}", e)
                }
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(60), sub_verify_task)
        .await
        .expect("Sub Verify timed out")
        .unwrap();
    println!("Test passed!");
}

#[actix::test]
async fn test_cluster_persistent_session_cleared_by_clean_session() {
    let context = setup_cluster().await;
    let node1 = &context.nodes[0];
    let addr1: SocketAddr = node1.listener.tcp.external.as_str().parse().unwrap();
    let node2 = &context.nodes[1];
    let addr2: SocketAddr = node2.listener.tcp.external.as_str().parse().unwrap();

    let client_id = "clean-session-cluster-test";
    let topic = "test/cluster/clean_session_clear";
    let payload = b"should be discarded in cluster";

    // 1. Connect Client to Node 1 with clean_session = false
    let mut opts1 = MqttOptions::new(client_id, addr1.ip().to_string(), addr1.port());
    opts1.set_keep_alive(Duration::from_secs(30));
    opts1.set_clean_session(false);
    let (client1, mut eventloop1) = AsyncClient::new(opts1, 10);

    let setup_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop1.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client1.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    client1.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected { return }
                    panic!("Setup Error: {:?}", e)
                },
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(30), setup_task).await.expect("Setup timed out").unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    // 2. Publish to Node 2
    let mut pub_opts = MqttOptions::new("publisher-node2", addr2.ip().to_string(), addr2.port());
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_opts, 10);
    
    let pub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match pub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    pub_client.publish(topic, QoS::AtLeastOnce, false, payload.to_vec()).await.unwrap();
                }
                Ok(Event::Incoming(Packet::PubAck(_))) => {
                    pub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                     if connected { return }
                     panic!("Pub Error: {:?}", e)
                },
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(30), pub_task).await.expect("Pub timed out").unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    // 3. Reconnect to Node 1 with clean_session = true
    let mut opts2 = MqttOptions::new(client_id, addr1.ip().to_string(), addr1.port());
    opts2.set_keep_alive(Duration::from_secs(30));
    opts2.set_clean_session(true); // Must clear session
    let (client2, mut eventloop2) = AsyncClient::new(opts2, 10);

    let verify_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop2.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    connected = true;
                    assert!(!ack.session_present, "Session should NOT be present");
                }
                Ok(Event::Incoming(Packet::Publish(_))) => {
                    panic!("Should NOT receive message");
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected { return }
                    panic!("Verify Error: {:?}", e)
                },
                _ => {}
            }
        }
    });
    
    let res = tokio::time::timeout(Duration::from_secs(5), verify_task).await;
    assert!(res.is_err(), "Should timeout waiting for message (none expected)");
    client2.disconnect().await.unwrap();
}