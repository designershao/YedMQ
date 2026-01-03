use std::{net::SocketAddr, time::Duration};
use rumqttc::{MqttOptions, AsyncClient, Event, Packet, QoS};

mod cluster_setup;
use cluster_setup::setup_cluster;

// Helper to drain event loop until a specific event or timeout
async fn wait_for_connect(eventloop: &mut rumqttc::EventLoop) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => return,
            Ok(_) => continue,
            Err(e) => panic!("Connection failed: {:?}", e),
        }
    }
}

async fn wait_for_disconnect(eventloop: &mut rumqttc::EventLoop) {
    let timeout = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(_) => return, // Connection closed error is also fine
                _ => continue,
            }
        }
    }).await;
    assert!(timeout.is_ok(), "Timed out waiting for disconnect");
}

#[actix::test]
async fn test_persistent_session_takeover() {
    let context = setup_cluster().await;
    let node1 = &context.nodes[0];
    let node2 = &context.nodes[1];
    let addr1: SocketAddr = node1.listener.tcp.external.as_str().parse().unwrap();
    let addr2: SocketAddr = node2.listener.tcp.external.as_str().parse().unwrap();

    let client_id = "takeover-client";

    // 1. Connect Client 1 to Node 1
    let mut opts1 = MqttOptions::new(client_id, addr1.ip().to_string(), addr1.port());
    opts1.set_keep_alive(Duration::from_secs(60));
    opts1.set_clean_session(false);
    let (client1, mut eventloop1) = AsyncClient::new(opts1, 10);
    wait_for_connect(&mut eventloop1).await;
    println!("Client 1 connected to Node 1");

    // 2. Connect Client 2 (Same ID) to Node 2
    let mut opts2 = MqttOptions::new(client_id, addr2.ip().to_string(), addr2.port());
    opts2.set_keep_alive(Duration::from_secs(60));
    opts2.set_clean_session(false);
    let (client2, mut eventloop2) = AsyncClient::new(opts2, 10);
    wait_for_connect(&mut eventloop2).await;
    println!("Client 2 connected to Node 2");

    // 3. Verify Client 1 is disconnected
    wait_for_disconnect(&mut eventloop1).await;
    println!("Client 1 was disconnected as expected");

    let _ = client1.disconnect().await;
    let _ = client2.disconnect().await;
}

#[actix::test]
async fn test_clean_session_reconnect_clears_messages() {
    let context = setup_cluster().await;
    let node1 = &context.nodes[0];
    let node2 = &context.nodes[1];
    let addr1: SocketAddr = node1.listener.tcp.external.as_str().parse().unwrap();
    let addr2: SocketAddr = node2.listener.tcp.external.as_str().parse().unwrap();

    let client_id = "clean-session-check";
    let topic = "test/clean_session";

    // 1. Connect clean=false to Node 1, Sub, Disconnect
    let mut opts1 = MqttOptions::new(client_id, addr1.ip().to_string(), addr1.port());
    opts1.set_clean_session(false);
    let (client1, mut eventloop1) = AsyncClient::new(opts1, 10);
    wait_for_connect(&mut eventloop1).await;
    client1.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    // Wait for SubAck
    loop {
        if let Ok(Event::Incoming(Packet::SubAck(_))) = eventloop1.poll().await { break; }
    }
    client1.disconnect().await.unwrap();
    // Wait for disconnect to propagate
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 2. Publish message to Node 1 (should be stored if session persists)
    let pub_opts = MqttOptions::new("pub-client", addr1.ip().to_string(), addr1.port());
    let (pub_client, mut pub_loop) = AsyncClient::new(pub_opts, 10);
    wait_for_connect(&mut pub_loop).await;
    pub_client.publish(topic, QoS::AtLeastOnce, false, b"offline-msg".to_vec()).await.unwrap();
    loop {
        if let Ok(Event::Incoming(Packet::PubAck(_))) = pub_loop.poll().await { break; }
    }
    pub_client.disconnect().await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 3. Connect clean=true to Node 2
    let mut opts2 = MqttOptions::new(client_id, addr2.ip().to_string(), addr2.port());
    opts2.set_clean_session(true);
    let (client2, mut eventloop2) = AsyncClient::new(opts2, 10);
    
    // Check ConnAck session_present
    loop {
        match eventloop2.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                assert!(!ack.session_present, "Session should not be present for clean=true");
                break;
            }
            Ok(_) => continue,
            Err(e) => panic!("Connection failed: {:?}", e),
        }
    }

    // 4. Verify NO message received (wait 2s)
    let timeout = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(Event::Incoming(Packet::Publish(_))) = eventloop2.poll().await {
                panic!("Should not receive offline message with clean=true");
            }
        }
    }).await;
    
    // Timeout is success here
    assert!(timeout.is_err());
    let _ = client2.disconnect().await;
}

#[actix::test]
async fn test_subscription_accumulation_roaming() {
    let context = setup_cluster().await;
    let node1 = &context.nodes[0];
    let node2 = &context.nodes[1];
    let node3 = &context.nodes[2];
    let addr1: SocketAddr = node1.listener.tcp.external.as_str().parse().unwrap();
    let addr2: SocketAddr = node2.listener.tcp.external.as_str().parse().unwrap();
    let addr3: SocketAddr = node3.listener.tcp.external.as_str().parse().unwrap();

    let client_id = "roaming-sub";
    let topic1 = "test/roam/1";
    let topic2 = "test/roam/2";

    // 1. Connect Node 1, Sub Topic 1, Disconnect
    let mut opts1 = MqttOptions::new(client_id, addr1.ip().to_string(), addr1.port());
    opts1.set_clean_session(false);
    let (client1, mut eventloop1) = AsyncClient::new(opts1, 10);
    wait_for_connect(&mut eventloop1).await;
    client1.subscribe(topic1, QoS::AtLeastOnce).await.unwrap();
    loop { if let Ok(Event::Incoming(Packet::SubAck(_))) = eventloop1.poll().await { break; } }
    client1.disconnect().await.unwrap();

    // 2. Connect Node 2, Sub Topic 2, Disconnect
    let mut opts2 = MqttOptions::new(client_id, addr2.ip().to_string(), addr2.port());
    opts2.set_clean_session(false);
    let (client2, mut eventloop2) = AsyncClient::new(opts2, 10);
    wait_for_connect(&mut eventloop2).await;
    client2.subscribe(topic2, QoS::AtLeastOnce).await.unwrap();
    loop { if let Ok(Event::Incoming(Packet::SubAck(_))) = eventloop2.poll().await { break; } }
    client2.disconnect().await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // 3. Publish to both topics via Node 1
    let pub_opts = MqttOptions::new("publisher", addr1.ip().to_string(), addr1.port());
    let (pub_client, mut pub_loop) = AsyncClient::new(pub_opts, 10);
    wait_for_connect(&mut pub_loop).await;
    pub_client.publish(topic1, QoS::AtLeastOnce, false, b"msg1".to_vec()).await.unwrap();
    pub_client.publish(topic2, QoS::AtLeastOnce, false, b"msg2".to_vec()).await.unwrap();
    // Wait for PubAcks
    let mut acks = 0;
    loop {
        if let Ok(Event::Incoming(Packet::PubAck(_))) = pub_loop.poll().await {
            acks += 1;
            if acks == 2 { break; }
        }
    }
    pub_client.disconnect().await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 4. Connect Node 3, Receive both
    let mut opts3 = MqttOptions::new(client_id, addr3.ip().to_string(), addr3.port());
    opts3.set_clean_session(false);
    let (client3, mut eventloop3) = AsyncClient::new(opts3, 10);
    
    // Expecting 2 messages
    let mut received = 0;
    let timeout = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(Event::Incoming(Packet::Publish(_))) = eventloop3.poll().await {
                received += 1;
                if received == 2 { return; }
            }
        }
    }).await;
    
    assert!(timeout.is_ok(), "Timed out waiting for accumulated messages. Received: {}", received);
    let _ = client3.disconnect().await;
}

#[actix::test]
async fn test_unsubscribe_persistence() {
    let context = setup_cluster().await;
    let node1 = &context.nodes[0];
    let node2 = &context.nodes[1];
    let node3 = &context.nodes[2];
    let addr1: SocketAddr = node1.listener.tcp.external.as_str().parse().unwrap();
    let addr2: SocketAddr = node2.listener.tcp.external.as_str().parse().unwrap();
    let addr3: SocketAddr = node3.listener.tcp.external.as_str().parse().unwrap();

    let client_id = "unsub-test";
    let topic = "test/unsub";

    // 1. Connect Node 1, Sub, Disconnect
    let mut opts1 = MqttOptions::new(client_id, addr1.ip().to_string(), addr1.port());
    opts1.set_clean_session(false);
    let (client1, mut eventloop1) = AsyncClient::new(opts1, 10);
    wait_for_connect(&mut eventloop1).await;
    client1.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    loop { if let Ok(Event::Incoming(Packet::SubAck(_))) = eventloop1.poll().await { break; } }
    client1.disconnect().await.unwrap();

    // 2. Connect Node 2, Unsub, Disconnect
    let mut opts2 = MqttOptions::new(client_id, addr2.ip().to_string(), addr2.port());
    opts2.set_clean_session(false);
    let (client2, mut eventloop2) = AsyncClient::new(opts2, 10);
    wait_for_connect(&mut eventloop2).await;
    client2.unsubscribe(topic).await.unwrap();
    loop { if let Ok(Event::Incoming(Packet::UnsubAck(_))) = eventloop2.poll().await { break; } }
    client2.disconnect().await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // 3. Publish message
    let pub_opts = MqttOptions::new("publisher-unsub", addr1.ip().to_string(), addr1.port());
    let (pub_client, mut pub_loop) = AsyncClient::new(pub_opts, 10);
    wait_for_connect(&mut pub_loop).await;
    pub_client.publish(topic, QoS::AtLeastOnce, false, b"msg".to_vec()).await.unwrap();
    loop { if let Ok(Event::Incoming(Packet::PubAck(_))) = pub_loop.poll().await { break; } }
    pub_client.disconnect().await.unwrap();

    // 4. Connect Node 3, Should NOT receive message
    let mut opts3 = MqttOptions::new(client_id, addr3.ip().to_string(), addr3.port());
    opts3.set_clean_session(false);
    let (client3, mut eventloop3) = AsyncClient::new(opts3, 10);
    
    let timeout = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(Event::Incoming(Packet::Publish(_))) = eventloop3.poll().await {
                panic!("Received message after unsubscribe");
            }
        }
    }).await;
    
    assert!(timeout.is_err()); // Timeout means success (no message)
    let _ = client3.disconnect().await;
}

#[actix::test]
async fn test_offline_message_qos_behavior() {
    let context = setup_cluster().await;

    tokio::time::sleep(Duration::from_secs(20)).await; // ensure cluster start succeed

    let node1 = &context.nodes[0];
    let node2 = &context.nodes[1];
    let addr1: SocketAddr = node1.listener.tcp.external.as_str().parse().unwrap();
    let addr2: SocketAddr = node2.listener.tcp.external.as_str().parse().unwrap();

    let client_id = "qos-behavior";
    let topic_qos0 = "test/qos0";
    let topic_qos1 = "test/qos1";

    // 1. Connect Node 1, Sub both, Disconnect
    let mut opts1 = MqttOptions::new(client_id, addr1.ip().to_string(), addr1.port());
    opts1.set_clean_session(false);
    let (client1, mut eventloop1) = AsyncClient::new(opts1, 10);
    wait_for_connect(&mut eventloop1).await;
    client1.subscribe(topic_qos0, QoS::AtMostOnce).await.unwrap();
    loop { if let Ok(Event::Incoming(Packet::SubAck(_))) = eventloop1.poll().await { break; } }
    client1.subscribe(topic_qos1, QoS::AtLeastOnce).await.unwrap();
    loop { if let Ok(Event::Incoming(Packet::SubAck(_))) = eventloop1.poll().await { break; } }
    client1.disconnect().await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // 2. Publish Qos0 and Qos1
    let pub_opts = MqttOptions::new("pub-qos", addr2.ip().to_string(), addr2.port());
    let (pub_client, mut pub_loop) = AsyncClient::new(pub_opts, 10);
    wait_for_connect(&mut pub_loop).await;
    
    pub_client.publish(topic_qos0, QoS::AtMostOnce, false, b"qos0-msg".to_vec()).await.unwrap();
    // QoS 0 no ack, just wait a bit
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    pub_client.publish(topic_qos1, QoS::AtLeastOnce, false, b"qos1-msg".to_vec()).await.unwrap();
    loop { if let Ok(Event::Incoming(Packet::PubAck(_))) = pub_loop.poll().await { break; } }
    
    pub_client.disconnect().await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 3. Connect Node 2, Should receive ONLY QoS 1
    let mut opts2 = MqttOptions::new(client_id, addr2.ip().to_string(), addr2.port());
    opts2.set_clean_session(false);
    let (client2, mut eventloop2) = AsyncClient::new(opts2, 10);
    
    let mut received_qos0 = false;
    let mut received_qos1 = false;

    let timeout = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(Event::Incoming(Packet::Publish(p))) = eventloop2.poll().await {
                if p.topic == topic_qos0 { received_qos0 = true; }
                if p.topic == topic_qos1 { received_qos1 = true; }
                if received_qos1 { return; } // Found the expected one, wait a bit more for unexpected?
            }
        }
    }).await;
    
    assert!(timeout.is_ok(), "Timed out waiting for QoS 1 message");
    assert!(received_qos1, "Should receive QoS 1 message");
    assert!(!received_qos0, "Should NOT receive QoS 0 offline message");
    
    let _ = client2.disconnect().await;
}

// Session Expiry Test (Depends on setup_cluster having 10s TTL)
#[actix::test]
async fn test_session_expiry() {
    let context = setup_cluster().await;
    let node1 = &context.nodes[0];
    let node2 = &context.nodes[1];
    let addr1: SocketAddr = node1.listener.tcp.external.as_str().parse().unwrap();
    let addr2: SocketAddr = node2.listener.tcp.external.as_str().parse().unwrap();

    let client_id = "expiry-test";
    let topic = "test/expiry";

    // 1. Connect Node 1, Sub, Disconnect
    let mut opts1 = MqttOptions::new(client_id, addr1.ip().to_string(), addr1.port());
    opts1.set_clean_session(false);
    let (client1, mut eventloop1) = AsyncClient::new(opts1, 10);
    wait_for_connect(&mut eventloop1).await;
    client1.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
    loop { if let Ok(Event::Incoming(Packet::SubAck(_))) = eventloop1.poll().await { break; } }
    client1.disconnect().await.unwrap();

    println!("Waiting 20s for session expiry (TTL is 10s)...");
    tokio::time::sleep(Duration::from_secs(20)).await;

    // 2. Connect Node 2, Check session_present
    let mut opts2 = MqttOptions::new(client_id, addr2.ip().to_string(), addr2.port());
    opts2.set_clean_session(false);
    let (client2, mut eventloop2) = AsyncClient::new(opts2, 10);

    loop {
        match eventloop2.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                assert!(!ack.session_present, "Session should have expired");
                break;
            }
            Ok(_) => continue,
            Err(e) => panic!("Connection failed: {:?}", e),
        }
    }
    let _ = client2.disconnect().await;
}
