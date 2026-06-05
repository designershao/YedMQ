#[allow(dead_code)]
mod common;

use rumqttc::tokio_rustls::rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    ClientConfig, RootCertStore,
};
use rumqttc::{AsyncClient, Event, LastWill, MqttOptions, Packet, QoS, Transport};
use rustls_pemfile as pemfile;
use std::io::BufReader;
use std::{
    env, fs,
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tokio_rustls::{client::TlsStream, rustls as direct_rustls, TlsConnector};
use tokio_tungstenite::{client_async, tungstenite::client::IntoClientRequest, WebSocketStream};
use yedmq::app::YedMQApp;
use yedmq::settings::Settings;

static ASYNC_SETUP: OnceCell<TestContext> = OnceCell::const_new();
static ASYNC_SETUP_MTLS: OnceCell<TestContext> = OnceCell::const_new();

struct TestContext {
    _test_dir: TempDir,

    original_dir: PathBuf,

    settings: Arc<Settings>,
}

impl Drop for TestContext {
    fn drop(&mut self) {
        env::set_current_dir(self.original_dir.clone()).unwrap();
    }
}

async fn setup_instance() -> &'static TestContext {
    setup_instance_for(&ASYNC_SETUP, false).await
}

async fn setup_mtls_instance() -> &'static TestContext {
    setup_instance_for(&ASYNC_SETUP_MTLS, true).await
}

async fn setup_instance_for(
    cell: &'static OnceCell<TestContext>,
    verify_client_cert: bool,
) -> &'static TestContext {
    cell.get_or_init(|| async move {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .format_target(false)
            .format_timestamp(None)
            .try_init();

        let original_dir = env::current_dir().unwrap();

        let temp_dir = TempDir::new().unwrap();

        fs::copy("./yedmq.toml", temp_dir.path().join("yedmq.toml")).unwrap();

        env::set_current_dir(temp_dir.path()).unwrap();

        let reserved_ports = ReservedPorts::new();
        let test_settings = Arc::new(get_test_settings(
            2,
            10,
            temp_dir.path(),
            &reserved_ports,
            verify_client_cert,
        ));
        drop(reserved_ports);

        let settings_clone = test_settings.clone();

        std::thread::spawn(move || {
            let rt = actix::System::new();
            rt.block_on(async {
                let app = Arc::new(YedMQApp::new(settings_clone).await.unwrap());

                YedMQApp::start(app.clone()).await;
            });
            rt.run().unwrap();
        });

        tokio::time::sleep(Duration::from_secs(1)).await;

        TestContext {
            settings: test_settings,
            original_dir,
            _test_dir: temp_dir,
        }
    })
    .await
}

struct ReservedPorts {
    tcp: StdTcpListener,
    tcp_tls: StdTcpListener,
    ws: StdTcpListener,
    wss: StdTcpListener,
    rpc: StdTcpListener,
    api: StdTcpListener,
}

impl ReservedPorts {
    fn new() -> Self {
        Self {
            tcp: reserve_tcp_listener(),
            tcp_tls: reserve_tcp_listener(),
            ws: reserve_tcp_listener(),
            wss: reserve_tcp_listener(),
            rpc: reserve_tcp_listener(),
            api: reserve_tcp_listener(),
        }
    }

    fn tcp_port(&self) -> u16 {
        self.tcp.local_addr().unwrap().port()
    }

    fn tcp_tls_port(&self) -> u16 {
        self.tcp_tls.local_addr().unwrap().port()
    }

    fn ws_port(&self) -> u16 {
        self.ws.local_addr().unwrap().port()
    }

    fn wss_port(&self) -> u16 {
        self.wss.local_addr().unwrap().port()
    }

    fn rpc_port(&self) -> u16 {
        self.rpc.local_addr().unwrap().port()
    }

    fn api_port(&self) -> u16 {
        self.api.local_addr().unwrap().port()
    }
}

fn reserve_tcp_listener() -> StdTcpListener {
    StdTcpListener::bind("127.0.0.1:0").expect("failed to reserve tcp port")
}

fn get_test_settings(
    qos_expired_secs: u64,
    resend_duration_sec: u64,
    temp_dir: &Path,
    reserved_ports: &ReservedPorts,
    verify_client_cert: bool,
) -> Settings {
    // Generate a random temporary directory
    fs::create_dir_all(temp_dir).unwrap();
    let test_temp_store_dir = temp_dir.to_str().unwrap().to_string();

    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let plugin_path = PathBuf::from(crate_root_path).join("tests").join("plugins");

    let certs_path = PathBuf::from(crate_root_path).join("tests").join("certs");
    let absolute_certs_path = std::fs::canonicalize(&certs_path).unwrap();

    let settings = Settings {
        session: yedmq::settings::Session {
            qos_expired_secs,
            packet_resend_interval_secs: resend_duration_sec,
            session_clock_path: temp_dir.join("clock").to_str().unwrap().to_string(),
        },
        listener: yedmq::settings::Listener {
            tcp: yedmq::settings::Tcp {
                external: format!("127.0.0.1:{}", reserved_ports.tcp_port()).to_string(),
                rate_limit: Default::default(),
            },
            tcp_tls: yedmq::settings::TcpTls {
                external: format!("127.0.0.1:{}", reserved_ports.tcp_tls_port()).to_string(),
                cacert_file: absolute_certs_path
                    .join("ca.crt")
                    .to_str()
                    .unwrap()
                    .to_string(),
                cert_file: absolute_certs_path
                    .join("server.crt")
                    .to_str()
                    .unwrap()
                    .to_string(),
                key_file: absolute_certs_path
                    .join("server.key")
                    .to_str()
                    .unwrap()
                    .to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            ws: yedmq::settings::Ws {
                external: format!("127.0.0.1:{}", reserved_ports.ws_port()).to_string(),
                rate_limit: Default::default(),
            },
            wss: yedmq::settings::Wss {
                external: format!("127.0.0.1:{}", reserved_ports.wss_port()).to_string(),
                cacert_file: absolute_certs_path
                    .join("ca.crt")
                    .to_str()
                    .unwrap()
                    .to_string(),
                cert_file: absolute_certs_path
                    .join("server.crt")
                    .to_str()
                    .unwrap()
                    .to_string(),
                key_file: absolute_certs_path
                    .join("server.key")
                    .to_str()
                    .unwrap()
                    .to_string(),
                verify_client_cert,
                rate_limit: Default::default(),
            },
            api: yedmq::settings::Api {
                external: format!("127.0.0.1:{}", reserved_ports.api_port()).to_string(),
                auth: yedmq::settings::AuthConfig { users: vec![] },
            },
        },
        plugin: yedmq::settings::Plugin {
            dir: plugin_path.to_str().unwrap().to_string(),
            local_socket_path: format!("{}/yedmq_plugin_host.sock", temp_dir.to_str().unwrap()),
            default_authorize_result: true,
            default_authenticate_result: true,
        },
        mqtt: yedmq::settings::Mqtt {
            sys_topic_interval_secs: 10,
            max_message_size: yedmq_mqtt::MQTT_MAX_MESSAGE_SIZE,
            default_authentication: yedmq::settings::DefaultAuthenticationValue::Allow,
            default_authorization: yedmq::settings::DefaultAuthorizationValue::Allow,
            inflight_retry_interval_secs: 10,
        },
        cluster: yedmq::settings::Cluster {
            node_id: 1001,
            cluster_name: "YedMQTest".to_string(),
            heartbeat_interval: 10,
            store_dir: test_temp_store_dir,
            rpc: yedmq::settings::RPC {
                external: format!("127.0.0.1:{}", reserved_ports.rpc_port()).to_string(),
            },
            nodes: vec![yedmq::settings::Node {
                id: 1001,
                rpc_address: format!("127.0.0.1:{}", reserved_ports.rpc_port()).to_string(),
                api_address: format!("127.0.0.1:{}", reserved_ports.api_port()).to_string(),
            }],
            session_ttl: 10,
            startup_mode: yedmq::settings::ClusterStartupMode::Bootstrap,
        },
    };
    settings
}

fn configure_wss(client_auth: bool) -> Transport {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let certs_path = PathBuf::from(crate_root_path).join("tests").join("certs");
    let ca_file_path = certs_path.join("ca.crt");

    let mut root_cert_store = RootCertStore::empty();
    let ca_file = fs::File::open(ca_file_path).expect("Failed to open CA file");
    let mut reader = BufReader::new(ca_file);
    for cert_result in pemfile::certs(&mut reader) {
        root_cert_store.add(cert_result.unwrap()).unwrap();
    }

    let config_builder = ClientConfig::builder().with_root_certificates(root_cert_store);
    let client_config = if client_auth {
        let certs = CertificateDer::pem_file_iter(certs_path.join("client.crt").to_str().unwrap())
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let key =
            PrivateKeyDer::from_pem_file(certs_path.join("client.key").to_str().unwrap()).unwrap();
        config_builder.with_client_auth_cert(certs, key).unwrap()
    } else {
        config_builder.with_no_client_auth()
    };

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
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let keep_live_duration_secs = 5;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();

    println!("borker wss addr {:?}", broker_addr.to_string());

    let mut options = MqttOptions::new(
        "test_client_for_wss_connect",
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    options.set_keep_alive(std::time::Duration::from_secs(keep_live_duration_secs));
    options.set_transport(configure_wss(false));

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

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), connection_handle).await;

    assert!(result.is_ok());
    let connect_result = result.unwrap().unwrap();
    assert!(connect_result.is_ok());
    assert_eq!(connect_result.unwrap(), rumqttc::ConnectReturnCode::Success);
}

#[actix::test]
pub async fn test_wss_listener_mqtt5_publish_subscribe_smoke() {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();

    let suffix = uuid::Uuid::new_v4();
    let topic = format!("test/mqtt5/wss/{suffix}");
    let payload = b"hello mqtt5 wss";

    let mut subscriber = connect_mqtt5_wss_client(broker_addr, "mqtt5-wss-sub").await;
    common::mqtt5::write_websocket_packet(
        &mut subscriber,
        common::mqtt5::subscribe_packet(1, &topic, 1),
    )
    .await;
    let suback = common::mqtt5::read_websocket_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.reason_codes, vec![0x01]);

    let mut publisher = connect_mqtt5_wss_client(broker_addr, "mqtt5-wss-pub").await;
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

#[actix::test]
pub async fn test_mtls_wss_listener_connect_requires_client_certificate() {
    let context = setup_mtls_instance().await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();

    let mut options = MqttOptions::new(
        "test_client_for_mtls_wss_missing_cert",
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    options.set_keep_alive(Duration::from_secs(5));
    options.set_transport(configure_wss(false));

    let (_client, mut eventloop) = AsyncClient::new(options, 10);

    let connection_handle: tokio::task::JoinHandle<
        std::result::Result<(), rumqttc::ConnectionError>,
    > = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    panic!("mTLS WSS listener accepted a client without a client certificate")
                }
                Ok(_) => continue,
                Err(e) => return Err(e),
            }
        }
    });

    let result = tokio::time::timeout(Duration::from_secs(5), connection_handle)
        .await
        .expect("mTLS WSS handshake should complete");

    assert!(result.unwrap().is_err());
}

#[actix::test]
pub async fn test_mtls_wss_listener_connect_with_client_certificate() {
    let context = setup_mtls_instance().await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();

    let mut options = MqttOptions::new(
        "test_client_for_mtls_wss_connect",
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    options.set_keep_alive(Duration::from_secs(5));
    options.set_transport(configure_wss(true));

    let (client, mut eventloop) = AsyncClient::new(options, 10);

    let connection_handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    client.disconnect().await.unwrap();
                    return Ok(ack.code);
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    return Ok(rumqttc::ConnectReturnCode::Success);
                }
                Ok(_) => continue,
                Err(e) => return Err(e),
            }
        }
    });

    let result = tokio::time::timeout(Duration::from_secs(5), connection_handle)
        .await
        .expect("mTLS WSS connection should not time out");

    let connect_result = result.unwrap();
    assert!(connect_result.is_ok());
    assert_eq!(connect_result.unwrap(), rumqttc::ConnectReturnCode::Success);
}

async fn test_wss_publish_subscribe(qos: QoS) {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();

    println!("borker wss addr {:?}", broker_addr.to_string());

    let mut mqtt_options = MqttOptions::new(
        format!("test-wss-pubsub-{:?}", qos),
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    mqtt_options.set_transport(configure_wss(false));

    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    let topic = format!("test-wss/{:?}", qos);

    let payload = format!("hello wss {:?}", qos).into_bytes();

    let task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client.subscribe(topic.clone(), qos).await.unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    client
                        .publish(topic.clone(), qos, false, payload.clone())
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, topic);
                    assert_eq!(publish.payload.as_ref(), payload.as_slice());
                    assert_eq!(publish.qos, qos);
                    client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => {
                    return;
                }
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Eventloop error: {:?}", e);
                }
                _ => {}
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("Test timed out")
        .unwrap();
}

#[actix::test]
async fn test_wss_publish_subscribe_qos0() {
    test_wss_publish_subscribe(QoS::AtMostOnce).await;
}

#[actix::test]
async fn test_wss_publish_subscribe_qos1() {
    test_wss_publish_subscribe(QoS::AtLeastOnce).await;
}

#[actix::test]
async fn test_wss_publish_subscribe_qos2() {
    test_wss_publish_subscribe(QoS::ExactlyOnce).await;
}

#[actix::test]
async fn test_wss_retained_message() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();
    let topic = "test-wss/retained";
    let payload = b"retained message wss";

    // Client 1 publishes a retained message and disconnects
    let mut mqtt_options1 = MqttOptions::new(
        "wss-retained-publisher",
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    mqtt_options1.set_keep_alive(Duration::from_secs(5));
    mqtt_options1.set_transport(configure_wss(false));
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
                    panic!("Eventloop 1 error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task1)
        .await
        .expect("Task 1 timed out")
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client 2 connects and subscribes, should receive the retained message
    let mut mqtt_options2 = MqttOptions::new(
        "wss-retained-subscriber",
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    mqtt_options2.set_keep_alive(Duration::from_secs(5));
    mqtt_options2.set_transport(configure_wss(false));
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
                    panic!("Eventloop 2 error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task2)
        .await
        .expect("Task 2 timed out")
        .unwrap();
}

#[actix::test]
async fn test_wss_last_will_message() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();
    let will_topic = "test-wss/will";
    let will_message = b"client disconnected uncleanly";
    let last_will = LastWill::new(will_topic, will_message, QoS::AtLeastOnce, false);

    // Subscriber client
    let mut sub_options = MqttOptions::new(
        "wss-will-subscriber",
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    sub_options.set_keep_alive(Duration::from_secs(5));
    sub_options.set_transport(configure_wss(false));
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_options, 10);

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
                    panic!("Subscriber eventloop error: {:?}", e)
                }
                _ => {}
            }
        }
    });

    notify_sub_succeed_receiver.recv().await.unwrap();

    // Client with Last Will, will simulate an unclean disconnect
    let mut will_client_options = MqttOptions::new(
        "wss-will-client",
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    will_client_options
        .set_keep_alive(Duration::from_secs(2))
        .set_last_will(last_will);
    will_client_options.set_transport(configure_wss(false));
    let (_will_client, mut will_eventloop) = AsyncClient::new(will_client_options, 10);

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

    // Wait for subscriber to receive the will message
    tokio::time::timeout(Duration::from_secs(10), sub_task)
        .await
        .expect("Subscriber task timed out")
        .unwrap();
}

#[actix::test]
async fn test_wss_max_qos_subscription() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();
    println!("broker wss addr {:?}", broker_addr.to_string());
    let topic = "test-wss/max_qos";

    // Subscriber with QoS 1
    let mut sub_options = MqttOptions::new(
        "wss-max-qos-subscriber",
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    sub_options.set_keep_alive(Duration::from_secs(5));
    sub_options.set_transport(configure_wss(false));
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_options, 10);

    // Publisher
    let mut pub_options = MqttOptions::new(
        "wss-max-qos-publisher",
        format!("wss://localhost:{}/mqtt", broker_addr.port()),
        broker_addr.port(),
    );
    pub_options.set_keep_alive(Duration::from_secs(5));
    pub_options.set_transport(configure_wss(false));
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_options, 10);

    let payload = b"message for qos downgrade wss";

    let (notify_sub_succeed_sender, mut notify_sub_succeed_receiver) =
        tokio::sync::broadcast::channel(1);

    let sub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match sub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    sub_client.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    notify_sub_succeed_sender.send(()).unwrap();
                    // Ready
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, topic);
                    assert_eq!(publish.payload.as_ref(), payload);
                    // QoS should be downgraded to the subscriber's max QoS (1)
                    assert_eq!(publish.qos, QoS::AtLeastOnce);
                    sub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Subscriber eventloop error: {:?}", e)
                }
                _ => {}
            }
        }
    });

    notify_sub_succeed_receiver.recv().await.unwrap();

    let pub_task = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match pub_eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    // Publish with QoS 2
                    pub_client
                        .publish(topic, QoS::ExactlyOnce, false, payload.to_vec())
                        .await
                        .unwrap();
                    println!("pub succeed")
                }
                Ok(Event::Incoming(Packet::PubComp(_))) => {
                    pub_client.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Publisher eventloop error: {:?}", e)
                }
                _ => {}
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(5), pub_task)
        .await
        .expect("Publisher task timed out")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), sub_task)
        .await
        .expect("Subscriber task timed out")
        .unwrap();
}

#[actix::test]
async fn test_persistent_session() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .wss
        .external
        .as_str()
        .parse()
        .unwrap();
    let topic = "test-wss/persistent";
    let payload = b"persistent message wss";
    let url = format!("wss://localhost:{}/mqtt", broker_addr.port());

    // Client 1 (Persistent) connects, subscribes, disconnects
    let mut mqtt_options1 =
        MqttOptions::new("wss-persistent-client", url.clone(), broker_addr.port());
    mqtt_options1.set_keep_alive(Duration::from_secs(5));
    mqtt_options1.set_transport(configure_wss(false));
    mqtt_options1.set_clean_session(false);
    let (client1, mut eventloop1) = AsyncClient::new(mqtt_options1, 10);

    let task1 = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop1.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client1.subscribe(topic, QoS::AtLeastOnce).await.unwrap();
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    client1.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Eventloop 1 error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task1)
        .await
        .expect("Task 1 timed out")
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client 2 (Publisher) publishes a message
    let mut mqtt_options_pub =
        MqttOptions::new("wss-persistent-publisher", url.clone(), broker_addr.port());
    mqtt_options_pub.set_keep_alive(Duration::from_secs(5));
    mqtt_options_pub.set_transport(configure_wss(false));
    let (client_pub, mut eventloop_pub) = AsyncClient::new(mqtt_options_pub, 10);

    let task_pub = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop_pub.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    connected = true;
                    client_pub
                        .publish(topic, QoS::AtLeastOnce, false, payload.to_vec())
                        .await
                        .unwrap();
                }
                Ok(Event::Incoming(Packet::PubAck(_))) => {
                    client_pub.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Publisher eventloop error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task_pub)
        .await
        .expect("Publisher task timed out")
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client 1 Reconnects
    let mut mqtt_options2 =
        MqttOptions::new("wss-persistent-client", url.clone(), broker_addr.port());
    mqtt_options2.set_keep_alive(Duration::from_secs(5));
    mqtt_options2.set_transport(configure_wss(false));
    mqtt_options2.set_clean_session(false);
    let (client2, mut eventloop2) = AsyncClient::new(mqtt_options2, 10);

    let task2 = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop2.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    connected = true;
                    assert!(ack.session_present); // Expect session present
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    assert_eq!(publish.topic, topic);
                    assert_eq!(publish.payload.as_ref(), payload);
                    client2.disconnect().await.unwrap();
                }
                Ok(Event::Incoming(Packet::Disconnect)) => return,
                Err(e) => {
                    if connected {
                        return;
                    }
                    panic!("Eventloop 2 error: {:?}", e)
                }
                _ => {}
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), task2)
        .await
        .expect("Task 2 timed out")
        .unwrap();
}
