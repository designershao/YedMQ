#[allow(dead_code)]
mod common;

use rumqttc::{AsyncClient, Event, LastWill, MqttOptions, Packet, QoS};
use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use yedmq::app::YedMQApp;
use yedmq::settings::Settings;

static ASYNC_SETUP: OnceCell<TestContext> = OnceCell::const_new();

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
    ASYNC_SETUP
        .get_or_init(|| async {
            let _ = env_logger::builder()
                .filter_level(log::LevelFilter::Info)
                .format_target(false)
                .format_timestamp(None)
                .is_test(true)
                .try_init();

            let original_dir = env::current_dir().unwrap();

            let temp_dir = TempDir::new().unwrap();

            fs::copy("./yedmq.toml", temp_dir.path().join("yedmq.toml")).unwrap();

            env::set_current_dir(temp_dir.path()).unwrap();

            let test_settings = Arc::new(get_test_settings(2, 10, temp_dir.path()));

            let settings_clone = test_settings.clone();

            std::thread::spawn(move || {
                let rt = actix::System::new();
                rt.block_on(async {
                    let app = Arc::new(YedMQApp::new(settings_clone).await);

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

fn random_tcp_port() -> u16 {
    use rand::Rng;
    rand::thread_rng().gen_range(1024..=65535)
}

fn get_test_settings(qos_expired_secs: u64, resend_duration_sec: u64, temp_dir: &Path) -> Settings {
    // Generate a random temporary directory
    fs::create_dir_all(temp_dir).unwrap();
    let test_temp_store_dir = temp_dir.to_str().unwrap().to_string();

    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    let plugin_path = PathBuf::from(crate_root_path).join("tests").join("plugins");

    let tcp_port = random_tcp_port();

    let rpc_port = random_tcp_port();

    let api_port = random_tcp_port();

    let settings = Settings {
        session: yedmq::settings::Session {
            qos_expired_secs,
            packet_resend_interval_secs: resend_duration_sec,
            session_clock_path: temp_dir.join("clock").to_str().unwrap().to_string(),
        },
        listener: yedmq::settings::Listener {
            tcp: yedmq::settings::Tcp {
                external: format!("127.0.0.1:{}", tcp_port).to_string(),
                rate_limit: Default::default(),
            },
            tcp_tls: yedmq::settings::TcpTls {
                external: format!("127.0.0.1:{}", tcp_port + 1).to_string(),
                cacert_file: "".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            ws: yedmq::settings::Ws {
                external: format!("127.0.0.1:{}", tcp_port + 2).to_string(),
                rate_limit: Default::default(),
            },
            wss: yedmq::settings::Wss {
                external: format!("127.0.0.1:{}", tcp_port + 3).to_string(),
                cacert_file: "".to_string(),
                cert_file: "".to_string(),
                key_file: "".to_string(),
                verify_client_cert: false,
                rate_limit: Default::default(),
            },
            api: yedmq::settings::Api {
                external: format!("127.0.0.1:{}", api_port).to_string(),
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
                external: format!("127.0.0.1:{}", rpc_port).to_string(),
            },
            nodes: vec![yedmq::settings::Node {
                id: 1001,
                rpc_address: format!("127.0.0.1:{}", rpc_port).to_string(),
                api_address: format!("127.0.0.1:{}", api_port).to_string(),
            }],
            session_ttl: 10,
            startup_mode: yedmq::settings::ClusterStartupMode::Bootstrap,
        },
    };
    settings
}

#[actix::test]
pub async fn test_tcp_listener_connect() {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let keep_live_duration_secs = 5;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap();

    println!("borker tcp addr {:?}", broker_addr.to_string());

    let mut options = MqttOptions::new(
        "test_client_for_connect",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    options.set_keep_alive(std::time::Duration::from_secs(keep_live_duration_secs));

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
pub async fn test_tcp_listener_mqtt5_connect() {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap();

    let mut stream = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
    stream
        .write_all(&common::mqtt5::connect_packet("mqtt5-connect", true, 5))
        .await
        .unwrap();

    let mut response = vec![0u8; 128];
    let read_len = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    response.truncate(read_len);

    let connack = common::mqtt5::parse_connack(&response).expect("parse MQTT 5 CONNACK");
    assert_eq!(connack.reason_code, 0x00);
    assert!(!connack.session_present);
    assert!(connack.properties.contains(&0x22)); // Topic Alias Maximum
    assert!(connack.properties.contains(&0x29)); // Subscription Identifier Available
    assert!(connack.properties.contains(&0x2a)); // Shared Subscription Available

    stream
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_rejects_unsupported_mqtt_protocol_level() {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap();

    let mut stream = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
    let mut connect = common::mqtt5::connect_packet("mqtt-unsupported", true, 5);
    connect[8] = 0x06;
    stream.write_all(&connect).await.unwrap();

    let mut response = vec![0u8; 4];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(response, vec![0x20, 0x02, 0x00, 0x01]);
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_connect_with_enhanced_auth_is_rejected() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let mut stream = TcpStream::connect(broker_addr).await.unwrap();
    let auth_method = common::mqtt5::authentication_method_property("unsupported");
    stream
        .write_all(&common::mqtt5::connect_packet_with_properties(
            "mqtt5-enhanced-auth-connect",
            true,
            5,
            &auth_method,
        ))
        .await
        .unwrap();

    let connack = read_mqtt5_packet(&mut stream).await;
    let connack = common::mqtt5::parse_connack(&connack).expect("parse MQTT 5 CONNACK");
    assert_eq!(connack.reason_code, 0x8c);
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_auth_packet_disconnects() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let mut client = connect_mqtt5_client(broker_addr, "mqtt5-auth-packet").await;
    client
        .write_all(&common::mqtt5::auth_packet(0x18, &[]))
        .await
        .unwrap();

    let disconnect = read_mqtt5_packet(&mut client).await;
    let disconnect = common::mqtt5::parse_disconnect(&disconnect).expect("parse MQTT 5 DISCONNECT");
    assert_eq!(disconnect.reason_code, 0x8c);
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_topic_alias_disconnects() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let mut client = connect_mqtt5_client(broker_addr, "mqtt5-topic-alias").await;
    let topic_alias = common::mqtt5::topic_alias_property(1);
    client
        .write_all(&common::mqtt5::publish_packet_with_properties(
            "test/mqtt5/topic-alias",
            b"alias",
            0,
            false,
            None,
            &topic_alias,
        ))
        .await
        .unwrap();

    let disconnect = read_mqtt5_packet(&mut client).await;
    let disconnect = common::mqtt5::parse_disconnect(&disconnect).expect("parse MQTT 5 DISCONNECT");
    assert_eq!(disconnect.reason_code, 0x82);
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_subscription_identifier_is_rejected() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let topic = format!(
        "test/mqtt5/subscription-identifier/{}",
        uuid::Uuid::new_v4()
    );
    let mut client = connect_mqtt5_client(broker_addr, "mqtt5-sub-id").await;
    let subscription_identifier = common::mqtt5::subscription_identifier_property(1);
    client
        .write_all(
            &common::mqtt5::subscribe_packet_with_properties_and_options(
                601,
                &topic,
                &subscription_identifier,
                1,
            ),
        )
        .await
        .unwrap();

    let suback = read_mqtt5_packet(&mut client).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.packet_id, 601);
    assert_eq!(suback.reason_codes, vec![0xa1]);

    client
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_publish_subscribe_qos0() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let topic = format!("test/mqtt5/qos0/{}", uuid::Uuid::new_v4());
    let payload = b"hello mqtt5 qos0";

    let mut subscriber = connect_mqtt5_client(broker_addr, "mqtt5-sub-qos0").await;
    subscriber
        .write_all(&common::mqtt5::subscribe_packet(1, &topic, 0))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.packet_id, 1);
    assert_eq!(suback.reason_codes, vec![0x00]);

    let mut publisher = connect_mqtt5_client(broker_addr, "mqtt5-pub-qos0").await;
    publisher
        .write_all(&common::mqtt5::publish_packet(
            &topic, payload, 0, false, None,
        ))
        .await
        .unwrap();

    let publish = read_mqtt5_packet(&mut subscriber).await;
    let publish = common::mqtt5::parse_publish(&publish).expect("parse MQTT 5 PUBLISH");
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert_eq!(publish.qos, 0);
    assert_eq!(publish.packet_id, None);

    publisher
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_publish_subscribe_qos1() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let topic = format!("test/mqtt5/qos1/{}", uuid::Uuid::new_v4());
    let payload = b"hello mqtt5 qos1";

    let mut subscriber = connect_mqtt5_client(broker_addr, "mqtt5-sub-qos1").await;
    subscriber
        .write_all(&common::mqtt5::subscribe_packet(11, &topic, 1))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.packet_id, 11);
    assert_eq!(suback.reason_codes, vec![0x01]);

    let mut publisher = connect_mqtt5_client(broker_addr, "mqtt5-pub-qos1").await;
    publisher
        .write_all(&common::mqtt5::publish_packet(
            &topic,
            payload,
            1,
            false,
            Some(77),
        ))
        .await
        .unwrap();

    let puback = read_mqtt5_packet(&mut publisher).await;
    let puback = common::mqtt5::parse_puback(&puback).expect("parse MQTT 5 PUBACK");
    assert_eq!(puback.packet_id, 77);
    assert_eq!(puback.reason_code, 0x00);

    let publish = read_mqtt5_packet(&mut subscriber).await;
    let publish = common::mqtt5::parse_publish(&publish).expect("parse MQTT 5 PUBLISH");
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert_eq!(publish.qos, 1);
    assert!(publish.packet_id.is_some());

    publisher
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_qos2_publish_subscribe_flow() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let topic = format!("test/mqtt5/qos2/{}", uuid::Uuid::new_v4());
    let payload = b"hello mqtt5 qos2";

    let mut subscriber = connect_mqtt5_client(broker_addr, "mqtt5-sub-qos2").await;
    subscriber
        .write_all(&common::mqtt5::subscribe_packet(22, &topic, 2))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.packet_id, 22);
    assert_eq!(suback.reason_codes, vec![0x02]);

    let mut publisher = connect_mqtt5_client(broker_addr, "mqtt5-pub-qos2").await;
    publisher
        .write_all(&common::mqtt5::publish_packet(
            &topic,
            payload,
            2,
            false,
            Some(88),
        ))
        .await
        .unwrap();

    let pubrec = read_mqtt5_packet(&mut publisher).await;
    let pubrec = common::mqtt5::parse_pubrec(&pubrec).expect("parse MQTT 5 PUBREC");
    assert_eq!(pubrec.packet_id, 88);
    assert_eq!(pubrec.reason_code, 0x00);

    publisher
        .write_all(&common::mqtt5::pubrel_packet(88))
        .await
        .unwrap();
    let pubcomp = read_mqtt5_packet(&mut publisher).await;
    let pubcomp = common::mqtt5::parse_pubcomp(&pubcomp).expect("parse MQTT 5 PUBCOMP");
    assert_eq!(pubcomp.packet_id, 88);
    assert_eq!(pubcomp.reason_code, 0x00);

    let publish = read_mqtt5_packet(&mut subscriber).await;
    let publish = common::mqtt5::parse_publish(&publish).expect("parse MQTT 5 PUBLISH");
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert_eq!(publish.qos, 2);
    let subscriber_packet_id = publish
        .packet_id
        .expect("broker should assign packet id for QoS 2 delivery");

    subscriber
        .write_all(&common::mqtt5::pubrec_packet(subscriber_packet_id))
        .await
        .unwrap();
    let pubrel = read_mqtt5_packet(&mut subscriber).await;
    let pubrel = common::mqtt5::parse_pubrel(&pubrel).expect("parse MQTT 5 PUBREL");
    assert_eq!(pubrel.packet_id, subscriber_packet_id);
    assert_eq!(pubrel.reason_code, 0x00);

    subscriber
        .write_all(&common::mqtt5::pubcomp_packet(subscriber_packet_id))
        .await
        .unwrap();

    publisher
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_retained_message_options() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let topic = format!("test/mqtt5/retain/{}", uuid::Uuid::new_v4());
    let payload = b"hello retained mqtt5";
    let content_type_property = [vec![0x03], mqtt_utf8_string("text/plain")].concat();

    let mut publisher = connect_mqtt5_client(broker_addr, "mqtt5-retain-pub").await;
    publisher
        .write_all(&common::mqtt5::publish_packet_with_properties(
            &topic,
            payload,
            1,
            true,
            Some(91),
            &content_type_property,
        ))
        .await
        .unwrap();
    let puback = read_mqtt5_packet(&mut publisher).await;
    let puback = common::mqtt5::parse_puback(&puback).expect("parse MQTT 5 PUBACK");
    assert_eq!(puback.packet_id, 91);

    let mut default_subscriber = connect_mqtt5_client(broker_addr, "mqtt5-retain-default").await;
    default_subscriber
        .write_all(&common::mqtt5::subscribe_packet(92, &topic, 0))
        .await
        .unwrap();
    let (_, publish) = read_mqtt5_suback_and_publish(&mut default_subscriber).await;
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert!(!publish.retain);
    assert_eq!(publish.properties, content_type_property);

    let mut rap_subscriber = connect_mqtt5_client(broker_addr, "mqtt5-retain-rap").await;
    rap_subscriber
        .write_all(&common::mqtt5::subscribe_packet_with_options(
            93, &topic, 0x08,
        ))
        .await
        .unwrap();
    let (_, publish) = read_mqtt5_suback_and_publish(&mut rap_subscriber).await;
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert!(publish.retain);

    let mut suppress_subscriber = connect_mqtt5_client(broker_addr, "mqtt5-retain-rh2").await;
    suppress_subscriber
        .write_all(&common::mqtt5::subscribe_packet_with_options(
            94, &topic, 0x20,
        ))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut suppress_subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.packet_id, 94);
    assert_eq!(suback.reason_codes, vec![0x00]);
    let no_publish = tokio::time::timeout(
        Duration::from_millis(500),
        read_mqtt5_packet(&mut suppress_subscriber),
    )
    .await;
    assert!(
        no_publish.is_err(),
        "Retain Handling 2 subscription received a retained publish"
    );

    let mut mqtt3_subscriber = connect_mqtt3_client(broker_addr, "mqtt3-retain-sub").await;
    mqtt3_subscriber
        .write_all(&mqtt3_subscribe_packet(95, &topic, 0))
        .await
        .unwrap();
    let (_, publish) = read_mqtt3_suback_and_publish(&mut mqtt3_subscriber, 95).await;
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert!(publish.retain);

    publisher
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    default_subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    rap_subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    suppress_subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    mqtt3_subscriber
        .write_all(&mqtt3_disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt3_retained_message_reaches_mqtt5_subscriber() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let topic = format!("test/mixed/v3-retain-to-v5/{}", uuid::Uuid::new_v4());
    let payload = b"from mqtt3 retained publisher";

    let mut publisher = connect_mqtt3_client(broker_addr, "mqtt3-retain-pub").await;
    publisher
        .write_all(&mqtt3_publish_packet(&topic, payload, 0, true))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut subscriber = connect_mqtt5_client(broker_addr, "mqtt5-retain-from-v3").await;
    subscriber
        .write_all(&common::mqtt5::subscribe_packet_with_options(
            96, &topic, 0x08,
        ))
        .await
        .unwrap();
    let (_, publish) = read_mqtt5_suback_and_publish(&mut subscriber).await;
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert!(publish.retain);
    assert!(publish.properties.is_empty());

    publisher
        .write_all(&mqtt3_disconnect_packet())
        .await
        .unwrap();
    subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_no_local_skips_self_publish() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let topic = format!("test/mqtt5/no-local/{}", uuid::Uuid::new_v4());

    let mut client = connect_mqtt5_client(broker_addr, "mqtt5-no-local").await;
    client
        .write_all(&common::mqtt5::subscribe_packet_with_options(
            41, &topic, 0x04,
        ))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut client).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.reason_codes, vec![0x00]);

    client
        .write_all(&common::mqtt5::publish_packet(
            &topic,
            b"should-not-loop",
            0,
            false,
            None,
        ))
        .await
        .unwrap();

    let no_publish =
        tokio::time::timeout(Duration::from_millis(500), read_mqtt5_packet(&mut client)).await;
    assert!(
        no_publish.is_err(),
        "No Local subscription received its own publish"
    );

    client
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_default_session_expiry_drops_session() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let suffix = uuid::Uuid::new_v4();
    let client_id = format!("mqtt5-default-expiry-{suffix}");
    let publisher_id = format!("mqtt5-default-expiry-pub-{suffix}");
    let topic = format!("test/mqtt5/default-expiry/{suffix}");
    let payload = b"default-expiry-message";

    let (mut subscriber, first_connack) =
        connect_mqtt5_client_with_properties(broker_addr, &client_id, true, &[]).await;
    assert!(!first_connack.session_present);
    subscriber
        .write_all(&common::mqtt5::subscribe_packet(501, &topic, 1))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.reason_codes, vec![0x01]);
    subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();

    let mut publisher = connect_mqtt5_client(broker_addr, &publisher_id).await;
    publisher
        .write_all(&common::mqtt5::publish_packet(
            &topic,
            payload,
            1,
            false,
            Some(502),
        ))
        .await
        .unwrap();
    let puback = read_mqtt5_packet(&mut publisher).await;
    let puback = common::mqtt5::parse_puback(&puback).expect("parse MQTT 5 PUBACK");
    assert_eq!(puback.packet_id, 502);
    publisher
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();

    let (mut reconnect, reconnect_connack) =
        connect_mqtt5_client_with_properties(broker_addr, &client_id, false, &[]).await;
    assert!(!reconnect_connack.session_present);
    let no_publish = tokio::time::timeout(
        Duration::from_millis(500),
        read_mqtt5_packet(&mut reconnect),
    )
    .await;
    assert!(
        no_publish.is_err(),
        "MQTT 5 default session expiry preserved an offline publish"
    );
    reconnect
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_nonzero_session_expiry_recovers_offline_message() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let suffix = uuid::Uuid::new_v4();
    let client_id = format!("mqtt5-expiring-session-{suffix}");
    let publisher_id = format!("mqtt5-expiring-session-pub-{suffix}");
    let topic = format!("test/mqtt5/session-expiry/{suffix}");
    let payload = b"persistent-mqtt5-offline-message";
    let session_expiry = common::mqtt5::session_expiry_interval_property(60);

    let (mut subscriber, first_connack) =
        connect_mqtt5_client_with_properties(broker_addr, &client_id, true, &session_expiry).await;
    assert!(!first_connack.session_present);
    subscriber
        .write_all(&common::mqtt5::subscribe_packet(511, &topic, 1))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.reason_codes, vec![0x01]);
    subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();

    let mut publisher = connect_mqtt5_client(broker_addr, &publisher_id).await;
    publisher
        .write_all(&common::mqtt5::publish_packet(
            &topic,
            payload,
            1,
            false,
            Some(512),
        ))
        .await
        .unwrap();
    let puback = read_mqtt5_packet(&mut publisher).await;
    let puback = common::mqtt5::parse_puback(&puback).expect("parse MQTT 5 PUBACK");
    assert_eq!(puback.packet_id, 512);
    publisher
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();

    let (mut reconnect, reconnect_connack) =
        connect_mqtt5_client_with_properties(broker_addr, &client_id, false, &session_expiry).await;
    assert!(reconnect_connack.session_present);
    let publish = read_mqtt5_packet(&mut reconnect).await;
    let publish = common::mqtt5::parse_publish(&publish).expect("parse MQTT 5 PUBLISH");
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert_eq!(publish.qos, 1);
    reconnect
        .write_all(&common::mqtt5::puback_packet(
            publish.packet_id.expect("offline publish packet id"),
        ))
        .await
        .unwrap();
    reconnect
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_message_expiry_adjusts_live_publish() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let suffix = uuid::Uuid::new_v4();
    let topic = format!("test/mqtt5/message-expiry/live/{suffix}");
    let payload = b"live-message-expiry";
    let expiry = common::mqtt5::message_expiry_interval_property(60);

    let mut subscriber = connect_mqtt5_client(broker_addr, "mqtt5-expiry-live-sub").await;
    subscriber
        .write_all(&common::mqtt5::subscribe_packet(521, &topic, 0))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.reason_codes, vec![0x00]);

    let mut publisher = connect_mqtt5_client(broker_addr, "mqtt5-expiry-live-pub").await;
    publisher
        .write_all(&common::mqtt5::publish_packet_with_properties(
            &topic, payload, 0, false, None, &expiry,
        ))
        .await
        .unwrap();

    let publish = read_mqtt5_packet(&mut subscriber).await;
    let publish = common::mqtt5::parse_publish(&publish).expect("parse MQTT 5 PUBLISH");
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    let remaining = common::mqtt5::find_message_expiry_interval(&publish.properties)
        .expect("parse publish properties")
        .expect("message expiry interval should be forwarded");
    assert!((1..=60).contains(&remaining));

    subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    publisher
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_expired_retained_message_is_not_delivered() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let suffix = uuid::Uuid::new_v4();
    let topic = format!("test/mqtt5/message-expiry/retain/{suffix}");
    let payload = b"expired-retained-message";
    let expiry = common::mqtt5::message_expiry_interval_property(1);

    let mut publisher = connect_mqtt5_client(broker_addr, "mqtt5-expired-retain-pub").await;
    publisher
        .write_all(&common::mqtt5::publish_packet_with_properties(
            &topic,
            payload,
            1,
            true,
            Some(531),
            &expiry,
        ))
        .await
        .unwrap();
    let puback = read_mqtt5_packet(&mut publisher).await;
    let puback = common::mqtt5::parse_puback(&puback).expect("parse MQTT 5 PUBACK");
    assert_eq!(puback.packet_id, 531);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut subscriber = connect_mqtt5_client(broker_addr, "mqtt5-expired-retain-sub").await;
    subscriber
        .write_all(&common::mqtt5::subscribe_packet(532, &topic, 0))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.packet_id, 532);

    let no_publish = tokio::time::timeout(
        Duration::from_millis(500),
        read_mqtt5_packet(&mut subscriber),
    )
    .await;
    assert!(
        no_publish.is_err(),
        "expired retained MQTT 5 publish was delivered"
    );

    subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    publisher
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_expired_offline_message_is_not_delivered() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let suffix = uuid::Uuid::new_v4();
    let client_id = format!("mqtt5-expired-offline-{suffix}");
    let publisher_id = format!("mqtt5-expired-offline-pub-{suffix}");
    let topic = format!("test/mqtt5/message-expiry/offline/{suffix}");
    let payload = b"expired-offline-message";
    let session_expiry = common::mqtt5::session_expiry_interval_property(60);
    let message_expiry = common::mqtt5::message_expiry_interval_property(1);

    let (mut subscriber, first_connack) =
        connect_mqtt5_client_with_properties(broker_addr, &client_id, true, &session_expiry).await;
    assert!(!first_connack.session_present);
    subscriber
        .write_all(&common::mqtt5::subscribe_packet(541, &topic, 1))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.reason_codes, vec![0x01]);
    subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();

    let mut publisher = connect_mqtt5_client(broker_addr, &publisher_id).await;
    publisher
        .write_all(&common::mqtt5::publish_packet_with_properties(
            &topic,
            payload,
            1,
            false,
            Some(542),
            &message_expiry,
        ))
        .await
        .unwrap();
    let puback = read_mqtt5_packet(&mut publisher).await;
    let puback = common::mqtt5::parse_puback(&puback).expect("parse MQTT 5 PUBACK");
    assert_eq!(puback.packet_id, 542);
    publisher
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let (mut reconnect, reconnect_connack) =
        connect_mqtt5_client_with_properties(broker_addr, &client_id, false, &session_expiry).await;
    assert!(reconnect_connack.session_present);
    let no_publish = tokio::time::timeout(
        Duration::from_millis(500),
        read_mqtt5_packet(&mut reconnect),
    )
    .await;
    assert!(
        no_publish.is_err(),
        "expired offline MQTT 5 publish was delivered"
    );
    reconnect
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt5_publisher_to_mqtt3_subscriber() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let topic = format!("test/mixed/v5-to-v3/{}", uuid::Uuid::new_v4());
    let payload = b"from mqtt5 publisher".to_vec();

    let mut subscriber = connect_mqtt3_client(broker_addr, "mixed-v3-sub-to-v5-pub").await;
    subscriber
        .write_all(&mqtt3_subscribe_packet(31, &topic, 0))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut subscriber).await;
    assert_mqtt3_suback(&suback, 31, 0);

    let mut publisher = connect_mqtt5_client(broker_addr, "mixed-v5-pub-to-v3-sub").await;
    publisher
        .write_all(&common::mqtt5::publish_packet(
            &topic, &payload, 0, false, None,
        ))
        .await
        .unwrap();

    let publish = read_mqtt5_packet(&mut subscriber).await;
    let publish = parse_mqtt3_publish(&publish).expect("parse MQTT 3 PUBLISH");
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert_eq!(publish.qos, 0);

    publisher
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
    subscriber
        .write_all(&mqtt3_disconnect_packet())
        .await
        .unwrap();
}

#[actix::test]
pub async fn test_tcp_listener_mqtt3_publisher_to_mqtt5_subscriber() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr = tcp_broker_addr(context);
    let topic = format!("test/mixed/v3-to-v5/{}", uuid::Uuid::new_v4());
    let payload = b"from mqtt3 publisher";

    let mut subscriber = connect_mqtt5_client(broker_addr, "mixed-v5-sub-to-v3-pub").await;
    subscriber
        .write_all(&common::mqtt5::subscribe_packet(21, &topic, 0))
        .await
        .unwrap();
    let suback = read_mqtt5_packet(&mut subscriber).await;
    let suback = common::mqtt5::parse_suback(&suback).expect("parse MQTT 5 SUBACK");
    assert_eq!(suback.reason_codes, vec![0x00]);

    let mut publisher = connect_mqtt3_client(broker_addr, "mixed-v3-pub-to-v5-sub").await;
    publisher
        .write_all(&mqtt3_publish_packet(&topic, payload, 0, false))
        .await
        .unwrap();

    let publish = read_mqtt5_packet(&mut subscriber).await;
    let publish = common::mqtt5::parse_publish(&publish).expect("parse MQTT 5 PUBLISH");
    assert_eq!(publish.topic, topic);
    assert_eq!(publish.payload, payload);
    assert_eq!(publish.qos, 0);
    assert_eq!(publish.packet_id, None);

    publisher
        .write_all(&mqtt3_disconnect_packet())
        .await
        .unwrap();
    subscriber
        .write_all(&common::mqtt5::disconnect_packet())
        .await
        .unwrap();
}

fn tcp_broker_addr(context: &TestContext) -> SocketAddr {
    context
        .settings
        .listener
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap()
}

async fn connect_mqtt5_client(broker_addr: SocketAddr, client_id: &str) -> TcpStream {
    let (stream, connack) =
        connect_mqtt5_client_with_properties(broker_addr, client_id, true, &[]).await;
    assert_eq!(connack.reason_code, 0x00);
    stream
}

async fn connect_mqtt5_client_with_properties(
    broker_addr: SocketAddr,
    client_id: &str,
    clean_start: bool,
    properties: &[u8],
) -> (TcpStream, common::mqtt5::Connack) {
    let mut stream = TcpStream::connect(broker_addr).await.unwrap();
    stream
        .write_all(&common::mqtt5::connect_packet_with_properties(
            client_id,
            clean_start,
            5,
            properties,
        ))
        .await
        .unwrap();

    let connack = read_mqtt5_packet(&mut stream).await;
    let connack = common::mqtt5::parse_connack(&connack).expect("parse MQTT 5 CONNACK");
    assert_eq!(connack.reason_code, 0x00);
    (stream, connack)
}

async fn read_mqtt5_packet(stream: &mut TcpStream) -> Vec<u8> {
    let mut fixed_header = vec![0u8; 1];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut fixed_header))
        .await
        .expect("timed out reading MQTT packet first byte")
        .expect("read MQTT packet first byte");

    let mut multiplier = 1usize;
    let mut remaining_len = 0usize;
    for _ in 0..4 {
        let mut byte = [0u8; 1];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut byte))
            .await
            .expect("timed out reading MQTT remaining length")
            .expect("read MQTT remaining length");
        fixed_header.push(byte[0]);
        remaining_len += ((byte[0] & 0x7F) as usize) * multiplier;
        if byte[0] & 0x80 == 0 {
            let mut body = vec![0u8; remaining_len];
            if remaining_len > 0 {
                tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut body))
                    .await
                    .expect("timed out reading MQTT packet body")
                    .expect("read MQTT packet body");
            }
            fixed_header.extend_from_slice(&body);
            return fixed_header;
        }
        multiplier *= 128;
    }

    panic!("malformed MQTT remaining length");
}

async fn read_mqtt5_suback_and_publish(
    stream: &mut TcpStream,
) -> (common::mqtt5::Suback, common::mqtt5::Publish) {
    let mut suback = None;
    let mut publish = None;

    for _ in 0..2 {
        let packet = read_mqtt5_packet(stream).await;
        match packet[0] >> 4 {
            0x03 => {
                publish = Some(
                    common::mqtt5::parse_publish(&packet).expect("parse MQTT 5 retained PUBLISH"),
                );
            }
            0x09 => {
                suback = Some(common::mqtt5::parse_suback(&packet).expect("parse MQTT 5 SUBACK"));
            }
            packet_type => panic!("unexpected MQTT 5 packet type {packet_type}"),
        }
    }

    (
        suback.expect("SUBACK should be received"),
        publish.expect("retained PUBLISH should be received"),
    )
}

async fn read_mqtt3_suback_and_publish(
    stream: &mut TcpStream,
    packet_id: u16,
) -> ((), Mqtt3Publish) {
    let mut suback_seen = false;
    let mut publish = None;

    for _ in 0..2 {
        let packet = read_mqtt5_packet(stream).await;
        match packet[0] >> 4 {
            0x03 => {
                publish =
                    Some(parse_mqtt3_publish(&packet).expect("parse MQTT 3 retained PUBLISH"));
            }
            0x09 => {
                assert_mqtt3_suback(&packet, packet_id, 0);
                suback_seen = true;
            }
            packet_type => panic!("unexpected MQTT 3 packet type {packet_type}"),
        }
    }

    assert!(suback_seen, "SUBACK should be received");
    ((), publish.expect("retained PUBLISH should be received"))
}

struct Mqtt3Publish {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
}

async fn connect_mqtt3_client(broker_addr: SocketAddr, client_id: &str) -> TcpStream {
    let mut stream = TcpStream::connect(broker_addr).await.unwrap();
    stream
        .write_all(&mqtt3_connect_packet(client_id, true, 5))
        .await
        .unwrap();

    let connack = read_mqtt5_packet(&mut stream).await;
    assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
    stream
}

fn mqtt3_connect_packet(client_id: &str, clean_session: bool, keep_alive_secs: u16) -> Vec<u8> {
    let mut variable_header = Vec::new();
    variable_header.extend_from_slice(&mqtt_utf8_string("MQTT"));
    variable_header.push(0x04);
    variable_header.push(if clean_session { 0x02 } else { 0x00 });
    variable_header.extend_from_slice(&keep_alive_secs.to_be_bytes());

    let mut payload = Vec::new();
    payload.extend_from_slice(&mqtt_utf8_string(client_id));

    mqtt_control_packet(0x10, [variable_header, payload].concat())
}

fn mqtt3_subscribe_packet(packet_id: u16, topic: &str, qos: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(&mqtt_utf8_string(topic));
    body.push(qos & 0x03);

    mqtt_control_packet(0x82, body)
}

fn mqtt3_publish_packet(topic: &str, payload: &[u8], qos: u8, retain: bool) -> Vec<u8> {
    assert_eq!(qos, 0, "test helper only builds QoS 0 MQTT 3 PUBLISH");
    let mut body = Vec::new();
    body.extend_from_slice(&mqtt_utf8_string(topic));
    body.extend_from_slice(payload);

    let flags = ((qos & 0x03) << 1) | u8::from(retain);
    mqtt_control_packet(0x30 | flags, body)
}

fn mqtt3_disconnect_packet() -> Vec<u8> {
    vec![0xE0, 0x00]
}

fn assert_mqtt3_suback(packet: &[u8], packet_id: u16, return_code: u8) {
    assert_eq!(
        packet,
        [
            vec![0x90, 0x03],
            packet_id.to_be_bytes().to_vec(),
            vec![return_code],
        ]
        .concat()
    );
}

fn parse_mqtt3_publish(packet: &[u8]) -> Result<Mqtt3Publish, String> {
    if packet.len() < 4 {
        return Err("MQTT 3 PUBLISH packet is too short".to_string());
    }
    if packet[0] >> 4 != 0x03 {
        return Err("expected MQTT 3 PUBLISH packet".to_string());
    }
    let retain = packet[0] & 0b0001 != 0;
    let qos = (packet[0] & 0b0110) >> 1;
    if qos != 0 {
        return Err("test helper only parses QoS 0 MQTT 3 PUBLISH".to_string());
    }
    let (remaining_len, len_bytes) = decode_mqtt_variable_byte_integer(&packet[1..])?;
    let body_start = 1 + len_bytes;
    let body_end = body_start + remaining_len as usize;
    if body_end != packet.len() {
        return Err("MQTT 3 PUBLISH remaining length mismatch".to_string());
    }

    let (topic, topic_len) = parse_mqtt_utf8_string(&packet[body_start..body_end])?;
    Ok(Mqtt3Publish {
        topic,
        payload: packet[body_start + topic_len..body_end].to_vec(),
        qos,
        retain,
    })
}

fn mqtt_control_packet(first_byte: u8, body: Vec<u8>) -> Vec<u8> {
    let mut packet = Vec::with_capacity(1 + 4 + body.len());
    packet.push(first_byte);
    packet.extend_from_slice(&encode_mqtt_variable_byte_integer(body.len() as u32));
    packet.extend_from_slice(&body);
    packet
}

fn mqtt_utf8_string(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).expect("MQTT UTF-8 string length fits into u16");
    let mut out = Vec::with_capacity(2 + bytes.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

fn parse_mqtt_utf8_string(input: &[u8]) -> Result<(String, usize), String> {
    if input.len() < 2 {
        return Err("MQTT UTF-8 string length is missing".to_string());
    }
    let len = u16::from_be_bytes([input[0], input[1]]) as usize;
    if input.len() < 2 + len {
        return Err("MQTT UTF-8 string bytes are incomplete".to_string());
    }
    let value = std::str::from_utf8(&input[2..2 + len])
        .map_err(|err| format!("invalid MQTT UTF-8 string: {err}"))?
        .to_string();
    Ok((value, 2 + len))
}

fn encode_mqtt_variable_byte_integer(mut value: u32) -> Vec<u8> {
    assert!(
        value <= 268_435_455,
        "MQTT variable byte integer is too large"
    );
    let mut out = Vec::new();
    loop {
        let mut encoded = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            encoded |= 128;
        }
        out.push(encoded);
        if value == 0 {
            return out;
        }
    }
}

fn decode_mqtt_variable_byte_integer(input: &[u8]) -> Result<(u32, usize), String> {
    let mut multiplier = 1u32;
    let mut value = 0u32;

    for (idx, byte) in input.iter().copied().enumerate() {
        value += ((byte & 0x7F) as u32) * multiplier;
        if byte & 0x80 == 0 {
            return Ok((value, idx + 1));
        }
        multiplier = multiplier
            .checked_mul(128)
            .ok_or_else(|| "malformed MQTT variable byte integer".to_string())?;
        if idx == 3 {
            return Err("malformed MQTT variable byte integer".to_string());
        }
    }

    Err("incomplete MQTT variable byte integer".to_string())
}

async fn test_publish_subscribe(qos: QoS) {
    let context = setup_instance().await;

    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap();

    println!("borker tcp addr {:?}", broker_addr.to_string());

    let mut mqtt_options = MqttOptions::new(
        format!("test-pubsub-{:?}", qos),
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    mqtt_options.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    let topic = format!("test/{:?}", qos);

    let payload = format!("hello {:?}", qos).into_bytes();

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
async fn test_publish_subscribe_qos0() {
    test_publish_subscribe(QoS::AtMostOnce).await;
}

#[actix::test]
async fn test_publish_subscribe_qos1() {
    test_publish_subscribe(QoS::AtLeastOnce).await;
}

#[actix::test]
async fn test_publish_subscribe_qos2() {
    test_publish_subscribe(QoS::ExactlyOnce).await;
}

#[actix::test]
async fn test_retained_message() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap();
    let topic = "test/retained";
    let payload = b"retained message";

    // Client 1 publishes a retained message and disconnects
    let mut mqtt_options1 = MqttOptions::new(
        "retained-publisher",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    mqtt_options1.set_keep_alive(Duration::from_secs(5));
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
        "retained-subscriber",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    mqtt_options2.set_keep_alive(Duration::from_secs(5));
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
async fn test_last_will_message() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap();
    let will_topic = "test/will";
    let will_message = b"client disconnected uncleanly";
    let last_will = LastWill::new(will_topic, will_message, QoS::AtLeastOnce, false);

    // Subscriber client
    let mut sub_options = MqttOptions::new(
        "will-subscriber",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    sub_options.set_keep_alive(Duration::from_secs(5));
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
        "will-client",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    will_client_options
        .set_keep_alive(Duration::from_secs(2))
        .set_last_will(last_will);
    let (_will_client, mut will_eventloop) = AsyncClient::new(will_client_options, 10);

    let will_task = tokio::spawn(async move {
        let mut connected = false;
        while !connected {
            match will_eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                    connected = true;
                }
                Ok(_) => {} // Continue processing other initialization events (such as SubAck, etc.)
                Err(_e) => {
                    return;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    // Wait for the will client to connect
    tokio::time::timeout(Duration::from_secs(6), will_task)
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
async fn test_max_qos_subscription() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let broker_addr: SocketAddr = context
        .settings
        .listener
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap();
    println!("broker tcp addr {:?}", broker_addr.to_string());
    let topic = "test/max_qos";

    // Subscriber with QoS 1
    let mut sub_options = MqttOptions::new(
        "max-qos-subscriber",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    sub_options.set_keep_alive(Duration::from_secs(5));
    let (sub_client, mut sub_eventloop) = AsyncClient::new(sub_options, 10);

    // Publisher
    let mut pub_options = MqttOptions::new(
        "max-qos-publisher",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    pub_options.set_keep_alive(Duration::from_secs(5));
    let (pub_client, mut pub_eventloop) = AsyncClient::new(pub_options, 10);

    let payload = b"message for qos downgrade";

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
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap();
    let topic = "test/persistent";
    let payload = b"persistent message";

    // Client 1 (Persistent) connects, subscribes, disconnects
    let mut mqtt_options1 = MqttOptions::new(
        "persistent-client",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    mqtt_options1.set_keep_alive(Duration::from_secs(5));
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
    let mut mqtt_options_pub = MqttOptions::new(
        "persistent-publisher",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    mqtt_options_pub.set_keep_alive(Duration::from_secs(5));
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
    let mut mqtt_options2 = MqttOptions::new(
        "persistent-client",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    mqtt_options2.set_keep_alive(Duration::from_secs(5));
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

#[actix::test]
async fn test_persistent_session_cleared_by_clean_session() {
    let context = setup_instance().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr: SocketAddr = context
        .settings
        .listener
        .tcp
        .external
        .as_str()
        .parse()
        .unwrap();
    let topic = "test/persistent/clear";
    let payload = b"should be discarded";

    // 1. Client 1 (Persistent) connects, subscribes, disconnects
    let mut mqtt_options1 = MqttOptions::new(
        "persistent-to-clean-client",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    mqtt_options1.set_keep_alive(Duration::from_secs(5));
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

    // 2. Client 2 (Publisher) publishes a message
    let mut mqtt_options_pub = MqttOptions::new(
        "persistent-clear-publisher",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    mqtt_options_pub.set_keep_alive(Duration::from_secs(5));
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

    // 3. Client 1 Reconnects with clean_session = true
    let mut mqtt_options2 = MqttOptions::new(
        "persistent-to-clean-client",
        broker_addr.ip().to_string(),
        broker_addr.port(),
    );
    mqtt_options2.set_keep_alive(Duration::from_secs(5));
    mqtt_options2.set_clean_session(true);
    let (client2, mut eventloop2) = AsyncClient::new(mqtt_options2, 10);

    let task2 = tokio::spawn(async move {
        let mut connected = false;
        loop {
            match eventloop2.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    connected = true;
                    assert!(
                        !ack.session_present,
                        "Session should NOT be present when clean_session=true"
                    );
                }
                Ok(Event::Incoming(Packet::Publish(_))) => {
                    panic!("Should NOT receive any message");
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

    // We expect timeout because we should NOT receive the message.
    let res = tokio::time::timeout(Duration::from_secs(2), task2).await;
    assert!(
        res.is_err(),
        "Task 2 should timeout waiting for messages (none expected)"
    );

    client2.disconnect().await.unwrap();
}
