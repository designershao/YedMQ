use super::*;
use crate::raft::payload::{store::Result as StoreResult, RocksDBPayloadStore};
use yedmq_plugin_host::plugin_host_config::PluginHostConfig;

#[derive(Debug)]
struct GatedStore {
    inner: RocksDBPayloadStore,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    fail: bool,
}

#[async_trait::async_trait]
impl PayloadStore for GatedStore {
    async fn put(&self, key: &String, data: Bytes) -> StoreResult<()> {
        self.entered.notify_one();
        self.release.notified().await;
        if self.fail {
            return Err(PayloadError::Storage("injected write failure".into()));
        }
        self.inner.put(key, data).await
    }
    async fn get(&self, key: &String) -> StoreResult<Option<Bytes>> {
        self.inner.get(key).await
    }
    async fn contains(&self, key: &String) -> StoreResult<bool> {
        self.inner.contains(key).await
    }
    async fn delete(&self, key: &String) -> StoreResult<()> {
        self.inner.delete(key).await
    }
    async fn put_batch(&self, entries: Vec<(String, Bytes)>) -> StoreResult<()> {
        self.inner.put_batch(entries).await
    }
}

struct TestConnection;
impl Actor for TestConnection {
    type Context = Context<Self>;
}
impl Handler<ConnectionActorMessage> for TestConnection {
    type Result = Result<(), crate::connection::ConnectionError>;
    fn handle(&mut self, _: ConnectionActorMessage, _: &mut Self::Context) -> Self::Result {
        Ok(())
    }
}

fn publish(id: u16, expiry: Option<u64>) -> Publish {
    let mut packet = Packet::from(yedmq_mqtt::MqttPacketV3::Publish(
        yedmq_mqtt::v3::publish::PublishPacketBuilder::new(
            "topic".into(),
            Bytes::from_static(b"data"),
        )
        .qos(1)
        .packet_identifier(id)
        .build(),
    ));
    let Packet::Publish(ref mut publish) = packet else {
        unreachable!()
    };
    publish.expires_at_unix_secs = expiry;
    publish.clone()
}

async fn actor_config(
    dir: &std::path::Path,
    store: Arc<dyn PayloadStore>,
) -> (SessionActorConfig, mpsc::Receiver<SessionLifecycleMessage>) {
    let (shutdown_signal, _) = tokio::sync::broadcast::channel(1);
    let plugin_manager = PluginManager::new(PluginHostConfig {
        broker_version: "test".into(),
        broker_node_id: 1,
        cluster_name: "test".into(),
        plugin_directory: dir.to_string_lossy().into(),
        local_socket_path: "unused".into(),
        max_restart_attempts: 0,
        health_check_interval_secs: 60,
        init_timeout_secs: 1,
        request_timeout_secs: 1,
        ping_timeout_secs: 1,
        shutdown_signal,
        default_authorize_result: true,
        default_authenticate_result: true,
    })
    .await
    .unwrap();
    let (tx, rx) = mpsc::channel(8);
    (
        SessionActorConfig {
            tenant_id: "t".into(),
            client_id: "c".into(),
            clean_session: true,
            protocol_version: ProtocolVersion::V5_0,
            session_expiry_interval: None,
            receive_maximum: 1,
            plugin_manager: Arc::new(plugin_manager),
            inflight_retry_duration_secs: 60,
            will_message: None,
            keep_alive: 60,
            connection_actor_addr: TestConnection.start().recipient(),
            peer_addr: "127.0.0.1:1883".parse().unwrap(),
            session_state: Arc::new(RwLock::new(SessionState::new(Duration::from_secs(60)))),
            session_lifecycle_tx: tx,
            session_state_service: SessionStateService::from_registry(),
            topic_service: TopicService::from_registry(),
            router_actors: vec![],
            payload_store: Some(store),
            timer_actor: TimerActor::new().start(),
            metric: Arc::new(Metric::new()),
            session_instance_id: SessionInstanceId::new(1),
        },
        rx,
    )
}

#[actix::test]
async fn routed_ack_waits_for_storage_and_propagates_write_failure() {
    for fail in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(GatedStore {
            inner: RocksDBPayloadStore::new(dir.path().join("payload")).unwrap(),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            fail,
        });
        let (config, mut lifecycle) = actor_config(dir.path(), store.clone()).await;
        let state = config.session_state.clone();
        let actor = SessionActor::new(config).start();
        lifecycle.recv().await.unwrap();
        let mut request = Box::pin(actor.send(AcceptRoutedPublish {
            packet: Packet::Publish(publish(1, None)),
        }));
        tokio::select! {
            _ = store.entered.notified() => {},
            result = &mut request => panic!("acknowledged before entering storage: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("delivery did not start"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut request)
                .await
                .is_err()
        );
        store.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(3), request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.is_err(), fail);
        assert_eq!(state.read().await.inflight.inner.contains_key(&1), !fail);
        actor.do_send(SessionActorMessage::ForceStop);
    }
}

#[actix::test]
async fn expired_unsent_cleanup_retries_after_raft_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(RocksDBPayloadStore::new(dir.path().join("payload")).unwrap());
    let (config, _lifecycle) = actor_config(dir.path(), store.clone()).await;
    let actor = SessionActor::new(config);
    let mut context = actor.outbound_delivery_context();
    store
        .put(
            &"key".into(),
            Bytes::from(serialize_stored_packet(&Packet::Publish(publish(1, Some(1)))).unwrap()),
        )
        .await
        .unwrap();
    {
        let mut state = context.session_state.write().await;
        state
            .inflight
            .register_with_tx_packet(1, 1, "key".into())
            .unwrap();
        state.inflight.inner.get_mut(&1).unwrap().delivery_started = false;
    }
    // An unavailable Raft must leave the record for another cleanup attempt.
    context.clean_session = false;
    assert!(context.cleanup_expired_unsent().await.is_empty());
    assert!(context
        .session_state
        .read()
        .await
        .inflight
        .inner
        .contains_key(&1));
    assert!(store.contains(&"key".into()).await.unwrap());
    // Exercise the same expiration detection and local cleanup with a working store.
    context.clean_session = true;
    assert!(context.cleanup_expired_unsent().await.contains(&1));
    assert_eq!(
        context
            .session_state
            .write()
            .await
            .inflight
            .allocate_packet_id(),
        Some(1)
    );
    assert!(!store.contains(&"key".into()).await.unwrap());
}
