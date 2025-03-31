use std::{collections::BTreeMap, sync::Arc};

use actix::Actor;
use actix::Addr;
use log::{info, warn};
use tokio::sync::{mpsc::Sender, Mutex, OnceCell, RwLock};

use crate::session::session_actor_map_storage::SessionActorMapStorage;
use crate::session::session_state_storage::SessionStateStorage;
use crate::{
    listener::{
        tcp_listener::MqttTcpListener, tcp_tls_listener::MqttTcpTlsListener,
        ws_listener::MqttWsListener, wss_listener::MqttWssListener,
    },
    metric,
    plugin_manager::PluginManager,
    raft::Node,
    rest_api,
    router::{Router, RouterCmd},
    session::session_manager_actor::SessionManagerActor,
    settings::Settings,
    topic::{
        topic_manager::{TopicManager, TopicManagerTrait},
        topic_storage::TopicStorage,
    },
};

// Representation of the application state.This struct can be shared around to share.
pub struct YedMQApp {
    pub session_manager: OnceCell<Addr<SessionManagerActor>>,

    pub plugin_manager: Arc<PluginManager>,

    pub topic_manager: Arc<RwLock<dyn TopicManagerTrait>>,

    pub topic_storage: Arc<RwLock<TopicStorage>>,

    pub topic_router: Arc<RwLock<BTreeMap<String, Vec<Node>>>>,

    pub metric: Arc<metric::Metric>,

    pub settings: Arc<Settings>,

    pub join_handles: Mutex<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>>,

    pub router_sender: OnceCell<Sender<RouterCmd>>,

    pub raft_manager: Arc<crate::raft::raft_manager::RaftManager>,
}

impl YedMQApp {
    pub async fn start(app: Arc<YedMQApp>) {
        let settings = app.settings.clone();

        let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

        let session_state_storage = Arc::new(RwLock::new(SessionStateStorage::new()));

        app.raft_manager.init_session_state_raft(session_state_storage.clone()).await;

        app.raft_manager.init_topic_raft(app.topic_storage.clone()).await;

        let session_actor_map_storage = Arc::new(RwLock::new(SessionActorMapStorage::new()));

        //
        info!("start router task");

        // init session manager
        let session_manager = SessionManagerActor::new(
            app.plugin_manager.clone(),
            app.topic_manager.clone(),
            router_sender.clone(),
            app.settings.clone(),
            app.raft_manager.clone(),
        )
        .start();
        //

        app.raft_manager
            .init_session_actor_map_raft(
                session_actor_map_storage,
                session_manager.clone().recipient(),
            )
            .await;

        // start rpc server
        crate::raft::raft_manager::RaftManager::start_grpc(
            app.raft_manager.clone(),
            router_sender.clone(),
            session_manager.clone().recipient(),
            session_manager.clone().recipient()
        )
        .await
        .unwrap();
        info!("start grpc service succeed");
        //

        app.session_manager.set(session_manager.clone()).unwrap();

        let mut router = Router {
            topic_manager: app.topic_manager.clone(),
            session_manager_recipient: session_manager.recipient(),
            router_receiver: router_receiver,
            raft_manager: app.raft_manager.clone(),
        };

        let router_join_handle = actix::spawn(async move {
            router.run().await;
            Ok(())
        });
        info!("start router task succeed");

        app.router_sender.set(router_sender.clone()).unwrap();
        //

        // sys topic task
        info!("start sys topic task");
        let sys_topic_task = metric::SysTopicTask::new(
            app.metric.clone(),
            settings.mqtt.sys_topic_interval_secs,
            router_sender.clone(),
        );
        let sys_topic_task_join_handle = actix::spawn(async move {
            sys_topic_task.run().await;
            Ok(())
        });
        info!("start sys topic task succeed");
        //

        // start api task
        let api_listen_external = settings.listener.api.external.clone();

        info!("start api task");
        let app_cloned = app.clone();
        actix::spawn(async move {
            if let Err(e) =
                rest_api::run_rest_api_task(&api_listen_external, app_cloned.clone()).await
            {
                warn!("start api task error: {}", e);
            }
        });
        info!("start api task succeed");
        //

        let listener = MqttTcpListener { app: app.clone() };

        let settings_clone = settings.clone();
        let tcp_listener_join = actix::spawn(async move {
            info!(
                "start tcp listener on {}",
                settings_clone.listener.tcp.external
            );
            if let Err(e) = listener.run().await {
                warn!("tcp listener can`t run, error: {}", e);
            }
            Ok(())
        });

        let mut tcp_tls_listener = MqttTcpTlsListener { app: app.clone() };

        let settings_clone = settings.clone();
        let tcp_tls_listener_join = actix::spawn(async move {
            let settings = settings_clone.clone();
            info!(
                "start tls listener on {}",
                settings.listener.tcp_tls.external
            );
            if let Err(e) = tcp_tls_listener.run().await {
                warn!("tls listener can`t run, error: {}", e);
            }
            Ok(())
        });

        let ws_listener = MqttWsListener { app: app.clone() };
        let settings_clone = settings.clone();
        let mqtt_ws_listener_join = actix::spawn(async move {
            let settings = settings_clone.clone();
            info!("start ws listener on {}", settings.listener.ws.external);
            if let Err(e) = ws_listener.run().await {
                warn!("ws listener can`t run, error: {}", e);
            };
            Ok(())
        });

        let wss_listener = MqttWssListener { app: app.clone() };
        let settings_clone = settings.clone();
        let mqtt_wss_listener_join = actix::spawn(async move {
            let settings = settings_clone.clone();
            info!("start wss listener on {}", settings.listener.wss.external);
            if let Err(e) = wss_listener.run().await {
                warn!("wss listener can`t run, error: {}", e);
            }
            Ok(())
        });

        //

        let mut hn = app.join_handles.lock().await;

        hn.push(router_join_handle);
        hn.push(sys_topic_task_join_handle);
        hn.push(tcp_listener_join);
        hn.push(tcp_tls_listener_join);
        hn.push(mqtt_ws_listener_join);
        hn.push(mqtt_wss_listener_join);
    }

    pub async fn new(settings: Arc<Settings>) -> Self {
        // init plugin manager
        info!("start load plugin manager");
        let plugin_manager =
            PluginManager::new(settings.plugin.dir.clone(), settings.clone()).unwrap();
        let plugin_manager = Arc::new(plugin_manager);
        info!("plugin manager load succeed");
        //

        let metric = Arc::new(metric::Metric::new());

        let join_handles = Mutex::new(vec![]);

        let topic_storage = Arc::new(RwLock::new(TopicStorage::new()));

        // init raft manager
        let raft_manager = Arc::new(
            crate::raft::raft_manager::RaftManager::new(
                settings.cluster.clone(),
            )
            .await,
        );
        //

        // init topic manager
        info!("start load topic manager");
        let topic_manager = Arc::new(RwLock::new(TopicManager::new(
            topic_storage.clone(),
            raft_manager.clone(),
            settings.cluster.node_id,
        )));
        info!("topic manager load succeed");
        //

        YedMQApp {
            session_manager: OnceCell::new(),
            plugin_manager,
            topic_manager,
            raft_manager,
            router_sender: OnceCell::new(),
            settings,
            metric,
            join_handles,
            topic_storage,
            topic_router: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}
