use std::{collections::HashMap, sync::Arc};

use log::{info, warn};
use tokio::sync::{mpsc::Sender, Mutex, OnceCell, RwLock};

use crate::{
    listener::{tcp_listener::MqttTcpListener, tcp_tls_listener::MqttTcpTlsListener, ws_listener::MqttWsListener, wss_listener::MqttWssListener}, metric, plugin_manager::PluginManager, rest_api, router::{Router, RouterCmd}, session::session_manager::{SessionManager, SessionMessage}, settings::Settings, topic::TopicManager
};

// Representation of the application state.This struct can be shared around to share.
pub struct YedMQApp {
    pub session_manager: Arc<RwLock<SessionManager>>,

    pub plugin_manager: Arc<PluginManager>,

    pub topic_manager: Arc<RwLock<TopicManager>>,

    pub metric: Arc<metric::Metric>,

    pub settings: Arc<Settings>,

    pub join_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,

    pub router_sender: OnceCell<Sender<RouterCmd>>,

}

impl YedMQApp {
    pub async fn start(app: Arc<YedMQApp>) {

        let settings = app.settings.clone();

        let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

        //
        info!("start router task");

        let mut router = Router {
            topic_manager: app.topic_manager.clone(),
            session_manager: app.session_manager.clone(),
            router_receiver: router_receiver,
        };

        let router_join_handle = tokio::spawn(async move {
            router.run().await;
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
        let sys_topic_task_join_handle = tokio::spawn(async move {
            sys_topic_task.run().await;
        });
        info!("start sys topic task succeed");
        //

        // start api task
        let api_listen_external = settings.listener.api.external.clone();

        info!("start api task");
        let app_cloned = app.clone();
        tokio::spawn(async move {
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
        let tcp_listener_join = tokio::spawn(async move {
            info!(
                "start tcp listener on {}",
                settings_clone.listener.tcp.external
            );
            if let Err(e) = listener.run().await {
                warn!("tcp listener can`t run, error: {}", e);
            }
        });

        let mut tcp_tls_listener = MqttTcpTlsListener { app: app.clone() };

        let settings_clone = settings.clone();
        let tcp_tls_listener_join = tokio::spawn(async move {
            let settings = settings_clone.clone();
            info!(
                "start tls listener on {}",
                settings.listener.tcp_tls.external
            );
            if let Err(e) = tcp_tls_listener.run().await {
                warn!("tls listener can`t run, error: {}", e);
            }
        });

        let ws_listener = MqttWsListener { app: app.clone() };
        let settings_clone = settings.clone();
        let mqtt_ws_listener_join = tokio::spawn(async move {
            let settings = settings_clone.clone();
            info!("start ws listener on {}", settings.listener.ws.external);
            if let Err(e) = ws_listener.run().await {
                warn!("ws listener can`t run, error: {}", e);
            };
        });

        let wss_listener = MqttWssListener { app: app.clone() };
        let settings_clone = settings.clone();
        let mqtt_wss_listener_join = tokio::spawn(async move {
            let settings = settings_clone.clone();
            info!("start wss listener on {}", settings.listener.wss.external);
            if let Err(e) = wss_listener.run().await {
                warn!("wss listener can`t run, error: {}", e);
            }
        });

        let mut hn = app.join_handles.lock().await;

        hn.push(router_join_handle);
        hn.push(sys_topic_task_join_handle);
        hn.push(tcp_listener_join);
        hn.push(tcp_tls_listener_join);
        hn.push(mqtt_ws_listener_join);
        hn.push(mqtt_wss_listener_join);

    }

    pub fn new(settings: Arc<Settings>) -> Self {

        // init session manager
        let session_manager = Arc::new(RwLock::new(SessionManager {
            sessions: HashMap::<String, HashMap<String, Sender<SessionMessage>>>::new(),
        }));
        //

        // init plugin manager
        info!("start load plugin manager");
        let plugin_manager =
            PluginManager::new(settings.plugin.dir.clone(), settings.clone()).unwrap();
        let plugin_manager = Arc::new(plugin_manager);
        info!("plugin manager load succeed");
        //

        // init topic manager
        info!("start load topic manager");
        let topic_manager = Arc::new(RwLock::new(TopicManager::new()));
        info!("topic manager load succeed");
        //

        let metric = Arc::new(metric::Metric::new());

        let join_handles = Mutex::new(vec![]);

        YedMQApp {
            session_manager,
            plugin_manager,
            topic_manager,
            router_sender: OnceCell::new(),
            settings,
            metric,
            join_handles,
        }
    }
}
