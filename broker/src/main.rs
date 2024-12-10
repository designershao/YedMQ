use std::{sync::Arc, collections::HashMap};

use log::{info, warn};
use plugin_manager::PluginManager;
use crate::listener::{ws_listener::MqttWsListener, wss_listener::MqttWssListener};
use settings::Settings;
use tokio::{sync::mpsc::Sender, sync::RwLock};
use topic::TopicManager;

use crate::{session::session_manager::{SessionMessage, SessionManager}, listener::{tcp_listener::MqttTcpListener, tcp_tls_listener::MqttTcpTlsListener}, router::Router};

mod connection;
mod session;
mod inflight;
mod router;
mod topic;
mod settings;
mod listener;
mod plugin_manager;
mod metric;
mod rest_api;

#[tokio::main]
async fn main() {
    env_logger::init();

    // init setting 
    let s = Settings::new();
    if let Err(e) = s {
        info!("load settings error: {}", e);
        return;
    }

    let settings =Arc::new(s.unwrap());
    //

    // init session manager
    let session_manager = Arc::new(RwLock::new(SessionManager{ sessions:  HashMap::<String,HashMap<String, Sender<SessionMessage>>>::new()}));
    //

    // init plugin manager
    info!("start load plugin manager");
    let plugin_manager = PluginManager::new(settings.plugin.dir.clone()).unwrap();
    let plugin_manager = Arc::new(plugin_manager);
    info!("plugin manager load succeed");
    //

    // init topic manager
    info!("start load topic manager");
    let topic_manager = Arc::new(RwLock::new(TopicManager::new()));
    info!("topic manager load succeed");
    //

    //
    info!("start router task");
    let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

    let mut router = Router {
        topic_manager: topic_manager.clone(),
        session_manager: session_manager.clone(),
        router_receiver: router_receiver,
    };

    tokio::spawn(async move {
        router.run().await;
    });
    info!("start router task succeed");
    //


    let metric = Arc::new(metric::Metric::new());

    // sys topic task
    info!("start sys topic task");
    let sys_topic_task = metric::SysTopicTask::new(metric.clone(), settings.mqtt.sys_topic_interval_secs, router_sender.clone());
    tokio::spawn(async move {
        sys_topic_task.run().await;
    });
    info!("start sys topic task succeed");
    //

    // start api task
    info!("start api task");
    let plugin_manager_cloned = plugin_manager.clone();
    let api_listen_external = settings.listener.api.external.clone();
    tokio::spawn(async move {
        if let Err(e) =rest_api::run_rest_api_task(&api_listen_external, plugin_manager_cloned).await {
            warn!("start api task error: {}", e);
        }
    });
    info!("start api task succeed");
    //

    let listener = MqttTcpListener {
        plugin_manager: plugin_manager.clone(),
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender.clone(),
        settings: settings.clone(),
        metric: metric.clone(),
    };

    let settings_clone = settings.clone();
    let tcp_listener_join = tokio::spawn(async move {
        info!("start tcp listener on {}", settings_clone.listener.tcp.external);
        if let Err(e) = listener.run().await {
            warn!("tcp listener can`t run, error: {}", e);
        }
    });

    let mut tcp_tls_listener = MqttTcpTlsListener {
        plugin_manager: plugin_manager.clone(),
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender.clone(),
        settings: settings.clone(),
        metric: metric.clone(),
    };

    let settings_clone = settings.clone();
    let tcp_tls_listener_join = tokio::spawn(async move {
        let settings = settings_clone.clone();
        info!("start tls listener on {}", settings.listener.tcp_tls.external);
        if let Err(e) = tcp_tls_listener.run().await {
            warn!("tls listener can`t run, error: {}", e);
        }
    });


    let ws_listener = MqttWsListener {
        plugin_manager: plugin_manager.clone(),
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender.clone(),
        settings: settings.clone(),
        metric: metric.clone(),
    };
    let settings_clone = settings.clone();
    let mqtt_ws_listener_join = tokio::spawn(async move {
        let settings = settings_clone.clone();
        info!("start ws listener on {}", settings.listener.ws.external);
        if let Err(e) = ws_listener.run().await {
            warn!("ws listener can`t run, error: {}", e);
        };
    });

    let wss_listener = MqttWssListener {
        plugin_manager: plugin_manager.clone(),
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender.clone(),
        settings: settings.clone(),
        metric: metric.clone(),
    };
    let settings_clone = settings.clone();
    let mqtt_wss_listener_join = tokio::spawn(async move {
        let settings = settings_clone.clone();
        info!("start wss listener on {}", settings.listener.wss.external);
        if let Err(e) = wss_listener.run().await {
            warn!("wss listener can`t run, error: {}", e);
        }
    });

    tcp_listener_join.await.unwrap();
    tcp_tls_listener_join.await.unwrap();
    mqtt_ws_listener_join.await.unwrap();
    mqtt_wss_listener_join.await.unwrap();
    
}
