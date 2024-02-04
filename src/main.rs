use std::{sync::Arc, collections::HashMap};

use log::info;
use crate::{listener::{ws_listener::MqttWsListener, wss_listener::MqttWssListener}, plugin_service::service::PluginService};
use settings::Settings;
use tokio::sync::RwLock;
use topic::TopicManager;

use crate::{session::{SessionManager, SessionHandle}, listener::{tcp_listener::MqttTcpListener, tcp_tls_listener::MqttTcpTlsListener}, router::Router};

mod protocol;
mod connection;
mod session;
mod inflight;
mod router;
mod topic;
mod settings;
mod listener;
mod plugin_service;

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
    let session_manager = Arc::new(RwLock::new(SessionManager{ session_table:  HashMap::<String,RwLock<HashMap<String, SessionHandle>>>::new()}));
    //

    // init plugin manager
    info!("start load plugin manager");

    let plugin_service = PluginService::new(settings.plugin.dir.clone()).unwrap();
    let plugin_service = Arc::new(plugin_service);
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


    let listener = MqttTcpListener {
        plugin_manager: plugin_service.clone(),
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender.clone(),
        settings: settings.clone(),
    };

    let settings_clone = settings.clone();
    let tcp_listener_join = tokio::spawn(async move {
        info!("start tcp listener on {}", settings_clone.listener.tcp.external);
        listener.run().await.unwrap();
    });

    let mut tcp_tls_listener = MqttTcpTlsListener {
        plugin_manager: plugin_service.clone(),
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender.clone(),
        settings: settings.clone(),
    };

    let settings_clone = settings.clone();
    let tcp_tls_listener_join = tokio::spawn(async move {
        let settings = settings_clone.clone();
        info!("start tls listener on {}", settings.listener.tcp_tls.external);
        tcp_tls_listener.run().await.unwrap();
    });


    let ws_listener = MqttWsListener {
        plugin_manager: plugin_service.clone(),
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender.clone(),
        settings: settings.clone(),
    };
    let settings_clone = settings.clone();
    let mqtt_ws_listener_join = tokio::spawn(async move {
        let settings = settings_clone.clone();
        info!("start ws listener on {}", settings.listener.ws.external);
        ws_listener.run().await.unwrap();
    });

    let wss_listener = MqttWssListener {
        plugin_manager: plugin_service.clone(),
        session_manager: session_manager.clone(),
        topic_manager: topic_manager.clone(),
        router_sender: router_sender.clone(),
        settings: settings.clone(),
    };
    let settings_clone = settings.clone();
    let mqtt_wss_listener_join = tokio::spawn(async move {
        let settings = settings_clone.clone();
        info!("start wss listener on {}", settings.listener.wss.external);
        wss_listener.run().await.unwrap();
    });

    tcp_listener_join.await.unwrap();
    tcp_tls_listener_join.await.unwrap();
    mqtt_ws_listener_join.await.unwrap();
    mqtt_wss_listener_join.await.unwrap();
    
}
