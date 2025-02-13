use std::{sync::Arc, collections::HashMap};

use app::YedMQApp;
use log::{info, warn};
use crate::listener::{ws_listener::MqttWsListener, wss_listener::MqttWssListener};
use settings::Settings;
use tokio::{sync::mpsc::Sender, sync::RwLock};
use topic::TopicManager;

use crate::{session::session_manager::{SessionMessage, SessionManager}, listener::{tcp_listener::MqttTcpListener, tcp_tls_listener::MqttTcpTlsListener}, router::Router};

mod app;
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
mod raft;

pub mod protobuf {
    tonic::include_proto!("openraftpb");
}

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

    let app = Arc::new(YedMQApp::new(settings.clone()));

    // start api task
    let api_listen_external = settings.listener.api.external.clone();

    info!("start api task");
    let app_ = app.clone();
    tokio::spawn(async move {
        if let Err(e) =rest_api::run_rest_api_task(
            &api_listen_external, 
            app_
        ).await {
            warn!("start api task error: {}", e);
        }
    });
    info!("start api task succeed");
    //

    let listener = MqttTcpListener {
        app: app.clone()
    };

    let settings_clone = settings.clone();
    let tcp_listener_join = tokio::spawn(async move {
        info!("start tcp listener on {}", settings_clone.listener.tcp.external);
        if let Err(e) = listener.run().await {
            warn!("tcp listener can`t run, error: {}", e);
        }
    });

    let mut tcp_tls_listener = MqttTcpTlsListener {
        app: app.clone()
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
        app: app.clone()
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
        app: app.clone()
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
