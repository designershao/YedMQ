use std::sync::Arc;

use app::YedMQApp;
use log::{info, warn};
use settings::Settings;
use tokio::signal;

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

    let app = Arc::new(YedMQApp::new(settings.clone()));

    YedMQApp::start(app).await;

    match signal::ctrl_c().await {
        Ok(()) => {
            info!("signal received, shutting down");
            // TODO: add shut down logic
        }
        Err(e) => {
            warn!("signal error: {}", e);
        }
    };
    
}
