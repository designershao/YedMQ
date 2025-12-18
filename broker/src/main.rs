use std::sync::Arc;

use actix::System;
use log::{info, warn};
use tokio::signal;
use yedmq::{app::YedMQApp, settings::Settings};

#[actix::main]
async fn main() {
    env_logger::init();

    // init setting 
    let settings = match Settings::new() {
        Ok(s) => Arc::new(s),
        Err(e) => {
            info!("load settings error: {}", e);
            return;
        }
    };

    let app = Arc::new(YedMQApp::new(settings.clone()).await);

    YedMQApp::start(app).await;

    match signal::ctrl_c().await {
        Ok(()) => {
            info!("signal received, shutting down");
            // TODO: add shut down logic
        }
        Err(e) => {
            warn!("signal error: {}", e);
            System::current().stop();
        }
    };
    
}
