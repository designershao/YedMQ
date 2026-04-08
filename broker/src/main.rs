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

    YedMQApp::start(app.clone()).await;

    match signal::ctrl_c().await {
        Ok(()) => {
            info!("signal received, shutting down");
            if let Err(e) = app.shutdown().await {
                warn!("shutdown error: {}", e);
            }
            System::current().stop();
        }
        Err(e) => {
            warn!("signal error: {}", e);
            if let Err(shutdown_error) = app.shutdown().await {
                warn!("shutdown error: {}", shutdown_error);
            }
            System::current().stop();
        }
    };
}
