use std::sync::Arc;

use log::{info, warn};
use tokio::signal;
use yedmq::{app::YedMQApp, settings::Settings};

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

    let app = Arc::new(YedMQApp::new(settings.clone()).await);

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
