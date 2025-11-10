use std::sync::Arc;

use tokio::sync::OnceCell;
use yedmq_plugin_host::plugin_manager::PluginManager;

use crate::session::session_actor_map_storage::SessionClock;



static PLUGIN_MANAGER: OnceCell<Arc<PluginManager>> = OnceCell::const_new();
static SESSION_CLOCK: OnceCell<Arc<SessionClock>> = OnceCell::const_new();

pub fn init_plugin_manager(plugin_manager: Arc<PluginManager>) {
    if PLUGIN_MANAGER.set(plugin_manager).is_err() {
        panic!("PluginManager already initialized");
    }
}

pub fn get_plugin_manager() -> Arc<PluginManager> {
    PLUGIN_MANAGER
        .get()
        .expect("PluginManager not initialized")
        .clone()
}

pub async fn init_session_clock(settings: &crate::settings::Settings) {
    let session_clock = SessionClock::new(
        settings.cluster.node_id,
        settings.session.session_clock_path.clone(),
    );
    session_clock.restore().await.unwrap();
}

pub fn get_session_clock() -> Arc<SessionClock> {
    SESSION_CLOCK
        .get()
        .expect("SessionClock not initialized")
        .clone()
}
