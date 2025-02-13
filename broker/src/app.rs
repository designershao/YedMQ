use std::{collections::HashMap, sync::Arc};

use log::info;
use tokio::sync::{mpsc::Sender, RwLock};

use crate::{
    metric, plugin_manager::PluginManager, router::{Router, RouterCmd}, session::session_manager::{SessionManager, SessionMessage}, settings::Settings, topic::TopicManager
};

// Representation of the application state.This struct can be shared around to share.
pub struct YedMQApp {
    pub session_manager: Arc<RwLock<SessionManager>>,

    pub plugin_manager: Arc<PluginManager>,

    pub topic_manager: Arc<RwLock<TopicManager>>,

    pub router_sender: Sender<RouterCmd>,

    pub metric: Arc<metric::Metric>,

    pub settings: Arc<Settings>,
}

impl YedMQApp {
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
        let sys_topic_task = metric::SysTopicTask::new(
            metric.clone(),
            settings.mqtt.sys_topic_interval_secs,
            router_sender.clone(),
        );
        tokio::spawn(async move {
            sys_topic_task.run().await;
        });
        info!("start sys topic task succeed");
        //

        YedMQApp {
            session_manager,
            plugin_manager,
            topic_manager,
            router_sender,
            settings,
            metric
        }
    }
}
