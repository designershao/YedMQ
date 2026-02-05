use std::sync::Arc;
use log::{error, info, warn};
use tokio::sync::Mutex;
use yedmq_plugin_host::plugin_manager::PluginManager;

use crate::arbiter_pool::ArbiterPool;
use crate::protobuf::cluster_service_client::ClusterServiceClient;
use crate::raft::NodeId;
use crate::service_registry::ServiceRegistry;
use crate::{
    listener::{
        tcp_listener::MqttTcpListener, tcp_tls_listener::MqttTcpTlsListener,
        ws_listener::MqttWsListener, wss_listener::MqttWssListener,
    },
    metric, rest_api,
    session::session_actor_map_storage::SessionClock,
    settings::Settings,
};

// Representation of the application state.This struct can be shared around to share.
pub struct YedMQApp {
    pub plugin_manager: Arc<PluginManager>,

    pub service_registry: Arc<ServiceRegistry>,

    pub metric: Arc<metric::Metric>,

    pub settings: Arc<Settings>,

    pub session_clock: Arc<SessionClock>,

    pub join_handles: Mutex<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>>,

    arbiter_pool: Arc<ArbiterPool>
}

impl YedMQApp {

    pub async fn get_cluster_service_rpc_client(&self, node_id: &NodeId) -> Option<ClusterServiceClient<tonic::transport::Channel>> {
        let nodes = self
            .service_registry
            .session_map_raft
            .send(crate::raft::session_actor_map::session_actor_map_raft_actor::GetClusterNodes)
            .await.expect("Session actor map is not ready");

        if let Ok(nodes) = nodes {
            match nodes.get(node_id) {
                Some(node) => {
                    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", node.rpc_addr.clone()))
                        .expect("invalid rpc address")
                        .connect_timeout(std::time::Duration::from_secs(5));

                    match endpoint.connect().await {
                        Ok(channel) => Some(ClusterServiceClient::new(channel)),
                        Err(e) => {
                            warn!("failed to connect to rpc client {}: {}", node_id, e);
                            None
                        }
                    }
                }
                None => {
                    warn!("node id {} not found in cluster nodes", node_id);
                    None
                }
            }
        } else {
            error!("Session acotor map get cluster nodes error: {:?}", nodes.err());
            None
        }

    }

    pub async fn start(app: Arc<YedMQApp>) {
        let settings = app.settings.clone();

        // sys topic task
        info!("start sys topic task");
        let mut sys_topic_task =
            metric::SysTopicTask::new(app.metric.clone(), settings.mqtt.sys_topic_interval_secs);
        if let Some(router) = app.service_registry.routers.first() {
            sys_topic_task.set_router_actor(router.clone());
        }
        let sys_topic_task_join_handle = actix::spawn(async move {
            sys_topic_task.run().await;
            Ok(())
        });
        info!("start sys topic task succeed");
        //

        // start api task
        let api_listen_external = settings.listener.api.external.clone();

        info!("start api task");
        let app_cloned = app.clone();
        actix::spawn(async move {
            if let Err(e) =
                rest_api::run_rest_api_task(&api_listen_external, app_cloned.clone()).await
            {
                warn!("start api task error: {}", e);
            }
        });
        info!("start api task succeed");
        //

        let listener = MqttTcpListener {
            app: app.clone(),
            arbiter_pool: app.arbiter_pool.clone(),
        };

        let settings_clone = settings.clone();
        let tcp_listener_join = actix::spawn(async move {
            info!(
                "start tcp listener on {}",
                settings_clone.listener.tcp.external
            );
            if let Err(e) = listener.run().await {
                warn!("tcp listener can`t run, error: {}", e);
            }
            Ok(())
        });

        let mut tcp_tls_listener = MqttTcpTlsListener {
            app: app.clone(),
            arbiter_pool: app.arbiter_pool.clone(),
        };

        let settings_clone = settings.clone();
        let tcp_tls_listener_join = actix::spawn(async move {
            let settings = settings_clone.clone();
            info!(
                "start tls listener on {}",
                settings.listener.tcp_tls.external
            );
            if let Err(e) = tcp_tls_listener.run().await {
                warn!("tls listener can`t run, error: {}", e);
            }
            Ok(())
        });

        let ws_listener = MqttWsListener {
            app: app.clone(),
            arbiter_pool: app.arbiter_pool.clone(),
        };
        let settings_clone = settings.clone();
        let mqtt_ws_listener_join = actix::spawn(async move {
            let settings = settings_clone.clone();
            info!("start ws listener on {}", settings.listener.ws.external);
            if let Err(e) = ws_listener.run().await {
                warn!("ws listener can`t run, error: {}", e);
            };
            Ok(())
        });

        let wss_listener = MqttWssListener {
            app: app.clone(),
            arbiter_pool: app.arbiter_pool.clone(),
        };
        let settings_clone = settings.clone();
        let mqtt_wss_listener_join = actix::spawn(async move {
            let settings = settings_clone.clone();
            info!("start wss listener on {}", settings.listener.wss.external);
            if let Err(e) = wss_listener.run().await {
                warn!("wss listener can`t run, error: {}", e);
            }
            Ok(())
        });

        //

        let mut hn = app.join_handles.lock().await;

        hn.push(sys_topic_task_join_handle);
        hn.push(tcp_listener_join);
        hn.push(tcp_tls_listener_join);
        hn.push(mqtt_ws_listener_join);
        hn.push(mqtt_wss_listener_join);
    }

    pub async fn new(settings: Arc<Settings>) -> Self {
        // init plugin manager
        info!("start load plugin manager");

        let plugin_host_config = yedmq_plugin_host::plugin_host_config::PluginHostConfig {
            broker_version: "0.1.0".to_string(),
            broker_node_id: settings.cluster.node_id as u32,
            cluster_name: "yedmq_cluster".to_string(),
            plugin_directory: settings.plugin.dir.clone(),
            local_socket_path: settings.plugin.local_socket_path.clone(),
            max_restart_attempts: 5,
            health_check_interval_secs: 10,
            shutdown_signal: tokio::sync::broadcast::channel(1).0,
            default_authorize_result: settings.plugin.default_authorize_result,
            default_authenticate_result: settings.plugin.default_authenticate_result,
        };

        let mut plugin_manager = PluginManager::new(plugin_host_config).await.expect("plugin manager init failed");

        match plugin_manager.start_listener().await {
            Ok(_) => info!("plugin manager listener start succeed"),
            Err(e) => panic!("plugin manager listener start failed: {}", e),
        }

        plugin_manager.start_heartbeat_check_task().await;

        info!("plugin manager load succeed");

        let plugin_manager = Arc::new(plugin_manager);

        match plugin_manager.start_all_plugins().await {
            Ok(_) => info!("all plugins started succeed"),
            Err(e) => panic!("start all plugins failed: {}", e),
        }

        let session_clock = SessionClock::new(
            settings.cluster.node_id,
            settings.session.session_clock_path.clone(),
        );
        session_clock.restore().await.unwrap();
        let session_clock = Arc::new(session_clock);

        let metric = Arc::new(metric::Metric::new());

        let join_handles = Mutex::new(vec![]);

        // start system service
        let arbiter_pool = ArbiterPool::new("app", num_cpus::get());
        let service_registry = ServiceRegistry::start(
            arbiter_pool.clone(),
            settings.clone(),
            plugin_manager.clone(),
            session_clock.clone(),
            metric.clone(),
        ).await;
        //


        // init raft manager
        YedMQApp {
            plugin_manager,
            settings,
            metric,
            session_clock,
            join_handles,
            service_registry,
            arbiter_pool: arbiter_pool.clone(),
        }
    }
}
