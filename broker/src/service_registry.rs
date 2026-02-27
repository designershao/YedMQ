use crate::arbiter_pool::ArbiterPool;
use crate::node_resolver::NodeResolver;
use crate::raft::session_actor_map::session_actor_map_raft_actor::{
    Initialize as InitializeSessionActorMapRaft, SessionActorMapRaftActor,
};
use crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor;
use crate::raft::topic::topic_raft_actor::TopicRaftActor;
use crate::router_actor::RouterActor;
use crate::rpc::rpc_actor::RpcActor;
use crate::session::session_actor_map_storage::SessionClock;
use crate::session::session_manager_actor::{
    Initialize as InitializeSessionManager, SessionManagerActor, SetRouterActors,
};
use crate::settings::Settings;
use actix::{Addr, SystemService};
use log::info;
use std::sync::Arc;
use std::time::Duration;
use yedmq_plugin_host::plugin_manager::PluginManager;

use crate::session::session_registry::SessionRegistry;
use crate::timer_actor::TimerActor;

#[derive(Clone)]
pub struct ServiceRegistry {
    pub routers: Vec<Addr<RouterActor>>,
    pub session_manager: Addr<SessionManagerActor>,
    pub rpc: Addr<RpcActor>,
    pub topic_raft: Addr<TopicRaftActor>,
    pub session_map_raft: Addr<SessionActorMapRaftActor>,
    pub session_state_raft: Addr<SessionStateRaftActor>,
    pub node_resolver: Arc<NodeResolver>,
}

impl ServiceRegistry {
    pub async fn start(
        pools: Arc<ArbiterPool>,
        settings: Arc<Settings>,
        plugin_manager: Arc<PluginManager>,
        session_clock: Arc<SessionClock>,
        metric: Arc<crate::metric::Metric>,
    ) -> Arc<Self> {
        info!("starting services");

        // Initialize PayloadStore
        let payload_store_path = std::path::Path::new(&settings.cluster.store_dir).join("payload");
        let payload_store = Arc::new(
            crate::raft::payload::RocksDBPayloadStore::new(payload_store_path)
                .expect("failed to initialize payload store"),
        );

        let topic_raft = TopicRaftActor::from_registry();
        let session_map_raft = SessionActorMapRaftActor::from_registry();
        let session_state_raft = SessionStateRaftActor::from_registry();
        let session_manager = SessionManagerActor::from_registry();

        let timer_actor = pools.start_actor(TimerActor::new);

        let session_registry = SessionRegistry::new();
        let node_resolver = Arc::new(NodeResolver::new(settings.clone()));

        session_manager.do_send(InitializeSessionManager {
            settings: settings.clone(),
            plugin_manager,
            session_clock: session_clock.clone(),
            session_registry: session_registry.clone(),
            payload_store: payload_store.clone(),
            timer_actor,
            metric: metric.clone(),
            arbiter_pool: pools.clone(),
            node_resolver: node_resolver.clone(),
        });

        session_map_raft.do_send(InitializeSessionActorMapRaft {
            settings: settings.clone(),
            session_clock,
        });

        topic_raft.do_send(crate::raft::topic::topic_raft_actor::Initialize {
            settings: settings.clone(),
        });

        session_state_raft.do_send(
            crate::raft::session_state::session_state_raft_actor::Initialize {
                settings: settings.clone(),
                payload_store: payload_store.clone(),
            },
        );

        tokio::time::sleep(Duration::from_secs(1)).await;

        let get_topic_storage_res = topic_raft
            .send(crate::raft::topic::topic_raft_actor::GetTopicStorage {})
            .await
            .expect("get topic storage error");

        let get_session_actor_map_storage_res = session_map_raft.send(crate::raft::session_actor_map::session_actor_map_raft_actor::GetSessionActorMapStorage{}).await.expect("get session actor map storage error");

        let topic_storage = get_topic_storage_res.topic_storage.clone();
        let session_actor_map_storage = get_session_actor_map_storage_res
            .session_actor_map_storage
            .clone();

        let num_cpus = num_cpus::get();
        let num_routers = std::cmp::max(1, num_cpus.saturating_sub(2));

        let mut router_actors = Vec::new();
        for _ in 0..num_routers {
            let settings_clone = settings.clone();
            let session_manager_clone = session_manager.clone();
            let topic_raft_clone = topic_raft.clone();
            let node_resolver_clone = node_resolver.clone();
            let topic_storage = topic_storage.clone();
            let session_actor_map_storage = session_actor_map_storage.clone();
            let session_registry_clone = session_registry.clone();
            let metric_clone = metric.clone();

            let router = pools.start_actor(move || {
                RouterActor::new(
                    settings_clone,
                    session_manager_clone,
                    topic_raft_clone,
                    node_resolver_clone,
                    topic_storage,
                    session_actor_map_storage,
                    session_registry_clone,
                    metric_clone,
                )
            });
            router_actors.push(router);
        }

        info!("started {} router actors", num_routers);

        let router_actors_clone = router_actors.clone();

        let settings_clone = settings.clone();
        let payload_store_clone = payload_store.clone();
        let rpc = pools.start_actor(move || {
            RpcActor::new(router_actors_clone, settings_clone, payload_store_clone)
        });

        let router_actors_clone = router_actors.clone();
        session_manager.do_send(SetRouterActors {
            router_actors: router_actors_clone,
        });

        Arc::new(ServiceRegistry {
            routers: router_actors,
            session_manager,
            rpc,
            topic_raft,
            session_map_raft,
            session_state_raft,
            node_resolver,
        })
    }
}
