use std::sync::Arc;
use actix::{Addr, SystemService};
use log::info;
use yedmq_plugin_host::plugin_manager::PluginManager;
use crate::arbiter_pool::ArbiterPool;
use crate::raft::session_actor_map::session_actor_map_raft_actor::{Initialize as InitializeSessionActorMapRaft, SessionActorMapRaftActor};
use crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor;
use crate::raft::topic::topic_raft_actor::TopicRaftActor;
use crate::router_actor::RouterActor;
use crate::rpc::rpc_actor::RpcActor;
use crate::session::session_manager_actor::{
    Initialize as InitializeSessionManager, SessionManagerActor, SetArbiterPool, SetRouterActor,
};
use crate::settings::Settings;
use crate::session::session_actor_map_storage::SessionClock;

#[derive(Clone)]
pub struct ServiceRegistry {
    pub router: Addr<RouterActor>,
    pub session_manager: Addr<SessionManagerActor>,
    pub rpc: Addr<RpcActor>,
    pub topic_raft: Addr<TopicRaftActor>,
    pub session_map_raft: Addr<SessionActorMapRaftActor>,
    pub session_state_raft: Addr<SessionStateRaftActor>,
}

impl ServiceRegistry {
    pub fn start(
        pools: Arc<ArbiterPool>,
        settings: Arc<Settings>,
        plugin_manager: Arc<PluginManager>,
        session_clock: Arc<SessionClock>
    ) -> Arc<Self> {
        info!("starting services");
        let topic_raft = TopicRaftActor::from_registry();
        let session_map_raft = SessionActorMapRaftActor::from_registry();
        let session_state_raft = SessionStateRaftActor::from_registry();
        let session_manager = SessionManagerActor::from_registry();

        session_manager.do_send(InitializeSessionManager {
            settings: settings.clone(),
            plugin_manager,
            session_clock: session_clock.clone(),
        });

        session_map_raft.do_send(InitializeSessionActorMapRaft {
            settings: settings.clone(),
            session_clock,
        });

        session_manager.do_send(SetArbiterPool{
            arbiter_pool: pools.clone(),
        });

        topic_raft.do_send(crate::raft::topic::topic_raft_actor::Initialize{
            settings: settings.clone(),
        });

        session_state_raft.do_send(crate::raft::session_state::session_state_raft_actor::Initialize{
            settings: settings.clone(),
        });

        let settings_clone = settings.clone();
        let session_manager_clone = session_manager.clone();

        let topic_raft_clone = topic_raft.clone();
        let router = pools.start_actor(|| {
            RouterActor::new(settings_clone, session_manager_clone, topic_raft_clone)
        });

        let router_clone = router.clone();

        let settings_clone = settings.clone();
        let rpc = pools.start_actor(|| {
            RpcActor::new(router_clone, settings_clone)
        });

        let router_clone = router.clone();
        session_manager.do_send(SetRouterActor{
            router_actor: router_clone
        });

        Arc::new(ServiceRegistry {
            router,
            session_manager,
            rpc,
            topic_raft,
            session_map_raft,
            session_state_raft,
        })

    }

}