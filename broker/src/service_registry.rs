use std::sync::Arc;
use actix::{Addr, SystemService};
use log::info;
use crate::arbiter_pool::ArbiterPool;
use crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftActor;
use crate::raft::session_state::session_state_raft_actor::SessionStateRaftActor;
use crate::raft::topic::topic_raft_actor;
use crate::raft::topic::topic_raft_actor::TopicRaftActor;
use crate::router_actor::RouterActor;
use crate::rpc::rpc_actor::RpcActor;
use crate::session::session_manager_actor::{SessionManagerActor, SetRouterActor};
use crate::settings::Settings;

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
    pub fn start(pools: Arc<ArbiterPool>, settings: Arc<Settings>) -> Arc<Self> {
        info!("starting services");
        let topic_raft = TopicRaftActor::from_registry();
        let session_map_raft = SessionActorMapRaftActor::from_registry();
        let session_state_raft = SessionStateRaftActor::from_registry();

        let session_manager = pools.start_actor(|| {
            SessionManagerActor::default()
        });

        let settings_clone = settings.clone();

        let router = pools.start_actor(|| {
            RouterActor::new(settings_clone)
        });

        let router_clone = router.clone();

        let rpc = pools.start_actor(|| {
            RpcActor::new(router_clone)
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