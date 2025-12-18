use std::net::SocketAddr;
use std::sync::Arc;

use actix::prelude::*;
use actix::Actor;
use log::error;
use tonic::transport::Server;

use crate::protobuf::cluster_service_server::ClusterServiceServer;
use crate::protobuf::raft_service_server::RaftServiceServer;
use crate::router_actor::RouterActor;
use crate::settings::Settings;

pub struct RpcActor {
    router_actor: Addr<RouterActor>,
    settings: Arc<Settings>,
}

impl RpcActor {
    pub fn new(router_actor: Addr<RouterActor>, settings: Arc<Settings>) -> Self {
        RpcActor { router_actor, settings }
    }
}

impl Actor for RpcActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        let cluster_rpc_external: SocketAddr = match self.settings.cluster.rpc.external.parse() {
            Ok(s) => s,
            Err(e) => {
                error!(
                    "Failed to parse cluster.rpc.external address in RpcActor: {}",
                    e
                );
                error!("RPC Actor will not start due to invalid address.");
                std::process::exit(1);
            }
        };
        let router_actor = self.router_actor.clone();
        ctx.spawn(
            async move {
                let cluster_service = crate::rpc::cluster_service::ClusterServiceImpl {
                    router_actor
                };
                let rpc_service = crate::rpc::raft_service::RustServiceImpl {};
                match Server::builder()
                    .add_service(ClusterServiceServer::new(cluster_service))
                    .add_service(RaftServiceServer::new(rpc_service))
                    .serve(cluster_rpc_external)
                    .await {
                    Ok(_) => {
                        log::info!(
                            "RPC Server started successfully on {}",
                            cluster_rpc_external
                        );
                    }
                    Err(e) => {
                        error!("RPC Server failed to start on {}: {}", cluster_rpc_external, e);
                        std::process::exit(1);
                    }
                }
            }
                .into_actor(self),
        );
    }
}
