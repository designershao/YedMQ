use std::net::SocketAddr;

use actix::prelude::*;
use actix::{Actor, Supervised, SystemService};
use log::error;
use tonic::transport::Server;

use crate::protobuf::cluster_service_server::ClusterServiceServer;
use crate::protobuf::raft_service_server::RaftServiceServer;
use crate::settings::Settings;

#[derive(Default)]
pub struct RpcActor {}

impl SystemService for RpcActor {
    fn service_started(&mut self, ctx: &mut Context<Self>) {
        let settings = match Settings::new() {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to load settings in RpcActor: {}", e);
                error!("RPC Actor will not start due to settings load failure.");
                std::process::exit(1);
            }
        };
        let cluster_rpc_external: SocketAddr = match settings.cluster.rpc.external.parse() {
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
        ctx.spawn(
            async move {
                let cluster_service = crate::rpc::cluster_service::ClusterServiceImpl {};
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

impl Supervised for RpcActor {}

impl Actor for RpcActor {
    type Context = Context<Self>;
}
