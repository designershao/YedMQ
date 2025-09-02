use actix::{Actor, Supervised, SystemService};
use actix::prelude::*;
use tonic::transport::Server;

use crate::protobuf::cluster_service_server::ClusterServiceServer;
use crate::protobuf::raft_service_server::RaftServiceServer;
use crate::settings::Settings;

pub struct RpcActor {}

impl Default for RpcActor {
    fn default() -> Self {
        RpcActor {}
    }
}

impl SystemService for RpcActor {
    fn service_started(&mut self, ctx: &mut Context<Self>) {
        let settings = Settings::new().unwrap();
        ctx.spawn(
            async move {
                let cluster_service = crate::rpc::cluster_service::ClusterServiceImpl{};
                let rpc_service = crate::rpc::raft_service::RustServiceImpl{};
                Server::builder()
                    .add_service(ClusterServiceServer::new(cluster_service))
                    .add_service(RaftServiceServer::new(rpc_service))
                    .serve(settings.cluster.rpc.external.parse().unwrap())
                    .await;
            }.into_actor(self)
        );
    }
}

impl Supervised for RpcActor {}

impl Actor for RpcActor {
    type Context = Context<Self>;
}

