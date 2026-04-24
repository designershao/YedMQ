use std::net::SocketAddr;
use std::sync::Arc;

use actix::prelude::*;
use actix::Actor;
use log::error;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::protobuf::cluster_service_server::ClusterServiceServer;
use crate::protobuf::raft_service_server::RaftServiceServer;
use crate::router_actor::RouterActor;
use crate::settings::Settings;

pub struct RpcActor {
    router_actors: Vec<Addr<RouterActor>>,
    settings: Arc<Settings>,
    payload_store: Arc<dyn crate::raft::payload::PayloadStore>,
    ready_tx: Option<oneshot::Sender<Result<(), String>>>,
}

impl RpcActor {
    pub fn new(
        router_actors: Vec<Addr<RouterActor>>,
        settings: Arc<Settings>,
        payload_store: Arc<dyn crate::raft::payload::PayloadStore>,
        ready_tx: oneshot::Sender<Result<(), String>>,
    ) -> Self {
        RpcActor {
            router_actors,
            settings,
            payload_store,
            ready_tx: Some(ready_tx),
        }
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
        let router_actors = self.router_actors.clone();
        let payload_store = self.payload_store.clone();
        let ready_tx = self.ready_tx.take();
        ctx.spawn(
            async move {
                let cluster_service = crate::rpc::cluster_service::ClusterServiceImpl {
                    router_actors
                };
                let rpc_service = crate::rpc::raft_service::RustServiceImpl {
                    store: payload_store.clone()
                };
                let payload_service = crate::raft::payload::PayloadServiceImpl::new(payload_store);
                let listener = match tokio::net::TcpListener::bind(cluster_rpc_external).await {
                    Ok(listener) => {
                        log::info!("RPC Server listening on {}", cluster_rpc_external);
                        if let Some(ready_tx) = ready_tx {
                            let _ = ready_tx.send(Ok(()));
                        }
                        listener
                    }
                    Err(e) => {
                        let message = format!(
                            "RPC Server failed to bind on {}: {}",
                            cluster_rpc_external, e
                        );
                        error!("{}", message);
                        if let Some(ready_tx) = ready_tx {
                            let _ = ready_tx.send(Err(message));
                        }
                        std::process::exit(1);
                    }
                };

                match Server::builder()
                    .add_service(ClusterServiceServer::new(cluster_service))
                    .add_service(RaftServiceServer::new(rpc_service))
                    .add_service(crate::protobuf::raft_payload::payload_service_server::PayloadServiceServer::new(payload_service))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await {
                    Ok(_) => {
                        log::info!(
                            "RPC Server stopped on {}",
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
