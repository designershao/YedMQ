use std::sync::Arc;

use log::info;
use tokio::sync::{watch, RwLock};

use super::{app::RaftApp, service::raft_service::RaftServiceImpl};
use crate::protobuf::raft_service_server::RaftServiceServer;

pub struct YedMQNode {
    pub running_rx: watch::Receiver<()>,
    pub app: Arc<RaftApp>,
    pub raft_joinhandle: RwLock<tokio::task::JoinHandle<std::result::Result<(), anyhow::Error>>>,
}

impl YedMQNode {
    pub async fn start_grpc(yedmq_node:Arc<YedMQNode>, addr: &str) -> anyhow::Result<()> {
        let mut rx = yedmq_node.running_rx.clone();

        let raft_service = RaftServiceImpl {
            app: yedmq_node.app.clone()
        };

        let addr_str = addr.to_string();
        let ret = addr_str.parse::<std::net::SocketAddr>();

        let addr = match ret {
            Ok(addr) => addr,
            Err(e) => {
                return Err(anyhow::anyhow!("parse address error: {}", e));
            },
        };

        let svc = RaftServiceServer::new(raft_service);
        let srv = tonic::transport::server::Server::builder().add_service(svc);

        let h = tokio::spawn(async move {
            srv.serve_with_shutdown(addr, async move {
                let _ = rx.changed().await;
                info!("signal receivbed, shutting down: id={} {}", addr, addr_str);
            }).await.map_err(|e| anyhow::anyhow!(e))?;
            Ok::<(), anyhow::Error>(())
        });

        let mut jh = yedmq_node.raft_joinhandle.write().await;

        *jh = h;

        Ok(())
    }
}