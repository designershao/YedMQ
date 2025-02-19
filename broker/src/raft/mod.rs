use std::{fmt::Display, io::Cursor, path::Path, sync::Arc};

use log::info;
use network::raft_network_impl::Network;
use openraft::Config;
use serde::{Deserialize, Serialize};
use service::raft_service::RaftServiceImpl;
use store::{new_storage, Request, Response};

use crate::{app::YedMQApp, protobuf::raft_service_server::RaftServiceServer};

pub mod network;
pub mod raft_node;
pub mod service;
pub mod store;

pub type NodeId = u64;

pub type YedMQRaft = openraft::Raft<TypeConfig>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Node {
    pub rpc_addr: String,
    pub api_addr: String,
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node {{ rpc_addr: {}, api_addr: {} }}",
            self.rpc_addr, self.api_addr
        )
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        Node = Node
);

pub type SnapshotData = Cursor<Vec<u8>>;

pub mod typ {

    use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

    use super::{NodeId, TypeConfig};

    pub type Entry = openraft::Entry<TypeConfig>;

    impl tonic::IntoRequest<crate::protobuf::AppendEntriesRequest>
        for AppendEntriesRequest<TypeConfig>
    {
        fn into_request(self) -> tonic::Request<crate::protobuf::AppendEntriesRequest> {
            let mes = crate::protobuf::AppendEntriesRequest {
                data: serde_json::to_string(&self).expect("fail to serialize"),
            };
            tonic::Request::new(mes)
        }
    }

    impl tonic::IntoRequest<crate::protobuf::InstallSnapshotRequest>
        for InstallSnapshotRequest<TypeConfig>
    {
        fn into_request(self) -> tonic::Request<crate::protobuf::InstallSnapshotRequest> {
            let mes = crate::protobuf::InstallSnapshotRequest {
                data: serde_json::to_string(&self).expect("fail to serialize"),
            };
            tonic::Request::new(mes)
        }
    }

    impl tonic::IntoRequest<crate::protobuf::VoteRequest> for VoteRequest<NodeId> {
        fn into_request(self) -> tonic::Request<crate::protobuf::VoteRequest> {
            let mes = crate::protobuf::VoteRequest {
                data: serde_json::to_string(&self).expect("fail to serialize"),
            };
            tonic::Request::new(mes)
        }
    }
}

pub async fn start_raft_node(app: Arc<YedMQApp>) -> anyhow::Result<()> {
    let heartbeat_interval = app.settings.cluster.heartbeat_interval as u64;
    let election_timeout_min = heartbeat_interval * 2;
    let config = Config {
        heartbeat_interval,
        election_timeout_min,
        ..Default::default()
    };

    let dir = Path::new(&app.settings.cluster.store_dir);

    let config = Arc::new(config.validate().unwrap());

    let (log_store, state_machine_store) =
        new_storage(&dir, app.topic_manager.clone(), app.topic_router.clone()).await;

    let network = Network {};

    let raft = openraft::Raft::new(
        app.settings.cluster.node_id,
        config.clone(),
        network,
        log_store,
        state_machine_store,
    )
    .await
    .unwrap();

    let _ = app.raft.set(raft);
    let _ = app.config.set(config.clone());

    let raft_service = RaftServiceImpl {
        app: app.clone(),
    };

    let addr_str = app.settings.cluster.rpc.external.to_string();
    let ret = addr_str.parse::<std::net::SocketAddr>();

    let addr = match ret {
        Ok(addr) => addr,
        Err(e) => {
            return Err(anyhow::anyhow!("parse address error: {}", e));
        }
    };

    let svc = RaftServiceServer::new(raft_service);
    let srv = tonic::transport::server::Server::builder().add_service(svc);

    let (tx, rx) = tokio::sync::oneshot::channel();

    let h = tokio::spawn(async move {
        srv.serve_with_shutdown(addr, async move {
            rx.await.ok();
            info!("signal receivbed, shutting down raft grpc: id={} {}", addr, addr_str);
        })
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
        Ok::<(), anyhow::Error>(())
    });

    app.raft_grpc_running_tx.set(tx).unwrap();

    let mut jh = app.join_handles.lock().await;

    jh.push(h);

    Ok(())
}
