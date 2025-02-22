use std::{collections::HashMap, path::Path, sync::Arc};

use log::info;
use openraft::Config;
use tokio::sync::{watch, Mutex, RwLock};

use crate::{
    protobuf::{
        raft_service_client::RaftServiceClient, raft_service_server::RaftServiceServer,
        AppendEntriesRequest,
    },
    settings::Settings,
    topic::topic_storage::TopicStorage,
};

use super::{
    network::raft_network_impl::Network,
    service::raft_service::RaftServiceImpl,
    store::{new_storage, Request},
    Node, NodeId, YedMQRaft,
};

pub struct RaftManager {

    pub raft: YedMQRaft,

    current_leader: Arc<RwLock<Option<NodeId>>>,

    nodes: Arc<RwLock<HashMap<NodeId, Node>>>,

    join_handles: Mutex<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>>,

    running_rx: watch::Receiver<()>,

    running_tx: watch::Sender<()>,

}

impl RaftManager {
    pub async fn new(settings: Arc<Settings>, topic_storage: Arc<RwLock<TopicStorage>>) -> Self {
        let raft_config = Self::get_raft_config(settings.clone()).await;

        let dir = Path::new(&settings.cluster.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let (log_store, state_machine_store) = new_storage(&dir, topic_storage.clone()).await;

        let network = Network {};

        let raft = openraft::Raft::new(
            settings.cluster.node_id,
            config.clone(),
            network,
            log_store,
            state_machine_store,
        )
        .await
        .unwrap();
        let (tx, rx) = watch::channel::<()>(());

        let manager = RaftManager {
            raft,
            current_leader: Arc::new(RwLock::new(None)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            join_handles: Mutex::new(vec![]),
            running_rx: rx,
            running_tx: tx,
        };

        manager.start_monitor_raft_metrics();
        manager
    }

    fn start_monitor_raft_metrics(&self) {
        let leader_state = self.current_leader.clone();
        let mut rx = self.raft.metrics();

        tokio::spawn(async move {
            loop {
                let _ = rx.changed().await;
                let mut state = leader_state.write().await;
                *state = rx.borrow().current_leader;
            }
        });
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().state == openraft::ServerState::Leader
    }

    pub async fn get_leader(&self) -> Option<NodeId> {
        self.current_leader.read().await.clone()
    }

    pub async fn execute_command(&self, command: Request) -> Result<(), anyhow::Error> {
        if !self.is_leader().await {
            let leader_node_id = self.get_leader().await;

            if leader_node_id.is_none() {
                return Err(anyhow::anyhow!("No leader available"));
            } else {
                let nodes = self.nodes.read().await;

                let leader_node = nodes.get(&leader_node_id.unwrap()).unwrap();

                let addr = format!("http://{}", leader_node.rpc_addr);

                let mut client = RaftServiceClient::connect(addr.clone()).await.unwrap();

                let append_request = AppendEntriesRequest {
                    data: serde_json::to_string(&command).unwrap(),
                };

                let res = client.append_entries(append_request).await;
                if res.is_err() {
                    return Err(anyhow::anyhow!("AppendEntries failed"));
                }
            }
        } else {
            // current node is leader
            let res = self.raft.client_write(command).await;
            if res.is_err() {
                return Err(anyhow::anyhow!("ClientWrite failed"));
            }
        }
        Ok(())
    }

    async fn get_raft_config(settings: Arc<Settings>) -> Config {
        let heartbeat_interval = settings.cluster.heartbeat_interval as u64;
        let election_timeout_min = heartbeat_interval * 2;

        Config {
            heartbeat_interval,
            election_timeout_min,
            ..Default::default()
        }
    }

    pub async fn start_grpc(
        raft_manager: Arc<RaftManager>,
        settings: Arc<Settings>,
    ) -> anyhow::Result<()> {
        let mut rx = raft_manager.running_rx.clone();

        let raft_service = RaftServiceImpl {
            raft_manager: raft_manager.clone(),
        };

        let addr_str = settings.cluster.rpc.external.to_string();
        let ret = addr_str.parse::<std::net::SocketAddr>();

        let addr = match ret {
            Ok(addr) => addr,
            Err(e) => {
                return Err(anyhow::anyhow!("parse address error: {}", e));
            }
        };

        let svc = RaftServiceServer::new(raft_service);
        let srv = tonic::transport::server::Server::builder().add_service(svc);

        let h = tokio::spawn(async move {
            srv.serve_with_shutdown(addr, async move {
                let _ = rx.changed().await;
                info!("signal receivbed, shutting down: id={} {}", addr, addr_str);
            })
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
            Ok::<(), anyhow::Error>(())
        });

        let mut jh = raft_manager.join_handles.lock().await;

        jh.push(h);

        Ok(())
    }
}
