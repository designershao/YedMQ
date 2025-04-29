use std::collections::BTreeMap;
use std::path::Path;
use std::{collections::HashMap, sync::Arc};

use actix::Recipient;
use log::info;
use mockall::automock;
use openraft::error::{ClientWriteError, Infallible, InitializeError};
use openraft::Config;
use raft_network_impl::Network;
use store::new_storage;
use tokio::sync::{watch, Mutex, RwLock};
use types::{SessionActorMapResponse, SessionActorMapTypeConfig};

use crate::session::session_actor_map_storage::{SessionActorMapEntry, SessionClock, SessionVersion};
use crate::settings::Settings;
use crate::session::session_actor_map_storage::SessionActorMapStorage;

use super::client::session_actor_map::SessionActorMapRaftClient;
use super::raft_manager::RaftManagerError;
use super::NodeId;
use super::Node;

pub mod raft_network_impl;
pub mod store;
pub mod types;

pub type SessionActorMapRaft = openraft::Raft<SessionActorMapTypeConfig>;

#[async_trait::async_trait]
#[automock]
pub trait SessionActorMapRaftManagerTrait {

    async fn get_node_by_id(&self, node_id: NodeId) -> Option<Node>;

    async fn get_leader(&self) -> Option<Node>;

    async fn init_cluster(&self) -> Result<(), RaftManagerError<InitializeError<NodeId, Node>>>;

    async fn register_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftManagerError<ClientWriteError<NodeId, Node>>>;

    async fn unregister_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftManagerError<ClientWriteError<NodeId,Node>>>;

    async fn get_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Option<SessionActorMapEntry>;

    fn raft(&self) -> &SessionActorMapRaft;
    

}

#[async_trait::async_trait]
impl SessionActorMapRaftManagerTrait for SessionActorMapRaftManager {

    async fn get_node_by_id(&self, id: NodeId) -> Option<Node> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .find(|x| *x.0 == id)
            .and_then(|x| Some(x.1.clone()))
    }

    async fn get_leader(&self) -> Option<Node>{
        self.raft.current_leader().await.and_then(|id| {
            self.raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .find(|x| *x.0 == id)
                .and_then(|x| Some(x.1.clone()))
        })
    }

    async fn init_cluster(
        &self,
    ) -> Result<(), RaftManagerError<InitializeError<NodeId, Node>>> {
        let mut cluster_nodes = BTreeMap::new();
        cluster_nodes.insert(
            self.settings.cluster.node_id,
            Node {
                rpc_addr: self.settings.cluster.rpc.external.to_string(),
                api_addr: self.settings.listener.api.external.to_string(),
            },
        );

        self.raft.initialize(cluster_nodes).await?;
        Ok(())
    }

    fn raft(&self) -> &SessionActorMapRaft {
        return &self.raft
    }

    async fn register_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        node_id: NodeId,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftManagerError<ClientWriteError<NodeId,Node>>> {
        let request = types::SessionActorMapRequest::RegisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            node_id,
            version,
        };

        let res = self.raft.client_write(request).await?;
        Ok(res.data)
    }

    async fn unregister_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
        version: SessionVersion,
    ) -> Result<SessionActorMapResponse, RaftManagerError<ClientWriteError<NodeId,Node>>> {
        let request = types::SessionActorMapRequest::UnregisterSession {
            tenant_id: tenant_id.to_string(),
            session_id: client_id.to_string(),
            session_version:version,
        };

        let res = self.raft.client_write(request).await?;
        Ok(res.data)
    }

    async fn get_session_actor_map(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Option<SessionActorMapEntry> {
        let session_map_storage_guard = self.session_actor_map_storage.read().await;
        session_map_storage_guard.get_session_actor_map(tenant_id, client_id)
    }
}

pub struct SessionActorMapRaftManager {
    session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,

    pub raft: Arc<SessionActorMapRaft>,

    current_leader: Arc<RwLock<Option<NodeId>>>,

    nodes: Arc<RwLock<HashMap<NodeId, Node>>>,

    join_handles: Mutex<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>>,

    settings: Arc<Settings>,

    running_rx: watch::Receiver<()>,

    running_tx: watch::Sender<()>,

    raft_client: Arc<dyn crate::raft::client::session_actor_map::SessionActorMapRaftClientTrait>,
}

impl SessionActorMapRaftManager {

    pub fn get_raft_client(&self) -> Arc<dyn crate::raft::client::session_actor_map::SessionActorMapRaftClientTrait> {
        self.raft_client.clone()
    }

    async fn get_raft_config(heartbeat_interval: u64) -> Config {
        Config {
            cluster_name: "yedmq_session_actor_map_cluster".to_string(),
            ..Default::default()
        }
    }

    pub async fn init_cluster(&self) -> Result<(), RaftManagerError<InitializeError<NodeId, Node>>> {
        let mut cluster_nodes = BTreeMap::new();
        cluster_nodes.insert(
            self.settings.cluster.node_id,
            Node {
                rpc_addr: self.settings.cluster.rpc.external.to_string(),
                api_addr: self.settings.listener.api.external.to_string(),
            },
        );

        self.raft.initialize(cluster_nodes).await?;
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), RaftManagerError<Infallible>> {
        let mut rx = self.raft.metrics();

        self.raft.shutdown().await.map_err(|e| {
            RaftManagerError::InternalError(format!("Failed to shutdown raft, {}", e))
        })?;

        if let Err(e) = self.running_tx.send(()) {
            return Err(RaftManagerError::InternalError(format!(
                "Failed to shutdown raft, {}",
                e
            )));
        }

        loop {
            let r = rx.changed().await;
            if r.is_err() {
                break;
            }
        }

        for j in self.join_handles.lock().await.iter_mut() {
            let _rst = j
                .await
                .map_err(|e| RaftManagerError::InternalError(format!("{}", e)))?;
        }

        info!("Raft shutdown: id={}", self.settings.cluster.node_id);
        Ok(())
    }

    pub async fn new(
        settings: Arc<Settings>,
        session_actor_map_storage: Arc<RwLock<SessionActorMapStorage>>,
        session_manager_force_stop_recipient: Recipient<
            crate::session::session_manager_actor::ForceStop,
        >,
        session_clock: Arc<SessionClock>,
    ) -> Self {
        let cluster_cfg = &settings.cluster;
        let raft_config = Self::get_raft_config(cluster_cfg.heartbeat_interval.into()).await;

        let dir = Path::new(&cluster_cfg.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let (log_store, state_machine_store) = new_storage(
            &dir,
            session_actor_map_storage.clone(),
            cluster_cfg.node_id,
            session_manager_force_stop_recipient,
            session_clock,
        )
        .await;

        let network = Network {};

        let raft = openraft::Raft::new(
            cluster_cfg.node_id,
            config.clone(),
            network,
            log_store,
            state_machine_store,
        )
        .await
        .unwrap();
        let (tx, rx) = watch::channel::<()>(());

        let raft_arc = Arc::new(raft);

        let raft_client = SessionActorMapRaftClient::new(raft_arc.clone());

        let manager = SessionActorMapRaftManager {
            raft: raft_arc.clone(),
            current_leader: Arc::new(RwLock::new(None)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            join_handles: Mutex::new(vec![]),
            running_rx: rx,
            running_tx: tx,
            settings: settings.clone(),
            raft_client: Arc::new(raft_client),
            session_actor_map_storage: session_actor_map_storage.clone(),
        };

        manager.start_monitor_raft_metrics();
        manager
    }

    fn start_monitor_raft_metrics(&self) {
        let leader_state = self.current_leader.clone();
        let mut rx = self.raft.metrics();

        actix::spawn(async move {
            loop {
                let _ = rx.changed().await;
                let mut state = leader_state.write().await;
                *state = rx.borrow().current_leader;
            }
        });
    }

}
