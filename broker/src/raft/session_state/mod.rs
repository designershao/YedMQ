use std::collections::BTreeMap;
use std::path::Path;
use std::{collections::HashMap, sync::Arc};

use log::info;
use mockall::automock;
use openraft::error::{CheckIsLeaderError, ClientWriteError, Infallible, InitializeError};
use openraft::Config;
use raft_network_impl::Network;
use store::new_storage;
use tokio::sync::{watch, Mutex, RwLock};
use types::{SessionStateRequest, SessionStateResponse, SessionStateTypeConfig};
use yedmq_mqtt::MqttPacketV3;

use crate::session::session_state_storage::{SessionState, SessionStateStorage};
use crate::settings::Settings;

use super::raft_manager::RaftManagerError;
use super::Node;
use super::NodeId;

pub mod raft_network_impl;
pub mod store;
pub mod types;

pub type SessionStateRaft = openraft::Raft<SessionStateTypeConfig>;

#[async_trait::async_trait]
#[automock]
pub trait SessionStateRaftManagerTrait {

    async fn init_cluster(&self) -> Result<(), RaftManagerError<InitializeError<NodeId, Node>>>;

    async fn get_leader(&self) -> Option<Node>;

    async fn get_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Result<Option<SessionState>, RaftManagerError<Infallible>>;

    async fn get_session_state_ensure_linearizable(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Result<Option<SessionState>, RaftManagerError<CheckIsLeaderError<NodeId, Node>>>;

    async fn create_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
        inflight_duration: u64,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>>;

    async fn delete_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>>;

    async fn inflight_register_rx_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>>;

    async fn inflight_register_tx_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>>;

    async fn inflight_get_current_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet_identifier: u16,
    ) -> Result<Option<MqttPacketV3>, RaftManagerError<CheckIsLeaderError<NodeId, Node>>>;

    async fn inflight_get_current_packet_ensure_linearizable(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet_identifier: u16,
    ) -> Result<Option<MqttPacketV3>, RaftManagerError<CheckIsLeaderError<NodeId, Node>>>;

    async fn inflight_next_state(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet_identifier: u16,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>>;

    async fn inflight_get_next_state_packet_ensure_linearizable(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet_identifier: u16,
    ) -> Result<Option<MqttPacketV3>, RaftManagerError<CheckIsLeaderError<NodeId, Node>>>;

    async fn inflight_clean_finished_items(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>>;

    async fn append_to_pending_queue(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>>;

    async fn subscribe_topic(
        &self,
        tenant_id: String,
        client_id: String,
        topic: String,
        qos: u8,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>>;

    async fn unsubscribe_topic(
        &self,
        tenant_id: String,
        client_id: String,
        topic: String,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>>;

    async fn session_state_exists(&self, tenant_id: &str, client_id: &str) -> bool;

    async fn session_state_exists_ensure_linearizable(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Result<bool, RaftManagerError<CheckIsLeaderError<NodeId, Node>>>;

    async fn pop_from_pending_queue(
        &self,
        tenant_id: String,
        client_id: String,
    ) -> Result<Option<MqttPacketV3>, RaftManagerError<ClientWriteError<NodeId, Node>>>;

    fn raft(&self) -> &SessionStateRaft;

}

#[async_trait::async_trait]
impl SessionStateRaftManagerTrait for SessionStateRaftManager {

    async fn inflight_get_next_state_packet_ensure_linearizable(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet_identifier: u16,
    ) -> Result<Option<MqttPacketV3>, RaftManagerError<CheckIsLeaderError<NodeId, Node>>> {
        self.raft.ensure_linearizable().await?;
        let session_state_guard = self.session_state_storage.read().await;
        let r = session_state_guard
            .inflight_get_next_state_packet(tenant_id.to_string(), client_id.to_string(), packet_identifier)
            .await;
        Ok(r)
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

    fn raft(&self) -> &SessionStateRaft {
        &self.raft
    }

    async fn pop_from_pending_queue(
        &self,
        tenant_id: String,
        client_id: String,
    ) -> Result<Option<MqttPacketV3>, RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let request = SessionStateRequest::PopFromPendingQueue {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        };
        let res = self.raft.client_write(request).await?;
        match res.data {
            SessionStateResponse::PopFromPendingQueueResult(packet) => Ok(packet),
            _ => Ok(None),
        }
    }

    async fn session_state_exists_ensure_linearizable(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Result<bool, RaftManagerError<CheckIsLeaderError<NodeId, Node>>> {
        self.raft.ensure_linearizable().await?;
        let session_state_guard = self.session_state_storage.read().await;
        let r = session_state_guard
            .get_session_state(tenant_id, client_id)
            .await;
        return Ok(r.is_some());
    }

    // get session existed from leader
    async fn session_state_exists(&self, tenant_id: &str, client_id: &str) -> bool {
        let session_state_guard = self.session_state_storage.read().await;
        let r = session_state_guard
            .get_session_state(tenant_id, client_id)
            .await;
        return r.is_some();
    }

    async fn get_session_state_ensure_linearizable(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Result<Option<SessionState>, RaftManagerError<CheckIsLeaderError<NodeId, Node>>> {
        self.raft.ensure_linearizable().await?;
        let session_state_guard = self.session_state_storage.read().await;
        let r = session_state_guard
            .get_session_state(tenant_id, client_id)
            .await;
        if let Some(session_state) = r {
            let session_state = session_state.read().await.clone();
            Ok(Some(session_state))
        } else {
            Ok(None)
        }
    }

    async fn get_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Result<Option<SessionState>, RaftManagerError<Infallible>> {
        let session_state_guard = self.session_state_storage.read().await;
        let r = session_state_guard
            .get_session_state(tenant_id, client_id)
            .await;
        if let Some(session_state) = r {
            let session_state = session_state.read().await.clone();
            Ok(Some(session_state))
        } else {
            Ok(None)
        }
    }

    async fn create_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
        inflight_duration: u64,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let request = SessionStateRequest::CreateSessionState {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            inflight_duration_secs: inflight_duration,
        };
        self.raft.client_write(request).await?;
        Ok(())
    }

    async fn delete_session_state(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let request = SessionStateRequest::DeleteSessionState {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        };

        self.raft.client_write(request).await?;

        Ok(())
    }

    async fn inflight_register_rx_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let request = SessionStateRequest::InflightRegisterRxPacket {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            packet,
        };
        self.raft.client_write(request).await?;
        Ok(())
    }

    async fn inflight_register_tx_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let request = SessionStateRequest::InflightRegisterTxPacket {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            packet,
        };

        self.raft.client_write(request).await?;

        Ok(())
    }

    async fn inflight_get_current_packet_ensure_linearizable(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet_identifier: u16,
    ) -> Result<Option<MqttPacketV3>, RaftManagerError<CheckIsLeaderError<NodeId, Node>>> {
        self.raft.ensure_linearizable().await?;
        let state_storage_guard = self.session_state_storage.read().await;
        let packet_opt = state_storage_guard
            .inflight_get_current_packet(
                tenant_id.to_string(),
                client_id.to_string(),
                packet_identifier,
            )
            .await;
        return Ok(packet_opt);
    }

    async fn inflight_get_current_packet(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet_identifier: u16,
    ) -> Result<Option<MqttPacketV3>, RaftManagerError<CheckIsLeaderError<NodeId, Node>>> {
        let state_storage_guard = self.session_state_storage.read().await;
        let packet_opt = state_storage_guard
            .inflight_get_current_packet(
                tenant_id.to_string(),
                client_id.to_string(),
                packet_identifier,
            )
            .await;
        return Ok(packet_opt);
    }

    async fn inflight_next_state(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet_identifier: u16,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let request = SessionStateRequest::InflightNextState {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            packet_identifier: packet_identifier.into(),
        };
        self.raft.client_write(request).await?;
        Ok(())
    }

    async fn inflight_clean_finished_items(
        &self,
        tenant_id: &str,
        client_id: &str,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let request = SessionStateRequest::InflightCleanFinishItems {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
        };

        self.raft.client_write(request).await?;
        Ok(())
    }

    async fn append_to_pending_queue(
        &self,
        tenant_id: &str,
        client_id: &str,
        packet: MqttPacketV3,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let request = SessionStateRequest::AppendToPendingQueue {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            packet,
        };
        self.raft.client_write(request).await?;
        Ok(())
    }

    async fn subscribe_topic(
        &self,
        tenant_id: String,
        client_id: String,
        topic: String,
        qos: u8,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let request = SessionStateRequest::SubscribeTopic {
            tenant_id,
            client_id,
            topic,
            qos,
        };
        self.raft.client_write(request).await?;
        Ok(())
    }

    async fn unsubscribe_topic(
        &self,
        tenant_id: String,
        client_id: String,
        topic: String,
    ) -> Result<(), RaftManagerError<ClientWriteError<NodeId, Node>>> {
        let request = SessionStateRequest::UnsubscribeTopic {
            tenant_id,
            client_id,
            topic,
        };
        self.raft.client_write(request).await?;
        Ok(())
    }
}

pub struct SessionStateRaftManager {
    pub session_state_storage: Arc<RwLock<SessionStateStorage>>,

    pub raft: Arc<SessionStateRaft>,

    current_leader: Arc<RwLock<Option<NodeId>>>,

    nodes: Arc<RwLock<HashMap<NodeId, Node>>>,

    join_handles: Mutex<Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>>,

    settings: Arc<Settings>,

    running_rx: watch::Receiver<()>,

    running_tx: watch::Sender<()>,

    raft_client: Arc<dyn crate::raft::client::session_state::SessionStateRaftClientTrait>,
}

impl SessionStateRaftManager {
    pub fn get_raft_client(&self) -> Arc<dyn crate::raft::client::session_state::SessionStateRaftClientTrait> {
        self.raft_client.clone()
    }

    pub async fn init_cluster(
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

    async fn get_raft_config(heartbeat_interval: u64) -> Config {
        Config {
            cluster_name: "yedmq_session_state_cluster".to_string(),
            ..Default::default()
        }
    }

    // get session state from local raft store
    pub async fn get_session_state_from_local_raft_store(
        &self,
        tenant_id: &str,
        session_id: &str,
    ) -> Option<Arc<RwLock<SessionState>>> {
        let session_state_guard = self.session_state_storage.read().await;
        session_state_guard
            .get_session_state(tenant_id, session_id)
            .await
    }

    pub async fn session_state_exists_from_local_raft_store(
        &self,
        tenant_id: &str,
        session_id: &str,
    ) -> bool {
        let session_state_guard = self.session_state_storage.read().await;
        session_state_guard
            .session_state_exists(tenant_id, session_id)
            .await
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
        session_state_storage: Arc<RwLock<SessionStateStorage>>,
    ) -> Self {
        let raft_config = Self::get_raft_config(settings.cluster.heartbeat_interval.into()).await;

        let dir = Path::new(&settings.cluster.store_dir);

        let config = Arc::new(raft_config.validate().unwrap());

        let (log_store, state_machine_store) =
            new_storage(&dir, session_state_storage.clone()).await;

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
        let raft_arc = Arc::new(raft);

        let raft_client = Arc::new(crate::raft::client::session_state::SessionStateRaftClient::new(raft_arc.clone()));

        let (tx, rx) = watch::channel::<()>(());

        let manager = SessionStateRaftManager {
            raft: raft_arc.clone(),
            current_leader: Arc::new(RwLock::new(None)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            join_handles: Mutex::new(vec![]),
            running_rx: rx,
            running_tx: tx,
            settings,
            session_state_storage,
            raft_client
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
