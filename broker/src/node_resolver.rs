use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use actix::Addr;
use log::warn;
use tokio::sync::RwLock;

use crate::{
    raft::{topic::topic_raft_actor, Node, NodeId},
    settings::Settings,
};

pub struct NodeResolver {
    fallback_nodes: HashMap<NodeId, Node>,
    dynamic_nodes: RwLock<HashMap<NodeId, Node>>,
    last_refresh_at: RwLock<Option<Instant>>,
    refresh_cooldown: Duration,
}

impl NodeResolver {
    pub fn new(settings: Arc<Settings>) -> Self {
        let fallback_nodes = settings
            .cluster
            .nodes
            .iter()
            .map(|n| {
                (
                    n.id,
                    Node {
                        node_id: Some(n.id),
                        rpc_addr: n.rpc_address.clone(),
                        api_addr: n.api_address.clone(),
                    },
                )
            })
            .collect();

        Self {
            fallback_nodes,
            dynamic_nodes: RwLock::new(HashMap::new()),
            last_refresh_at: RwLock::new(None),
            refresh_cooldown: Duration::from_secs(3),
        }
    }

    pub async fn get_node(
        &self,
        node_id: NodeId,
        topic_raft_actor: &Addr<topic_raft_actor::TopicRaftActor>,
    ) -> Option<Node> {
        if let Some(node) = self.dynamic_nodes.read().await.get(&node_id).cloned() {
            return Some(node);
        }

        if let Some(node) = self.fallback_nodes.get(&node_id).cloned() {
            let _ = self.refresh_from_topic_raft(topic_raft_actor, false).await;
            return Some(node);
        }

        let _ = self.refresh_from_topic_raft(topic_raft_actor, true).await;
        self.dynamic_nodes
            .read()
            .await
            .get(&node_id)
            .cloned()
            .or_else(|| self.fallback_nodes.get(&node_id).cloned())
    }

    pub async fn refresh_from_topic_raft(
        &self,
        topic_raft_actor: &Addr<topic_raft_actor::TopicRaftActor>,
        force: bool,
    ) -> Result<(), String> {
        if !force {
            let last_refresh = *self.last_refresh_at.read().await;
            if let Some(last_refresh) = last_refresh {
                if last_refresh.elapsed() < self.refresh_cooldown {
                    return Ok(());
                }
            }
        }

        let response = topic_raft_actor
            .send(topic_raft_actor::GetClusterNodes)
            .await
            .map_err(|e| format!("mailbox error while refreshing cluster nodes: {}", e))?;

        match response {
            Ok(nodes) => {
                if nodes.is_empty() {
                    warn!("topic raft returned empty cluster node set, keep previous cache");
                } else {
                    *self.dynamic_nodes.write().await = nodes;
                }
            }
            Err(e) => {
                return Err(format!("topic raft get cluster nodes error: {}", e));
            }
        }

        *self.last_refresh_at.write().await = Some(Instant::now());
        Ok(())
    }
}
