use std::sync::Arc;
use actix::{Actor, Context, Handler, Message, ResponseActFuture, WrapFuture};
use actix::dev::MessageResponse;
use tokio::sync::RwLock;
use yedmq_mqtt::MqttPacketV3;
use crate::raft::NodeId;
use crate::topic::topic_storage::TopicStorage;
use crate::topic::TopicError;

#[derive(Message)]
#[rtype(result="Result<(), TopicError>")]
pub struct Subscribe{
    tenant_id: String,
    client_id: String,
    topic_filter: String,
    qos: u8
}

#[derive(Message)]
#[rtype(result="Result<(), TopicError>")]
pub struct Unsubscribe{
    tenant_id: String,
    client_id: String,
    topic_filter: String,
}

pub struct SubscriptionInfo {
    pub node_id: NodeId,
    pub client_identifier: String,
    pub qos: u8,
}

pub struct GetSubscriptionsResponse {
    subscriptions: Vec<SubscriptionInfo>,
}

impl<A, M> MessageResponse<A, M> for GetSubscriptionsResponse
where
    A: Actor,
    M: Message<Result = GetSubscriptionsResponse>,
{
    fn handle(
        self,
        _ctx: &mut <A as Actor>::Context,
        tx: Option<actix::dev::OneshotSender<<M as Message>::Result>>,
    ) {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
    }
}

#[derive(Message)]
#[rtype(result="Result<GetSubscriptionsResponse, TopicError>")]
pub struct GetSubscriptions{
    tenant_id: String,
    topic: String,
}

#[derive(Message)]
#[rtype(result="()")]
pub struct CleanRetainPublishPacket {
    tenant_id: String,
    topic: String,
}

#[derive(Message)]
#[rtype(result="Result<Vec<Arc<MqttPacketV3>>, TopicError>")]
pub struct GetRetainPublishPacket {
    tenant_id: String,
    topic: String,
}

#[derive(Message)]
#[rtype(result="Result<(), TopicError>")]
pub struct SetRetainPublishPacket {
    tenant_id: String,
    client_id: String,
    publish_packet: MqttPacketV3
}

#[derive(Message)]
#[rtype(result="()")]
pub struct CreateTenant {
    tenant_id: String,
}

#[derive(Message)]
#[rtype(result="GetRetainMessagesResponse")]
pub struct GetRetainMessages {
    tenant_id: String,
    offset: u64,
    limit: u64,
}

pub struct GetRetainMessagesResponse {
    total: u64,
    messages: Vec<(String, String, u8)>,
}

impl<A, M> MessageResponse<A, M> for GetRetainMessagesResponse
where
    A: Actor,
    M: Message<Result = GetRetainMessagesResponse>,
{
    fn handle(
        self,
        _ctx: &mut <A as Actor>::Context,
        tx: Option<actix::dev::OneshotSender<<M as Message>::Result>>,
    ) {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
    }
}

pub struct GetTopicsResponse {
    total: u64,
    topics: Vec<(String, String, u8)>,
}

impl<A, M> MessageResponse<A, M> for GetTopicsResponse
where
    A: Actor,
    M: Message<Result = GetTopicsResponse>,
{
    fn handle(
        self,
        _ctx: &mut <A as Actor>::Context,
        tx: Option<actix::dev::OneshotSender<<M as Message>::Result>>,
    ) {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
    }
}

#[derive(Message)]
#[rtype(result="GetTopicsResponse")]
pub struct GetTopics{
    tenant_id: String,
    offset: u64,
    limit: u64,
}

#[derive(Message)]
#[rtype(result="()")]
pub struct SetCache {
    storage: TopicStorage,
}

#[derive(Message)]
#[rtype(result = "Result<(), TopicError>")]
pub enum UpdateCacheEvent {
    Subscribe {
        tenant_id: String,
        client_id: String,
        topic_filter: String,
        qos: u8
    },
    Unsubscribe {
        tenant_id: String,
        client_id: String,
        topic_filter: String,
    }
}

pub struct TopicManagerActor {
    local_topic_cache: Arc<RwLock<TopicStorage>>,
    topic_raft_client: Arc<dyn crate::raft::client::topic::TopicRaftClientTrait>,
    node_id: NodeId
}

impl Actor for TopicManagerActor {
    type Context = Context<Self>;
}

impl Handler<Subscribe> for TopicManagerActor {
    type Result = ResponseActFuture<Self, Result<(), TopicError>>;

    fn handle(&mut self, msg: Subscribe, ctx: &mut Self::Context) -> Self::Result {
        let topic_raft_client = self.topic_raft_client.clone();
        Box::pin(async move {
            topic_raft_client.handle_subscribe(
                msg.tenant_id,
                msg.client_id,
                msg.topic_filter,
                msg.qos
            ).await.map_err(|e| {
                TopicError::InternalError(e.to_string())
            })
        }.into_actor(self))
    }
}

impl Handler<Unsubscribe> for TopicManagerActor {
    type Result = ResponseActFuture<Self, Result<(), TopicError>>;

    fn handle(&mut self, msg: Unsubscribe, ctx: &mut Self::Context) -> Self::Result {
        let topic_raft_client = self.topic_raft_client.clone();
        Box::pin(async move {
            topic_raft_client.handle_unsubscribe(
                msg.tenant_id,
                msg.client_id,
                msg.topic_filter
            ).await.map_err(|e| {
                TopicError::InternalError(e.to_string())
            })
        }.into_actor(self))
    }
}

impl Handler<GetSubscriptions> for TopicManagerActor {
    type Result = ResponseActFuture<Self, Result<GetSubscriptionsResponse, TopicError>>;

    fn handle(&mut self, msg:GetSubscriptions, ctx: &mut Self::Context) -> Self::Result {
        let cache_arc = self.local_topic_cache.clone();

        Box::pin(async move {
            let cache = cache_arc.read().await;
            cache.get_subscriptions(
                msg.tenant_id,
                msg.topic
            ).map(|x| {
                let info = x.iter().map(move |x| {
                    SubscriptionInfo{
                        node_id: x.node_id,
                        client_identifier: x.client_identifier.clone(),
                        qos: x.qos,
                    }
                }).collect();
                GetSubscriptionsResponse {
                    subscriptions: info
                }
            })
        }.into_actor(self))
    }
}

impl Handler<GetRetainPublishPacket> for TopicManagerActor {
    type Result = ResponseActFuture<Self, Result<Vec<Arc<MqttPacketV3>>, TopicError>>;

    fn handle(&mut self, msg: GetRetainPublishPacket, ctx: &mut Self::Context) -> Self::Result {
        let cache_arc = self.local_topic_cache.clone();
        Box::pin(async move {
            let cache = cache_arc.read().await;
            cache.get_retain_publish_packet(
                msg.tenant_id,
                msg.topic,
            )
        }.into_actor(self))
    }
}

impl Handler<SetRetainPublishPacket> for TopicManagerActor {
    type Result = ResponseActFuture<Self, Result<(), TopicError>>;

    fn handle(&mut self, msg: SetRetainPublishPacket, ctx: &mut Self::Context) -> Self::Result {
        let cache_arc = self.local_topic_cache.clone();

        Box::pin(async move {
            let mut cache = cache_arc.write().await;
            cache.register_retain_publish_packet(
                msg.tenant_id,
                msg.client_id,
                &msg.publish_packet
            )
        }.into_actor(self))
    }
}

impl Handler<CreateTenant> for TopicManagerActor {
    type Result = ResponseActFuture<Self,()>;

    fn handle(&mut self, msg: CreateTenant, ctx: &mut Self::Context) -> Self::Result {
        let cache_arc = self.local_topic_cache.clone();

        Box::pin(async move {
            let mut cache = cache_arc.write().await;
            cache.create_tenant(&msg.tenant_id)
        }.into_actor(self))

    }
}

impl Handler<GetRetainMessages> for TopicManagerActor {
    type Result = ResponseActFuture<Self, GetRetainMessagesResponse>;

    fn handle(&mut self, msg: GetRetainMessages, ctx: &mut Self::Context) -> Self::Result {
        let cache_arc = self.local_topic_cache.clone();

        Box::pin(async move {
            let cache = cache_arc.read().await;
            let result = cache.get_retain_message_list_with_pagination(
                &msg.tenant_id,
                msg.offset,
                msg.limit,
            ).unwrap();
            GetRetainMessagesResponse{
                total: result.0,
                messages: result.1,
            }
        }.into_actor(self))
    }
}

impl Handler<GetTopics> for TopicManagerActor {
    type Result = ResponseActFuture<Self, GetTopicsResponse>;

    fn handle(&mut self, msg: GetTopics, ctx: &mut Self::Context) -> Self::Result {
        let cache_arc = self.local_topic_cache.clone();
        Box::pin(async move {
            let cache = cache_arc.read().await;
            let result = cache.get_topic_list_with_pagination(
                &msg.tenant_id,
                msg.offset,
                msg.limit,
            ).unwrap();
            GetTopicsResponse {
                total: result.0,
                topics: result.1,
            }
        }.into_actor(self))
    }
}

impl Handler<SetCache> for TopicManagerActor {
    type Result = ();

    /// Handles the SetCache message to update the subscription cache within the TopicManager Actor.
    /// This method replaces the current local topic cache with the provided storage.
    fn handle(&mut self, msg: SetCache, ctx: &mut Self::Context) -> Self::Result {
        self.local_topic_cache = Arc::new(RwLock::new(msg.storage));
    }
}

impl Handler<UpdateCacheEvent> for TopicManagerActor {
    type Result = ResponseActFuture<Self, Result<(), TopicError>>;

    /// Handles UpdateCacheEvent to notify TopicManager to update cache.
    /// In the current design, this is mainly used by the raft component
    /// to notify cache updates when cluster writes are successful.
    fn handle(&mut self, msg: UpdateCacheEvent, ctx: &mut Self::Context) -> Self::Result {
        let cache_arc = self.local_topic_cache.clone();
        let node_id = self.node_id.clone();
        Box::pin(async move {
            let mut cache = cache_arc.write().await;
            match msg {
                UpdateCacheEvent::Subscribe { tenant_id, client_id, topic_filter, qos } => {
                    if !cache.contains_tenant(&tenant_id) {
                        cache.create_tenant(&tenant_id);
                    }
                    cache.subscribe(
                        tenant_id,
                        client_id,
                        topic_filter,
                        qos,
                        node_id
                    )
                }
                UpdateCacheEvent::Unsubscribe { tenant_id, client_id, topic_filter } => {
                    if !cache.contains_tenant(&tenant_id) {
                        cache.create_tenant(&tenant_id);
                    }
                    let result = cache.unsubscribe(
                        &tenant_id,
                        &client_id,
                        &topic_filter,
                        node_id
                    );
                    if result.is_err() {
                        let err = result.unwrap_err();
                        // ignore topic not found
                        if matches!(err, TopicError::TopicNotFound(_)) {
                            return Ok(());
                        } else {
                            return Err(err);
                        }
                    }
                    Ok(())
                }
            }
        }.into_actor(self))
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::client::topic::MockTopicRaftClientTrait;

    use super::*;

    #[actix::test]
    async fn when_receive_update_cache_event_subscribe_then_update_cache() {
        let topic_manager = TopicManagerActor {
            local_topic_cache: Arc::new(RwLock::new(TopicStorage::new())),
            topic_raft_client: Arc::new(MockTopicRaftClientTrait::new()),
            node_id: NodeId::default(),
        }.start();

        let event = UpdateCacheEvent::Subscribe {
            tenant_id: "tenant1".to_string(),
            client_id: "client1".to_string(),
            topic_filter: "topic1".to_string(),
            qos: 1,
        };

        let result = topic_manager.send(event).await.unwrap();
        assert!(result.is_ok());
    }

    #[actix::test]
    async fn when_receive_update_cache_event_unsubscribe_then_update_cache() {

        let topic_manager = TopicManagerActor {
            local_topic_cache: Arc::new(RwLock::new(TopicStorage::new())),
            topic_raft_client: Arc::new(MockTopicRaftClientTrait::new()),
            node_id: NodeId::default(),
        }.start();

        // First, subscribe to ensure there is a subscription to unsubscribe from
        let subscribe_event = UpdateCacheEvent::Subscribe {
            tenant_id: "tenant1".to_string(),
            client_id: "client1".to_string(),
            topic_filter: "topic1".to_string(),
            qos: 1,
        };

        let _ = topic_manager.send(subscribe_event).await.unwrap();

        let event = UpdateCacheEvent::Unsubscribe {
            tenant_id: "tenant1".to_string(),
            client_id: "client1".to_string(),
            topic_filter: "topic1".to_string(),
        };

        let result = topic_manager.send(event).await.unwrap();
        assert!(result.is_ok());
    }

    #[actix::test]
    async fn when_receive_update_cache_event_unsubscribe_without_subscription_then_ok() {
        let topic_manager = TopicManagerActor {
            local_topic_cache: Arc::new(RwLock::new(TopicStorage::new())),
            topic_raft_client: Arc::new(MockTopicRaftClientTrait::new()),
            node_id: NodeId::default(),
        }.start();

        let event = UpdateCacheEvent::Unsubscribe {
            tenant_id: "tenant1".to_string(),
            client_id: "client1".to_string(),
            topic_filter: "topic1".to_string(),
        };

        let result = topic_manager.send(event).await.unwrap();
        assert!(result.is_ok());
    }

    #[actix::test]
    async fn when_receive_set_cache_then_update_cache() {
        let topic_manager = TopicManagerActor {
            local_topic_cache: Arc::new(RwLock::new(TopicStorage::new())),
            topic_raft_client: Arc::new(MockTopicRaftClientTrait::new()),
            node_id: NodeId::default(),
        }.start();

        let mut storage = TopicStorage::new();
        storage.create_tenant(&"tenant1".to_string());
        storage.subscribe("tenant1".to_string(), "client1".to_string(), "topic1".to_string(), 1,1).unwrap();
        let event = SetCache { storage };

        topic_manager.send(event).await.unwrap();
        
        // Verify that the cache has been set by sending a GetTopics message
        let response = topic_manager.send(GetTopics {
            tenant_id: "tenant1".to_string(),
            offset: 0,
            limit: 10,
        }).await.unwrap();
        assert_eq!(response.total, 1);
    }

}