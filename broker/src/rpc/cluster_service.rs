use tonic::{Request, Response, Status};
use crate::protobuf::cluster_service_server::ClusterService;
use crate::protobuf::{AdvanceInflightStateRequest, AdvanceInflightStateResponse, CleanRetainPublishMessageRequest, CleanRetainPublishMessageResponse, CreateSessionStateRequest, CreateSessionStateResponse, CreateTenantRequest, CreateTenantResponse, DeleteSessionStateRequest, DeleteSessionStateResponse, GetCurrentInflightPacketRequest, GetCurrentInflightPacketResponse, GetNextInflightPacketRequest, GetNextInflightPacketResponse, GetRetainPublishMessageRequest, GetRetainPublishMessageResponse, GetSessionStateRequest, GetSessionStateResponse, GetSubscribersByTopicRequest, GetSubscribersByTopicResponse, PopOfflineMessageRequest, PopOfflineMessageResponse, RegisterInflightRxPacketRequest, RegisterInflightRxPacketResponse, RegisterInflightTxPacketRequest, RegisterInflightTxPacketResponse, RegisterRetainPublishMessageRequest, RegisterRetainPublishMessageResponse, RegisterSessionActorMapRequest, RegisterSessionActorMapResponse, RenewSessionLeaseRequest, RenewSessionLeaseResponse, StoreOfflineMessageRequest, StoreOfflineMessageResponse, SubscribeTopicRequest, SubscribeTopicResponse, UnregisterSessionActorMapRequest, UnregisterSessionActorMapResponse, UnsubscribeTopicRequest, UnsubscribeTopicResponse};

struct ClusterServiceImpl;

#[tonic::async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn store_offline_message(&self, request: Request<StoreOfflineMessageRequest>) -> Result<Response<StoreOfflineMessageResponse>, Status> {
        todo!()
    }

    async fn pop_offline_message(&self, request: Request<PopOfflineMessageRequest>) -> Result<Response<PopOfflineMessageResponse>, Status> {
        todo!()
    }

    async fn get_session_state(&self, request: Request<GetSessionStateRequest>) -> Result<Response<GetSessionStateResponse>, Status> {
        todo!()
    }

    async fn create_session_state(&self, request: Request<CreateSessionStateRequest>) -> Result<Response<CreateSessionStateResponse>, Status> {
        todo!()
    }

    async fn delete_session_state(&self, request: Request<DeleteSessionStateRequest>) -> Result<Response<DeleteSessionStateResponse>, Status> {
        todo!()
    }

    async fn register_inflight_rx_packet(&self, request: Request<RegisterInflightRxPacketRequest>) -> Result<Response<RegisterInflightRxPacketResponse>, Status> {
        todo!()
    }

    async fn register_inflight_tx_packet(&self, request: Request<RegisterInflightTxPacketRequest>) -> Result<Response<RegisterInflightTxPacketResponse>, Status> {
        todo!()
    }

    async fn advance_inflight_state(&self, request: Request<AdvanceInflightStateRequest>) -> Result<Response<AdvanceInflightStateResponse>, Status> {
        todo!()
    }

    async fn get_current_inflight_packet(&self, request: Request<GetCurrentInflightPacketRequest>) -> Result<Response<GetCurrentInflightPacketResponse>, Status> {
        todo!()
    }

    async fn get_next_inflight_packet(&self, request: Request<GetNextInflightPacketRequest>) -> Result<Response<GetNextInflightPacketResponse>, Status> {
        todo!()
    }

    async fn subscribe_topic(&self, request: Request<SubscribeTopicRequest>) -> Result<Response<SubscribeTopicResponse>, Status> {
        todo!()
    }

    async fn unsubscribe_topic(&self, request: Request<UnsubscribeTopicRequest>) -> Result<Response<UnsubscribeTopicResponse>, Status> {
        todo!()
    }

    async fn get_subscribers_by_topic(&self, request: Request<GetSubscribersByTopicRequest>) -> Result<Response<GetSubscribersByTopicResponse>, Status> {
        todo!()
    }

    async fn register_retain_publish_message(&self, request: Request<RegisterRetainPublishMessageRequest>) -> Result<Response<RegisterRetainPublishMessageResponse>, Status> {
        todo!()
    }

    async fn get_retain_publish_message(&self, request: Request<GetRetainPublishMessageRequest>) -> Result<Response<GetRetainPublishMessageResponse>, Status> {
        todo!()
    }

    async fn clean_retain_publish_message(&self, request: Request<CleanRetainPublishMessageRequest>) -> Result<Response<CleanRetainPublishMessageResponse>, Status> {
        todo!()
    }

    async fn create_tenant(&self, request: Request<CreateTenantRequest>) -> Result<Response<CreateTenantResponse>, Status> {
        todo!()
    }

    async fn register_session_actor_map(&self, request: Request<RegisterSessionActorMapRequest>) -> Result<Response<RegisterSessionActorMapResponse>, Status> {
        todo!()
    }

    async fn un_register_session_actor_map(&self, request: Request<UnregisterSessionActorMapRequest>) -> Result<Response<UnregisterSessionActorMapResponse>, Status> {
        todo!()
    }

    async fn renew_session_lease(&self, request: Request<RenewSessionLeaseRequest>) -> Result<Response<RenewSessionLeaseResponse>, Status> {
        todo!()
    }
}