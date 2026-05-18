use crate::protobuf::cluster_service_server::ClusterService;
use crate::protobuf::{
    AdvanceInflightStateRequest, AdvanceInflightStateResponse, CleanRetainPublishMessageRequest,
    CleanRetainPublishMessageResponse, CreateSessionStateRequest, CreateSessionStateResponse,
    DeleteSessionStateRequest, DeleteSessionStateResponse, ForceStopSessionActorRequest,
    ForceStopSessionActorResponse, GetCurrentInflightPacketRequest,
    GetCurrentInflightPacketResponse, GetNextInflightPacketRequest, GetNextInflightPacketResponse,
    GetRetainPublishMessageRequest, GetRetainPublishMessageResponse, GetSessionActorMapRequest,
    GetSessionActorMapResponse, GetSessionStateRequest, GetSessionStateResponse,
    GetSubscribersByTopicRequest, GetSubscribersByTopicResponse, PopOfflineMessageRequest,
    PopOfflineMessageResponse, RegisterInflightRxPacketRequest, RegisterInflightRxPacketResponse,
    RegisterInflightTxPacketRequest, RegisterInflightTxPacketResponse,
    RegisterRetainPublishMessageRequest, RegisterRetainPublishMessageResponse,
    RegisterSessionActorMapRequest, RegisterSessionActorMapResponse, RenewSessionLeaseRequest,
    RenewSessionLeaseResponse, StoreOfflineMessageRequest, StoreOfflineMessageResponse,
    SubscribeTopicRequest, SubscribeTopicResponse, UnregisterSessionActorMapRequest,
    UnregisterSessionActorMapResponse, UnsubscribeTopicRequest, UnsubscribeTopicResponse,
};
use crate::raft::session_actor_map::session_actor_map_raft_actor;
use crate::raft::session_state::session_state_raft_actor::{self, SessionStateRaftActor};
use crate::router_actor::{RouteFromOtherNode, RouterActor};
use crate::rpc::grpc_status;
use crate::session::session_actor_map_storage::SessionVersion;
use crate::session::session_manager_actor::SessionManagerActor;
use crate::stored_packet::deserialize_stored_packet_from_str;
use crate::topic::TopicStorageError;
use actix::{Addr, SystemService};
use tonic::{Request, Response, Status};
use yedmq_mqtt::packet::Packet;

pub struct ClusterServiceImpl {
    pub router_actors: Vec<Addr<RouterActor>>,
}

fn map_topic_raft_error(
    action: &str,
    err: crate::raft::topic::topic_raft_actor::TopicRaftError,
) -> Status {
    match err {
        crate::raft::topic::topic_raft_actor::TopicRaftError::NotLeader {
            leader: Some(leader),
        } => grpc_status::leader_redirect_status(
            format!("{} requires leader handling at {}", action, leader.rpc_addr),
            leader.rpc_addr,
            leader.node_id,
        ),
        crate::raft::topic::topic_raft_actor::TopicRaftError::NotLeader { leader: None }
        | crate::raft::topic::topic_raft_actor::TopicRaftError::NoLeaderAvailable => {
            grpc_status::no_leader_status(format!("{} failed: no leader available", action))
        }
        crate::raft::topic::topic_raft_actor::TopicRaftError::NotInitialized => {
            grpc_status::not_ready_status(format!("{} failed: topic raft not initialized", action))
        }
        crate::raft::topic::topic_raft_actor::TopicRaftError::NotReady(message) => {
            grpc_status::not_ready_status(format!("{} failed: {}", action, message))
        }
        crate::raft::topic::topic_raft_actor::TopicRaftError::InvalidTopicName { topic } => {
            grpc_status::business_status(
                tonic::Code::InvalidArgument,
                grpc_status::business_detail(
                    crate::protobuf::ErrorCode::TopicInvalidName,
                    format!("invalid topic name: {}", topic),
                    "topic_raft".to_string(),
                ),
            )
        }
        crate::raft::topic::topic_raft_actor::TopicRaftError::TopicStorageError(
            TopicStorageError::TopicNotFound(topic),
        ) => grpc_status::business_status(
            tonic::Code::NotFound,
            grpc_status::business_detail(
                crate::protobuf::ErrorCode::TopicNotFound,
                format!("topic not found: {}", topic),
                "topic_raft".to_string(),
            ),
        ),
        crate::raft::topic::topic_raft_actor::TopicRaftError::TopicStorageError(
            TopicStorageError::TenantNotFound(tenant),
        ) => grpc_status::business_status(
            tonic::Code::NotFound,
            grpc_status::business_detail(
                crate::protobuf::ErrorCode::TopicTenantNotFound,
                format!("tenant not found: {}", tenant),
                "topic_raft".to_string(),
            ),
        ),
        crate::raft::topic::topic_raft_actor::TopicRaftError::TopicStorageError(
            TopicStorageError::InvalidTopicFilter(filter),
        ) => grpc_status::business_status(
            tonic::Code::InvalidArgument,
            grpc_status::business_detail(
                crate::protobuf::ErrorCode::TopicInvalidFilter,
                format!("invalid topic filter: {}", filter),
                "topic_raft".to_string(),
            ),
        ),
        crate::raft::topic::topic_raft_actor::TopicRaftError::TopicStorageError(
            TopicStorageError::InternalError(message),
        ) => grpc_status::fatal_status(tonic::Code::Internal, message),
        other => grpc_status::fatal_status(
            tonic::Code::Internal,
            format!("{} failed: {}", action, other),
        ),
    }
}

fn map_session_state_raft_error(
    action: &str,
    err: crate::raft::session_state::session_state_raft_actor::SessionStateRaftError,
) -> Status {
    match err {
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NotLeader {
            leader: Some(leader),
        } => grpc_status::leader_redirect_status(
            format!("{} requires leader handling at {}", action, leader.rpc_addr),
            leader.rpc_addr,
            leader.node_id,
        ),
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NotLeader {
            leader: None,
        }
        | crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NoLeaderAvailable => {
            grpc_status::no_leader_status(format!("{} failed: no leader available", action))
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NotInitialized => {
            grpc_status::not_ready_status(format!("{} failed: session state raft not initialized", action))
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::NotReady(message) => {
            grpc_status::not_ready_status(format!("{} failed: {}", action, message))
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::ServiceUnavailable(message)
        | crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::GRPCConnect(message) => {
            Status::unavailable(format!("{} failed: {}", action, message))
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::SessionStateNotExisted(message) => {
            grpc_status::business_status(
                tonic::Code::NotFound,
                grpc_status::business_detail(
                    crate::protobuf::ErrorCode::SessionStateNotFound,
                    message,
                    "session_state_raft".to_string(),
                ),
            )
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::InflightError(
            crate::inflight::InflightError::PacketIdentifierHasExisted,
        ) => grpc_status::business_status(
            tonic::Code::AlreadyExists,
            grpc_status::business_detail(
                crate::protobuf::ErrorCode::PacketIdentifierAlreadyExists,
                "packet identifier has existed",
                "session_state_raft".to_string(),
            ),
        ),
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::GRPCBusiness(err) => {
            grpc_status::business_status(
                err.grpc_code(),
                grpc_status::business_detail(err.code(), err.message(), err.node()),
            )
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::GRPC(status) => {
            status
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::Serialize(message) => {
            grpc_status::fatal_status(
                tonic::Code::Internal,
                format!("{} serialization failed: {}", action, message),
            )
        }
        crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::UnexpectedResponseType(message) => {
            grpc_status::fatal_status(
                tonic::Code::Internal,
                format!("{} returned unexpected response: {}", action, message),
            )
        }
        other => grpc_status::fatal_status(
            tonic::Code::Internal,
            format!("{} failed: {}", action, other),
        ),
    }
}

fn map_session_actor_map_raft_error(
    action: &str,
    err: crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError,
) -> Status {
    match err {
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NotLeader {
            leader: Some(leader),
        } => grpc_status::leader_redirect_status(
            format!("{} requires leader handling at {}", action, leader.rpc_addr),
            leader.rpc_addr,
            leader.node_id,
        ),
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NotLeader {
            leader: None,
        }
        | crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NoLeaderAvailable => {
            grpc_status::no_leader_status(format!("{} failed: no leader available", action))
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NotInitialized => {
            grpc_status::not_ready_status(format!("{} failed: session actor map raft not initialized", action))
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NotReady(message) => {
            grpc_status::not_ready_status(format!("{} failed: {}", action, message))
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::ServiceUnavailable(message)
        | crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::GRPCConnect(message) => {
            Status::unavailable(format!("{} failed: {}", action, message))
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::SessionVersionRejected {
            current_version,
            existing_version,
        } => grpc_status::business_status(
            tonic::Code::FailedPrecondition,
            grpc_status::session_version_rejected_detail(
                current_version.counter,
                current_version.node_id,
                existing_version.counter,
                existing_version.node_id,
                "session_actor_map_raft".to_string(),
            ),
        ),
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::TenantNotFound {
            tenant_id,
        } => grpc_status::business_status(
            tonic::Code::NotFound,
            grpc_status::business_detail(
                crate::protobuf::ErrorCode::SessionTenantNotFound,
                format!("tenant not found: {}", tenant_id),
                "session_actor_map_raft".to_string(),
            ),
        ),
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::GRPCBusiness(err) => {
            grpc_status::business_status(
                err.grpc_code(),
                grpc_status::business_detail(err.code(), err.message(), err.node()),
            )
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::GRPC(status) => {
            status
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::Serialize(message) => {
            grpc_status::fatal_status(
                tonic::Code::Internal,
                format!("{} serialization failed: {}", action, message),
            )
        }
        crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::UnexpectedResponseType(message) => {
            grpc_status::fatal_status(
                tonic::Code::Internal,
                format!("{} returned unexpected response: {}", action, message),
            )
        }
        other => grpc_status::fatal_status(
            tonic::Code::Internal,
            format!("{} failed: {}", action, other),
        ),
    }
}

#[tonic::async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn store_offline_message(
        &self,
        request: Request<StoreOfflineMessageRequest>,
    ) -> Result<Response<StoreOfflineMessageResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let store_offline_message_actor =
            crate::raft::session_state::session_state_raft_actor::StoreOfflineMessage {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                packet_key: inner.packet_key,
            };

        session_state_raft_actor_addr
            .send(store_offline_message_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to store offline message: {}", e)))?
            .map_err(|e| map_session_state_raft_error("store offline message", e))?;

        Ok(Response::new(StoreOfflineMessageResponse {}))
    }

    async fn pop_offline_message(
        &self,
        request: Request<PopOfflineMessageRequest>,
    ) -> Result<Response<PopOfflineMessageResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let pop_offline_message_actor =
            crate::raft::session_state::session_state_raft_actor::PopOfflineMessage {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
            };
        let res = session_state_raft_actor_addr
            .send(pop_offline_message_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to pop offline message: {}", e)))?
            .map_err(|e| map_session_state_raft_error("pop offline message", e))?;

        Ok(Response::new(PopOfflineMessageResponse {
            packet_key: res.0,
            packet_data: res.1,
        }))
    }

    async fn get_session_state(
        &self,
        request: Request<GetSessionStateRequest>,
    ) -> Result<Response<GetSessionStateResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let get_session_state_actor = session_state_raft_actor::GetSessionState {
            tenant_id: inner.tenant_id.clone(),
            client_id: inner.client_id.clone(),
        };
        let res = session_state_raft_actor_addr
            .send(get_session_state_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to get session state: {}", e)))?
            .map_err(|e| map_session_state_raft_error("get session state", e))?;

        let res_payload = serde_json::to_string(&res).map_err(|e| {
            Status::internal(format!("Failed to serialize session state response: {}", e))
        })?;

        Ok(Response::new(GetSessionStateResponse {
            payload: Some(res_payload),
            disconnected_at: None,
        }))
    }

    async fn create_session_state(
        &self,
        request: Request<CreateSessionStateRequest>,
    ) -> Result<Response<CreateSessionStateResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let create_session_state_actor = session_state_raft_actor::CreateSessionState {
            tenant_id: inner.tenant_id.clone(),
            client_id: inner.client_id.clone(),
            protocol_version: None,
            session_expiry_interval: None,
        };

        let r = session_state_raft_actor_addr
            .send(create_session_state_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to create session state: {}", e)))?;
        r.map_err(|e| map_session_state_raft_error("create session state", e))?;

        Ok(Response::new(CreateSessionStateResponse {}))
    }

    async fn delete_session_state(
        &self,
        request: Request<DeleteSessionStateRequest>,
    ) -> Result<Response<DeleteSessionStateResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let delete_session_state_actor = session_state_raft_actor::DeleteSessionState {
            tenant_id: inner.tenant_id.clone(),
            client_id: inner.client_id.clone(),
            expected_disconnected_at: None,
        };
        session_state_raft_actor_addr
            .send(delete_session_state_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete session state: {}", e)))?
            .map_err(|e| map_session_state_raft_error("delete session state", e))?;
        Ok(Response::new(DeleteSessionStateResponse {}))
    }

    async fn register_inflight_rx_packet(
        &self,
        request: Request<RegisterInflightRxPacketRequest>,
    ) -> Result<Response<RegisterInflightRxPacketResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let packet_id = u16::try_from(inner.packet_id).map_err(|_| {
            grpc_status::invalid_argument_status(
                "packet_id exceeds MQTT u16 range",
                "cluster_service",
            )
        })?;
        let register_inflight_rx_packet_actor =
            session_state_raft_actor::RegisterInflightRxPacket {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                packet_id,
                qos: inner.qos as u8,
                packet_key: inner.packet_key,
            };
        let r = session_state_raft_actor_addr
            .send(register_inflight_rx_packet_actor)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to register inflight RX packet: {}", e))
            })?;
        r.map_err(|e| map_session_state_raft_error("register inflight rx packet", e))?;
        Ok(Response::new(RegisterInflightRxPacketResponse {}))
    }

    async fn register_inflight_tx_packet(
        &self,
        request: Request<RegisterInflightTxPacketRequest>,
    ) -> Result<Response<RegisterInflightTxPacketResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let packet_id = u16::try_from(inner.packet_id).map_err(|_| {
            grpc_status::invalid_argument_status(
                "packet_id exceeds MQTT u16 range",
                "cluster_service",
            )
        })?;
        let register_inflight_tx_packet_actor =
            session_state_raft_actor::RegisterInflightTxPacket {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                packet_id,
                qos: inner.qos as u8,
                packet_key: inner.packet_key,
            };
        session_state_raft_actor_addr
            .send(register_inflight_tx_packet_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to register inflight TX packet: {}", e)))?
            .map_err(|e| map_session_state_raft_error("register inflight tx packet", e))?;
        Ok(Response::new(RegisterInflightTxPacketResponse {}))
    }

    async fn advance_inflight_state(
        &self,
        request: Request<AdvanceInflightStateRequest>,
    ) -> Result<Response<AdvanceInflightStateResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let packet_id = u16::try_from(inner.packet_id).map_err(|_| {
            grpc_status::invalid_argument_status(
                "packet_id exceeds MQTT u16 range",
                "cluster_service",
            )
        })?;
        let advance_inflight_state_actor = session_state_raft_actor::AdvanceInflightState {
            tenant_id: inner.tenant_id.clone(),
            client_id: inner.client_id.clone(),
            packet_id,
        };
        session_state_raft_actor_addr
            .send(advance_inflight_state_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to advance inflight state: {}", e)))?
            .map_err(|e| map_session_state_raft_error("advance inflight state", e))?;
        Ok(Response::new(AdvanceInflightStateResponse {}))
    }

    async fn get_current_inflight_packet(
        &self,
        request: Request<GetCurrentInflightPacketRequest>,
    ) -> Result<Response<GetCurrentInflightPacketResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let packet_id = u16::try_from(inner.packet_id).map_err(|_| {
            grpc_status::invalid_argument_status(
                "packet_id exceeds MQTT u16 range",
                "cluster_service",
            )
        })?;
        let get_current_inflight_packet_actor =
            session_state_raft_actor::GetCurrentInflightPacket {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                packet_id,
            };
        let res = session_state_raft_actor_addr
            .send(get_current_inflight_packet_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to get current inflight packet: {}", e)))?
            .map_err(|e| map_session_state_raft_error("get current inflight packet", e))?;
        let res_payload = serde_json::to_string(&res).map_err(|e| {
            Status::internal(format!(
                "Failed to serialize current inflight packet response: {}",
                e
            ))
        })?;
        Ok(Response::new(GetCurrentInflightPacketResponse {
            packet: Some(res_payload),
        }))
    }

    async fn get_next_inflight_packet(
        &self,
        request: Request<GetNextInflightPacketRequest>,
    ) -> Result<Response<GetNextInflightPacketResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let packet_id = u16::try_from(inner.packet_id).map_err(|_| {
            grpc_status::invalid_argument_status(
                "packet_id exceeds MQTT u16 range",
                "cluster_service",
            )
        })?;
        let get_next_inflight_packet_actor = session_state_raft_actor::GetNextInflightPacket {
            tenant_id: inner.tenant_id.clone(),
            client_id: inner.client_id.clone(),
            packet_id,
        };
        let res = session_state_raft_actor_addr
            .send(get_next_inflight_packet_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to get next inflight packet: {}", e)))?
            .map_err(|e| map_session_state_raft_error("get next inflight packet", e))?;
        let res_payload = serde_json::to_string(&res).map_err(|e| {
            Status::internal(format!(
                "Failed to serialize next inflight packet response: {}",
                e
            ))
        })?;
        Ok(Response::new(GetNextInflightPacketResponse {
            packet: Some(res_payload),
        }))
    }

    async fn subscribe_topic(
        &self,
        request: Request<SubscribeTopicRequest>,
    ) -> Result<Response<SubscribeTopicResponse>, Status> {
        let topic_raft_actor_addr =
            crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
        let inner = request.into_inner();
        let subscribe_topic_actor = crate::raft::topic::topic_raft_actor::Subscribe {
            tenant_id: inner.tenant_id.clone(),
            client_identifier: inner.client_id.clone(),
            topic: inner.topic.clone(),
            qos: inner.qos as u8,
            no_local: inner.no_local,
            retain_as_published: inner.retain_as_published,
        };
        let _ = topic_raft_actor_addr
            .send(subscribe_topic_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to subscribe topic: {}", e)))?
            .map_err(|e| map_topic_raft_error("subscribe topic", e))?;
        Ok(Response::new(SubscribeTopicResponse {}))
    }

    async fn unsubscribe_topic(
        &self,
        request: Request<UnsubscribeTopicRequest>,
    ) -> Result<Response<UnsubscribeTopicResponse>, Status> {
        let topic_raft_actor_addr =
            crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
        let inner = request.into_inner();
        let unsubscribe_topic_actor = crate::raft::topic::topic_raft_actor::Unsubscribe {
            tenant_id: inner.tenant_id.clone(),
            client_identifier: inner.client_id.clone(),
            topic: inner.topic.clone(),
        };
        topic_raft_actor_addr
            .send(unsubscribe_topic_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to unsubscribe topic: {}", e)))?
            .map_err(|e| map_topic_raft_error("unsubscribe topic", e))?;
        Ok(Response::new(UnsubscribeTopicResponse {}))
    }

    async fn get_subscribers_by_topic(
        &self,
        request: Request<GetSubscribersByTopicRequest>,
    ) -> Result<Response<GetSubscribersByTopicResponse>, Status> {
        let topic_raft_actor_addr =
            crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
        let inner = request.into_inner();
        let get_subscribers_by_topic_actor =
            crate::raft::topic::topic_raft_actor::GetSubscriptions {
                tenant_id: inner.tenant_id.clone(),
                topic: inner.topic.clone(),
            };
        let res = topic_raft_actor_addr
            .send(get_subscribers_by_topic_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to get subscribers by topic: {}", e)))?
            .map_err(|e| map_topic_raft_error("get subscribers by topic", e))?;

        let payload = res
            .subscriptions
            .iter()
            .map(|sub| crate::protobuf::Subscriber {
                qos: sub.qos as u32,
                client_id: sub.client_identifier.clone(),
                no_local: sub.no_local,
                retain_as_published: sub.retain_as_published,
            })
            .collect::<Vec<_>>();

        Ok(Response::new(GetSubscribersByTopicResponse { payload }))
    }

    async fn register_retain_publish_message(
        &self,
        request: Request<RegisterRetainPublishMessageRequest>,
    ) -> Result<Response<RegisterRetainPublishMessageResponse>, Status> {
        let topic_raft_actor_addr =
            crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
        let inner = request.into_inner();
        let retain_publish_message =
            deserialize_stored_packet_from_str(&inner.payload).map_err(|e| {
                grpc_status::invalid_argument_status(
                    format!("Invalid retain publish message format: {}", e),
                    "cluster_service",
                )
            })?;
        let register_retain_publish_message_actor =
            crate::raft::topic::topic_raft_actor::RegisterRetainPublishPacket {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                publish_packet: retain_publish_message,
            };
        topic_raft_actor_addr
            .send(register_retain_publish_message_actor)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to register retain publish message: {}", e))
            })?
            .map_err(|e| map_topic_raft_error("register retain publish message", e))?;
        Ok(Response::new(RegisterRetainPublishMessageResponse {}))
    }

    async fn get_retain_publish_message(
        &self,
        request: Request<GetRetainPublishMessageRequest>,
    ) -> Result<Response<GetRetainPublishMessageResponse>, Status> {
        let topic_raft_actor_addr =
            crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
        let inner = request.into_inner();
        let get_retain_publish_message_actor =
            crate::raft::topic::topic_raft_actor::GetRetainPublishPacketEnsureLinearizable {
                tenant_id: inner.tenant_id.clone(),
                topic: inner.topic.clone(),
            };
        let res = topic_raft_actor_addr
            .send(get_retain_publish_message_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to get retain publish message: {}", e)))?
            .map_err(|e| map_topic_raft_error("get retain publish message", e))?;
        let res_payload = serde_json::to_string(&res).map_err(|e| {
            Status::internal(format!(
                "Failed to serialize retain publish message response: {}",
                e
            ))
        })?;
        Ok(Response::new(GetRetainPublishMessageResponse {
            payload: Some(res_payload),
        }))
    }

    async fn clean_retain_publish_message(
        &self,
        request: Request<CleanRetainPublishMessageRequest>,
    ) -> Result<Response<CleanRetainPublishMessageResponse>, Status> {
        let topic_raft_actor_addr =
            crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
        let inner = request.into_inner();
        let clean_retain_publish_message_actor =
            crate::raft::topic::topic_raft_actor::CleanRetainPublishPacket {
                tenant_id: inner.tenant_id.clone(),
                topic_filter: inner.topic.clone(),
            };
        topic_raft_actor_addr
            .send(clean_retain_publish_message_actor)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to clean retain publish message: {}", e))
            })?
            .map_err(|e| map_topic_raft_error("clean retain publish message", e))?;
        Ok(Response::new(CleanRetainPublishMessageResponse {}))
    }

    async fn register_session_actor_map(
        &self,
        request: Request<RegisterSessionActorMapRequest>,
    ) -> Result<Response<RegisterSessionActorMapResponse>, Status> {
        let session_actor_map_raft_actor_addr =
            session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
        let inner = request.into_inner();
        let version = match inner.session_version {
            Some(s) => SessionVersion {
                counter: s.counter,
                node_id: s.node_id,
            },
            None => {
                return Err(grpc_status::invalid_argument_status(
                    "Session version is required for registering session actor map",
                    "cluster_service",
                ));
            }
        };
        let register_session_actor_map_actor =
            session_actor_map_raft_actor::RegisterSessionActorMap {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                node_id: inner.node_id,
                version,
            };
        session_actor_map_raft_actor_addr
            .send(register_session_actor_map_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to register session actor map: {}", e)))?
            .map_err(|e| map_session_actor_map_raft_error("register session actor map", e))?;
        Ok(Response::new(RegisterSessionActorMapResponse {}))
    }

    async fn un_register_session_actor_map(
        &self,
        request: Request<UnregisterSessionActorMapRequest>,
    ) -> Result<Response<UnregisterSessionActorMapResponse>, Status> {
        let session_actor_map_raft_actor_addr =
            session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
        let inner = request.into_inner();

        let version = match inner.session_version {
            Some(s) => SessionVersion {
                counter: s.counter,
                node_id: s.node_id,
            },
            None => {
                return Err(grpc_status::invalid_argument_status(
                    "Session version is required for unregistering session actor map",
                    "cluster_service",
                ));
            }
        };

        let unregister_session_actor_map_actor =
            session_actor_map_raft_actor::UnregisterSessionActorMap {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                version,
            };
        session_actor_map_raft_actor_addr
            .send(unregister_session_actor_map_actor)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to unregister session actor map: {}", e))
            })?
            .map_err(|e| map_session_actor_map_raft_error("unregister session actor map", e))?;
        Ok(Response::new(UnregisterSessionActorMapResponse {}))
    }

    async fn renew_session_lease(
        &self,
        request: Request<RenewSessionLeaseRequest>,
    ) -> Result<Response<RenewSessionLeaseResponse>, Status> {
        let session_actor_map_raft_actor_addr =
            session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
        let inner = request.into_inner();
        let renew_session_lease_actor = session_actor_map_raft_actor::RenewSession {
            tenant_id: inner.tenant_id.clone(),
            client_id: inner.client_id.clone(),
        };
        session_actor_map_raft_actor_addr
            .send(renew_session_lease_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to renew session lease: {}", e)))?
            .map_err(|e| map_session_actor_map_raft_error("renew session lease", e))?;
        Ok(Response::new(RenewSessionLeaseResponse {}))
    }

    async fn get_session_actor_map(
        &self,
        request: Request<GetSessionActorMapRequest>,
    ) -> Result<Response<GetSessionActorMapResponse>, Status> {
        let session_actor_map_raft_actor_addr =
            session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
        let inner = request.into_inner();
        let get_session_actor_map_actor =
            session_actor_map_raft_actor::GetSessionActorMapLinearizable {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
            };
        let res = session_actor_map_raft_actor_addr
            .send(get_session_actor_map_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to get session actor map: {}", e)))?
            .map_err(|e| map_session_actor_map_raft_error("get session actor map", e))?;
        let res_payload = serde_json::to_string(&res).map_err(|e| {
            Status::internal(format!(
                "Failed to serialize session actor map response: {}",
                e
            ))
        })?;
        Ok(Response::new(GetSessionActorMapResponse {
            payload: Some(res_payload),
        }))
    }

    async fn route_packet(
        &self,
        request: Request<crate::protobuf::RoutePacketRequest>,
    ) -> Result<Response<crate::protobuf::RoutePacketResponse>, Status> {
        let inner = request.into_inner();
        let packet = deserialize_stored_packet_from_str(&inner.payload).map_err(|e| {
            grpc_status::invalid_argument_status(
                format!("Invalid packet format: {}", e),
                "cluster_service",
            )
        })?;

        let router_actor = if let Packet::Publish(ref publish) = packet {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            std::hash::Hash::hash(&publish.topic_name, &mut hasher);
            let hash = std::hash::Hasher::finish(&hasher);
            &self.router_actors[hash as usize % self.router_actors.len()]
        } else {
            &self.router_actors[0]
        };

        router_actor
            .send(RouteFromOtherNode {
                tenant_id: inner.tenant_id.clone(),
                packet,
                route_id: inner.route_id,
                source_node_id: inner.source_node_id,
                target_client_id: inner.target_client_id,
                target_qos: inner.target_qos as u8,
                expiry_at: inner.expiry_at,
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to route packet: {}", e)))?
            .map_err(|e| Status::internal(format!("Error in routing packet: {}", e)))?;

        Ok(Response::new(crate::protobuf::RoutePacketResponse {}))
    }

    async fn force_stop_session_actor(
        &self,
        request: Request<ForceStopSessionActorRequest>,
    ) -> Result<Response<ForceStopSessionActorResponse>, Status> {
        let session_manager_actor_addr = SessionManagerActor::from_registry();
        let inner = request.into_inner();
        let force_stop_result = session_manager_actor_addr
            .send(crate::session::session_manager_actor::ForceStop {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to force stop session actor: {}", e)))?;
        if let Err(e) = force_stop_result {
            match e {
                crate::session::session_manager_actor::SessionManagerError::SessionNotExisted(
                    client_id,
                ) if client_id == inner.client_id => {}
                _ => {
                    return Err(Status::internal(format!(
                        "Error in force stopping session actor: {}",
                        e
                    )));
                }
            }
        }
        Ok(Response::new(ForceStopSessionActorResponse {}))
    }

    async fn get_session_info(
        &self,
        request: Request<crate::protobuf::GetSessionInfoRequest>,
    ) -> Result<Response<crate::protobuf::GetSessionInfoResponse>, Status> {
        let session_manager_actor_addr = SessionManagerActor::from_registry();
        let inner = request.into_inner();

        let result = session_manager_actor_addr
            .send(crate::session::session_manager_actor::GetSessionInfo {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to get session info: {}", e)))?
            .map_err(|e| match e {
                crate::session::session_manager_actor::SessionManagerError::SessionNotExisted(
                    client_id,
                ) => grpc_status::not_found_status(
                    format!("Session not existed: {}", client_id),
                    "session_manager",
                ),
                _ => Status::internal(format!("Error in getting session info: {}", e)),
            })?;

        let session_state = match result.session_state {
            crate::session::session_actor::ActivityState::Active => {
                crate::protobuf::ActivityState::Active
            }
            crate::session::session_actor::ActivityState::Inactive => {
                crate::protobuf::ActivityState::Inactive
            }
        };

        let payload = crate::protobuf::SessionInfo {
            tenant_identifier: result.tenant_identifier,
            client_identifier: result.client_identifier,
            subscription_topics: result.subscription_topics,
            session_state: session_state.into(),
            ip_address: result.ip_address,
            connected: result.connected,
            connected_at: result.connected_at,
            created_at: result.created_at,
            disconnected_at: result.disconnected_at,
            messages_received: result.messages_received,
            messages_sent: result.messages_sent,
        };

        Ok(Response::new(crate::protobuf::GetSessionInfoResponse {
            payload: Some(payload),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_session_state_raft_error_preserves_remote_business_code() {
        let status = map_session_state_raft_error(
            "op",
            crate::raft::session_state::session_state_raft_actor::SessionStateRaftError::GRPCBusiness(
                crate::raft::GRPCBusinessError::new(
                    tonic::Code::AlreadyExists,
                    grpc_status::business_detail(
                        crate::protobuf::ErrorCode::PacketIdentifierAlreadyExists,
                        "duplicate packet",
                        "session_state_raft",
                    ),
                ),
            ),
        );
        let parsed = grpc_status::decode_status(&status);

        assert_eq!(status.code(), tonic::Code::AlreadyExists);
        assert_eq!(
            parsed.error_kind.as_deref(),
            Some(grpc_status::ERROR_KIND_BUSINESS)
        );
        assert_eq!(
            parsed.detail.as_ref().map(|detail| detail.code()),
            Some(crate::protobuf::ErrorCode::PacketIdentifierAlreadyExists)
        );
    }

    #[test]
    fn map_session_actor_map_raft_error_publishes_leader_node_metadata() {
        let status = map_session_actor_map_raft_error(
            "op",
            crate::raft::session_actor_map::session_actor_map_raft_actor::SessionActorMapRaftError::NotLeader {
                leader: Some(crate::raft::Node {
                    node_id: Some(7),
                    rpc_addr: "10.0.0.7:9080".to_string(),
                    api_addr: String::new(),
                }),
            },
        );
        let parsed = grpc_status::decode_status(&status);

        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            parsed.error_kind.as_deref(),
            Some(grpc_status::ERROR_KIND_LEADER_REDIRECT)
        );
        assert_eq!(parsed.leader_node_id, Some(7));
        assert_eq!(parsed.leader_addr.as_deref(), Some("10.0.0.7:9080"));
    }
}
