use crate::protobuf::cluster_service_server::ClusterService;
use crate::protobuf::{
    AdvanceInflightStateRequest, AdvanceInflightStateResponse, CleanRetainPublishMessageRequest, CleanRetainPublishMessageResponse, CreateSessionStateRequest, CreateSessionStateResponse, DeleteSessionStateRequest, DeleteSessionStateResponse, ForceStopSessionActorRequest, ForceStopSessionActorResponse, GetCurrentInflightPacketRequest, GetCurrentInflightPacketResponse, GetNextInflightPacketRequest, GetNextInflightPacketResponse, GetRetainPublishMessageRequest, GetRetainPublishMessageResponse, GetSessionActorMapRequest, GetSessionActorMapResponse, GetSessionStateRequest, GetSessionStateResponse, GetSubscribersByTopicRequest, GetSubscribersByTopicResponse, PopOfflineMessageRequest, PopOfflineMessageResponse, RegisterInflightRxPacketRequest, RegisterInflightRxPacketResponse, RegisterInflightTxPacketRequest, RegisterInflightTxPacketResponse, RegisterRetainPublishMessageRequest, RegisterRetainPublishMessageResponse, RegisterSessionActorMapRequest, RegisterSessionActorMapResponse, RenewSessionLeaseRequest, RenewSessionLeaseResponse, StoreOfflineMessageRequest, StoreOfflineMessageResponse, SubscribeTopicRequest, SubscribeTopicResponse, UnregisterSessionActorMapRequest, UnregisterSessionActorMapResponse, UnsubscribeTopicRequest, UnsubscribeTopicResponse
};
use crate::raft::session_actor_map::session_actor_map_raft_actor;
use crate::raft::session_state::session_state_raft_actor::{self, SessionStateRaftActor};
use crate::router_actor::{RouteFromOtherNode, RoutePacket, RouterActor};
use crate::session::session_actor_map_storage::SessionVersion;
use crate::session::session_manager_actor::SessionManagerActor;
use actix::SystemService;
use tonic::{Request, Response, Status};
use yedmq_mqtt::MqttPacketV3;

pub struct ClusterServiceImpl;

#[tonic::async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn store_offline_message(
        &self,
        request: Request<StoreOfflineMessageRequest>,
    ) -> Result<Response<StoreOfflineMessageResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let packet = serde_json::from_str(&inner.payload)
            .map_err(|e| Status::invalid_argument(format!("Invalid payload format: {}", e)))?;
        let store_offline_message_actor =
            crate::raft::session_state::session_state_raft_actor::StoreOfflineMessage {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                packets: packet,
            };
        session_state_raft_actor_addr
            .send(store_offline_message_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to store offline message: {}", e)))?;
        Ok(Response::new(StoreOfflineMessageResponse {
            success: true,
            error: None,
        }))
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
            .map_err(|e| Status::internal(format!("Error in popping offline message: {}", e)))?;
        let payload = res.map(|p| serde_json::to_string(&p).unwrap());

        Ok(Response::new(PopOfflineMessageResponse {
            success: true,
            error: None,
            payload,
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
            .map_err(|e| Status::internal(format!("Error in getting session state: {}", e)))?;

        let payload = res.map(|p| serde_json::to_string(&p).unwrap());

        Ok(Response::new(GetSessionStateResponse {
            success: true,
            error: None,
            payload,
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
        };
        session_state_raft_actor_addr
            .send(create_session_state_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to create session state: {}", e)))?;
        Ok(Response::new(CreateSessionStateResponse {
            success: true,
            error: None,
        }))
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
        };
        session_state_raft_actor_addr
            .send(delete_session_state_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete session state: {}", e)))?;
        Ok(Response::new(DeleteSessionStateResponse {
            success: true,
            error: None,
        }))
    }

    async fn register_inflight_rx_packet(
        &self,
        request: Request<RegisterInflightRxPacketRequest>,
    ) -> Result<Response<RegisterInflightRxPacketResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let inflight_rx_packet = serde_json::from_str(&inner.payload).map_err(|e| {
            Status::invalid_argument(format!("Invalid inflight packet format: {}", e))
        })?;
        let register_inflight_rx_packet_actor =
            session_state_raft_actor::RegisterInflightRxPacket {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                inflight_rx_packet,
            };
        session_state_raft_actor_addr
            .send(register_inflight_rx_packet_actor)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to register inflight RX packet: {}", e))
            })?;
        Ok(Response::new(RegisterInflightRxPacketResponse {
            success: true,
            error: None,
        }))
    }

    async fn register_inflight_tx_packet(
        &self,
        request: Request<RegisterInflightTxPacketRequest>,
    ) -> Result<Response<RegisterInflightTxPacketResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let inflight_tx_packet = serde_json::from_str(&inner.payload).map_err(|e| {
            Status::invalid_argument(format!("Invalid inflight packet format: {}", e))
        })?;
        let register_inflight_tx_packet_actor =
            session_state_raft_actor::RegisterInflightTxPacket {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                inflight_tx_packet,
            };
        session_state_raft_actor_addr
            .send(register_inflight_tx_packet_actor)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to register inflight TX packet: {}", e))
            })?;
        Ok(Response::new(RegisterInflightTxPacketResponse {
            success: true,
            error: None,
        }))
    }

    async fn advance_inflight_state(
        &self,
        request: Request<AdvanceInflightStateRequest>,
    ) -> Result<Response<AdvanceInflightStateResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let advance_inflight_state_actor = session_state_raft_actor::AdvanceInflightState {
            tenant_id: inner.tenant_id.clone(),
            client_id: inner.client_id.clone(),
            packet_id: inner.packet_id,
        };
        session_state_raft_actor_addr
            .send(advance_inflight_state_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to advance inflight state: {}", e)))?;
        Ok(Response::new(AdvanceInflightStateResponse {
            success: true,
            error: None,
        }))
    }

    async fn get_current_inflight_packet(
        &self,
        request: Request<GetCurrentInflightPacketRequest>,
    ) -> Result<Response<GetCurrentInflightPacketResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let get_current_inflight_packet_actor =
            session_state_raft_actor::GetCurrentInflightPacket {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                packet_id: inner.packet_id,
            };
        let res = session_state_raft_actor_addr
            .send(get_current_inflight_packet_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to get current inflight packet: {}", e)))?
            .map_err(|e| {
                Status::internal(format!("Error in getting current inflight packet: {}", e))
            })?;
        let payload = res.map(|p| serde_json::to_string(&p).unwrap());
        Ok(Response::new(GetCurrentInflightPacketResponse {
            success: true,
            error: None,
            packet: payload,
        }))
    }

    async fn get_next_inflight_packet(
        &self,
        request: Request<GetNextInflightPacketRequest>,
    ) -> Result<Response<GetNextInflightPacketResponse>, Status> {
        let session_state_raft_actor_addr = SessionStateRaftActor::from_registry();
        let inner = request.into_inner();
        let get_next_inflight_packet_actor = session_state_raft_actor::GetNextInflightPacket {
            tenant_id: inner.tenant_id.clone(),
            client_id: inner.client_id.clone(),
            packet_id: inner.packet_id,
        };
        let res = session_state_raft_actor_addr
            .send(get_next_inflight_packet_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to get next inflight packet: {}", e)))?
            .map_err(|e| {
                Status::internal(format!("Error in getting next inflight packet: {}", e))
            })?;
        let payload = res.map(|p| serde_json::to_string(&p).unwrap());
        Ok(Response::new(GetNextInflightPacketResponse {
            success: true,
            error: None,
            packet: payload,
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
        };
        let res = topic_raft_actor_addr
            .send(subscribe_topic_actor)
            .await
            .map_err(|e| Status::internal(format!("Failed to subscribe topic: {}", e)))?
            .map_err(|e| Status::internal(format!("Error in subscribing topic: {}", e)))?;
        Ok(Response::new(SubscribeTopicResponse {
            success: true,
            error: None,
        }))
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
            .map_err(|e| Status::internal(format!("Failed to unsubscribe topic: {}", e)))?;
        Ok(Response::new(UnsubscribeTopicResponse {
            success: true,
            error: None,
        }))
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
            .map_err(|e| {
                Status::internal(format!("Error in getting subscribers by topic: {}", e))
            })?;

        let payload = res
            .subscriptions
            .iter()
            .map(|sub| crate::protobuf::Subscriber {
                qos: sub.qos as u32,
                client_id: sub.client_identifier.clone(),
            })
            .collect::<Vec<_>>();

        Ok(Response::new(GetSubscribersByTopicResponse {
            success: true,
            error: None,
            payload,
        }))
    }

    async fn register_retain_publish_message(
        &self,
        request: Request<RegisterRetainPublishMessageRequest>,
    ) -> Result<Response<RegisterRetainPublishMessageResponse>, Status> {
        let topic_raft_actor_addr =
            crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
        let inner = request.into_inner();
        let retain_publish_message = serde_json::from_str(&inner.payload).map_err(|e| {
            Status::invalid_argument(format!("Invalid retain publish message format: {}", e))
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
            })?;
        Ok(Response::new(RegisterRetainPublishMessageResponse {
            success: true,
            error: None,
        }))
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
            .map_err(|e| {
                Status::internal(format!("Error in getting retain publish message: {}", e))
            })?;
        let payload = serde_json::to_string(&res).unwrap_or_default();
        Ok(Response::new(GetRetainPublishMessageResponse {
            success: true,
            error: None,
            payload: Some(payload),
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
            })?;
        Ok(Response::new(CleanRetainPublishMessageResponse {
            success: true,
            error: None,
        }))
    }

    async fn register_session_actor_map(
        &self,
        request: Request<RegisterSessionActorMapRequest>,
    ) -> Result<Response<RegisterSessionActorMapResponse>, Status> {
        let session_actor_map_raft_actor_addr =
            session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
        let inner = request.into_inner();
        let version = SessionVersion {
            counter: inner.session_version.unwrap().counter,
            node_id: inner.node_id.clone(),
        };
        let register_session_actor_map_actor =
            session_actor_map_raft_actor::RegisterSessionActorMap {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
                node_id: inner.node_id.clone(),
                version,
            };
        session_actor_map_raft_actor_addr
            .send(register_session_actor_map_actor)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to register session actor map: {}", e))
            })?;
        Ok(Response::new(RegisterSessionActorMapResponse {
            success: true,
            error: None,
        }))
    }

    async fn un_register_session_actor_map(
        &self,
        request: Request<UnregisterSessionActorMapRequest>,
    ) -> Result<Response<UnregisterSessionActorMapResponse>, Status> {
        let session_actor_map_raft_actor_addr =
            session_actor_map_raft_actor::SessionActorMapRaftActor::from_registry();
        let inner = request.into_inner();
        let version = SessionVersion {
            counter: inner.session_version.unwrap().counter,
            node_id: inner.session_version.unwrap().node_id,
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
            })?;
        Ok(Response::new(UnregisterSessionActorMapResponse {
            success: true,
            error: None,
        }))
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
            .map_err(|e| Status::internal(format!("Failed to renew session lease: {}", e)))?;
        Ok(Response::new(RenewSessionLeaseResponse {
            success: true,
            error: None,
        }))
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
            .map_err(|e| Status::internal(format!("Error in getting session actor map: {}", e)))?;
        let payload = res.map(|m| serde_json::to_string(&m).unwrap_or_default());
        Ok(Response::new(GetSessionActorMapResponse {
            success: true,
            error: None,
            payload,
        }))
    }

    async fn route_packet(
        &self,
        request: Request<crate::protobuf::RoutePacketRequest>,
    ) -> Result<Response<crate::protobuf::RoutePacketResponse>, Status> {
        let inner = request.into_inner();
        let packet: MqttPacketV3 = serde_json::from_str(&inner.payload)
            .map_err(|e| Status::invalid_argument(format!("Invalid packet format: {}", e)))?;

        let router_actor_addr = RouterActor::from_registry();
        router_actor_addr
            .send(RouteFromOtherNode {
                tenant_id: inner.tenant_id.clone(),
                packet
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to route packet: {}", e)))?;
        
        Ok(Response::new(crate::protobuf::RoutePacketResponse {
            success: true,
            error: None,
        }))
    }

    async fn force_stop_session_actor(
        &self,
        request: Request<ForceStopSessionActorRequest>,
    ) -> Result<Response<ForceStopSessionActorResponse>, Status> {
        let session_manager_actor_addr = SessionManagerActor::from_registry();
        let inner = request.into_inner();
        session_manager_actor_addr.send(
            crate::session::session_manager_actor::ForceStop {
                tenant_id: inner.tenant_id.clone(),
                client_id: inner.client_id.clone(),
            }
        ).await
        .map_err(|e| Status::internal(format!("Failed to force stop session actor: {}", e)))?;
        Ok(Response::new(ForceStopSessionActorResponse {
            success: true,
            error: None,
        }))
    }
}
