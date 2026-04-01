use std::sync::Arc;

use super::{Pagination, PaginationListResult, PaginationMeta};
use crate::app::YedMQApp;
use crate::router_actor::RoutePacket;
use actix::SystemService;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use axum_macros::debug_handler;
use base64::{engine::general_purpose, Engine as _};
use bytes::Bytes;
use log::error;
use serde::{Deserialize, Serialize};
use yedmq_mqtt::v3::publish::PublishPacketBuilder;
use yedmq_mqtt::MqttPacketV3;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishMessage {
    topic: String,
    payload: String,
    qos: u8,
    retain: bool,
    payload_encoding: Option<String>, // e.g., "base64" "plain" , if not present, plain is assumed
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetainMessage {
    pub topic: String,
    pub qos: u8,
    pub client_identifier: String,
}

pub async fn clean_retain_message(
    State(_): State<Arc<YedMQApp>>,
    Path((tenant_id, topic_filter)): Path<(String, String)>,
) -> impl IntoResponse {
    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let r = topic_raft_actor_addr
        .send(
            crate::raft::topic::topic_raft_actor::CleanRetainPublishPacket {
                tenant_id: tenant_id.clone(),
                topic_filter: topic_filter.clone(),
            },
        )
        .await;
    let r = match r {
        Ok(r) => r,
        Err(err) => {
            error!("clean retain message mailbox error: {}", err);
            let error_response = super::ErrorResponse {
                code: 101,
                message: "Internal Server Error".to_string(),
            };
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response();
        }
    };
    if let Err(err) = r {
        error!("clean retain message error: {}", err);
        let error_response = super::ErrorResponse {
            code: 101,
            message: "Internal Server Error".to_string(),
        };
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response();
    }
    StatusCode::OK.into_response()
}

pub async fn retain_message_list(
    State(_): State<Arc<YedMQApp>>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> impl IntoResponse {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let topic_raft_actor_addr =
        crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let r = topic_raft_actor_addr
        .send(
            crate::raft::topic::topic_raft_actor::GetRetainMessageListWithPagination {
                tenant_id: tenant_id.clone(),
                offset: offset_param,
                limit: limit_param,
            },
        )
        .await;

    match r {
        Ok(Ok(retain_message_list)) => {
            let mut result = Vec::<RetainMessage>::new();

            for retain_message in retain_message_list.data {
                let retain_message = RetainMessage {
                    topic: retain_message.topic,
                    qos: retain_message.qos,
                    client_identifier: retain_message.client_id,
                };
                result.push(retain_message);
            }
            let meta = PaginationMeta {
                offset: offset_param,
                limit: limit_param,
                total: retain_message_list.total,
            };

            let result = PaginationListResult { meta, data: result };
            (StatusCode::OK, Json(result)).into_response()
        }
        Ok(Err(err)) => {
            if let crate::raft::topic::topic_raft_actor::TopicRaftError::TopicError(topic_error) =
                err
            {
                match topic_error {
                    crate::topic::TopicError::TenantNotFound(_) => {
                        let error_response = super::ErrorResponse {
                            code: 3,
                            message: format!("tenant {} not existed", tenant_id),
                        };
                        (StatusCode::NOT_FOUND, Json(error_response)).into_response()
                    }
                    _ => {
                        error!("get retain message list error: {}", topic_error);
                        let error_response = super::ErrorResponse {
                            code: 101,
                            message: "Internal Server Error".to_string(),
                        };
                        (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
                    }
                }
            } else {
                error!("get retain message list error: {}", err);
                let error_response = super::ErrorResponse {
                    code: 101,
                    message: "Internal Server Error".to_string(),
                };
                (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
            }
        }
        Err(err) => {
            error!("get retain message list mailbox error: {}", err);
            let error_response = super::ErrorResponse {
                code: 101,
                message: "Internal Server Error".to_string(),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

#[debug_handler]
pub async fn publish_message(
    State(app): State<Arc<YedMQApp>>,
    Path(tenant_id): Path<String>,
    Json(payload): Json<PublishMessage>,
) -> impl IntoResponse {
    let playload_encoding = payload.payload_encoding.unwrap_or("plain".to_string());

    let payload_bytes = if playload_encoding == "base64" {
        match general_purpose::STANDARD.decode(&payload.payload) {
            Ok(bytes) => Bytes::from(bytes),
            Err(_) => {
                let error_response = super::ErrorResponse {
                    code: 4,
                    message: "invalid base64 payload".to_string(),
                };
                return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
            }
        }
    } else if playload_encoding == "plain" {
        Bytes::from(payload.payload)
    } else {
        let error_response = super::ErrorResponse {
            code: 5,
            message: "unsupported payload encoding".to_string(),
        };
        return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
    };

    let router = app.service_registry.routers.first();
    let publish_packet = PublishPacketBuilder::new(payload.topic, payload_bytes)
        .qos(payload.qos)
        .build();
    router
        .expect("router actor not found")
        .do_send(RoutePacket {
            tenant_id,
            packet: MqttPacketV3::Publish(publish_packet),
        });
    StatusCode::OK.into_response()
}
