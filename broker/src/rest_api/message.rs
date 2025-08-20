use std::sync::Arc;

use crate::app::YedMQApp;
use actix::SystemService;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use log::error;
use serde::Serialize;

use super::{Pagination, PaginationListResult, PaginationMeta};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetainMessage {
    pub topic: String,
    pub qos: u8,
    pub client_identifier: String,
}

pub async fn clean_retain_message(
    State(app_state): State<Arc<YedMQApp>>,
    Path((tenant_id, topic_filter)): Path<(String, String)>,
) -> impl IntoResponse {
    let topic_raft_actor_addr = crate::raft::topic::topic_raft_actor::TopicRaftActor::from_registry();
    let r = topic_raft_actor_addr.send(
        crate::raft::topic::topic_raft_actor::CleanRetainPublishPacket {
            tenant_id: tenant_id.clone(),
            topic_filter: topic_filter.clone(),
        },
    ).await.unwrap();
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
    State(app_state): State<Arc<YedMQApp>>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> impl IntoResponse {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let r = app_state
        .topic_manager
        .read()
        .await
        .get_retain_message_list_with_pagination(tenant_id.as_str(), offset_param, limit_param)
        .await;

    if let Err(err) = r {
        if let Some(topic_error) = err.downcast_ref::<crate::topic::TopicError>() {
            let error_response = match topic_error {
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
            };
            return error_response;
        } else {
            error!("get retain message list error: {}", err);
            let error_response = super::ErrorResponse {
                code: 101,
                message: "Internal Server Error".to_string(),
            };
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response();
        }
    } else {
        let retain_message_list = r.unwrap();
        let mut result = Vec::<RetainMessage>::new();

        for (topic, client_identifier, qos) in retain_message_list.1 {
            let retain_message = RetainMessage {
                topic,
                qos,
                client_identifier,
            };
            result.push(retain_message);
        }
        let meta = PaginationMeta {
            offset: offset_param,
            limit: limit_param,
            total: retain_message_list.0,
        };

        let result = PaginationListResult { meta, data: result };
        (StatusCode::OK, Json(result)).into_response()
    }
}
