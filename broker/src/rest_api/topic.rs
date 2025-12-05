use std::sync::Arc;

use actix::SystemService;
use axum::{
    extract::{Path, Query, State}, http::StatusCode, response::IntoResponse, Json
};
use log::error;
use serde::Serialize;
use crate::{app::YedMQApp, raft::topic::topic_raft_actor::{self, TopicRaftActor}};

use super::{Pagination, PaginationListResult, PaginationMeta};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Topic {
    pub topic: String,
    pub client_id: String,
    pub qos: u8,
}

pub async fn topic_list(
    State(_): State<Arc<YedMQApp>>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> impl IntoResponse {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let topic_raft_actor_addr = TopicRaftActor::from_registry();
    let topic_list_result = topic_raft_actor_addr.send(
        topic_raft_actor::GetTopicListWithPagination{
            tenant_id: tenant_id.clone(),
            offset: offset_param,
            limit: limit_param
        }
    ).await.unwrap();

    if let Err(err) = topic_list_result {
        if let topic_raft_actor::TopicRaftError::TopicError(topic_error) = err {
            let error_response = match topic_error {
                crate::topic::TopicError::TenantNotFound(_) => {
                    let error_response = super::ErrorResponse {
                        code: 3,
                        message: format!("tenant {} not existed", tenant_id),
                    };
                    (StatusCode::NOT_FOUND, Json(error_response)).into_response()
                },
                _ => {
                    error!("get retain message list error: {}", topic_error);
                    let error_response = super::ErrorResponse {
                        code: 101,
                        message: "Internal Server Error".to_string(),
                    };
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
                }
            };
            error_response
        } else {
            error!("get topic list error: {}", err);
            let error_response = super::ErrorResponse {
                code: 101,
                message: "Internal Server Error".to_string(),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    } else {
        let topic_list = topic_list_result.unwrap();
        let mut result = Vec::<Topic>::new();

        for topic_info in topic_list.data {
            let topic = Topic {
                topic: topic_info.topic,
                client_id: topic_info.client_id,
                qos: topic_info.qos,
            };
            result.push(topic);
        }
        let meta = PaginationMeta {
            offset: offset_param,
            limit: limit_param,
            total: topic_list.total,
        };

        let result = PaginationListResult { meta, data: result };

        (StatusCode::OK, Json(result)).into_response()
    }
}