use std::sync::Arc;

use axum::{
    extract::{Path, Query, State}, http::StatusCode, response::IntoResponse, Json
};
use log::error;
use serde::Serialize;
use crate::{app::YedMQApp, topic::topic_manager::TopicManagerTrait};

use super::{Pagination, PaginationListResult, PaginationMeta};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Topic {
    pub topic: String,
    pub client_id: String,
    pub qos: u8,
}

pub async fn topic_list(
    State(app_state): State<Arc<YedMQApp>>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> impl IntoResponse {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let topic_list_result = app_state
        .topic_manager
        .read()
        .await
        .get_topic_list_with_pagination(&tenant_id, offset_param, limit_param).await;

    if let Err(err) = topic_list_result {

        let error = err.downcast_ref::<crate::topic::topic_storage::Error>();
        if let Some(topic_error) = error {
            let error_response = match topic_error {
                crate::topic::topic_storage::Error::TenantNotFound(_) => {
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
            return error_response;
        } else {
            error!("get topic list error: {}", err);
            let error_response = super::ErrorResponse {
                code: 101,
                message: "Internal Server Error".to_string(),
            };
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response();
        }
    } else {
        let topic_list = topic_list_result.unwrap();
        let mut result = Vec::<Topic>::new();

        for (topic, client_id, qos) in topic_list.1 {
            let topic = Topic {
                topic,
                client_id,
                qos,
            };
            result.push(topic);
        }
        let meta = PaginationMeta {
            offset: offset_param,
            limit: limit_param,
            total: topic_list.0,
        };

        let result = PaginationListResult { meta, data: result };

        (StatusCode::OK, Json(result)).into_response()
    }
}