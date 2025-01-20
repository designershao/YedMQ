use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use log::error;
use serde::Serialize;
use super::{AppState, Pagination, PaginationListResult, PaginationMeta};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Topic {
    pub topic: String,
    pub client_id: String,
    pub qos: u8,
}

pub async fn topic_list(
    State(app_state): State<AppState>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> (StatusCode, Json<PaginationListResult<Topic>>) {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let topic_list_result = app_state
        .topic_manager
        .read()
        .await
        .get_topic_list_with_pagination(tenant_id, offset_param, limit_param);

    if let Err(err) = topic_list_result {
        error!("get topic list error: {}", err);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PaginationListResult {
                meta: PaginationMeta {
                    offset: offset_param,
                    limit: limit_param,
                    total: 0,
                },
                data: Vec::new(),
            }),
        );
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

        (StatusCode::OK, Json(result))
    }
}