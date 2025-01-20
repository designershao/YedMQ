use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use log::error;
use serde::{Deserialize, Serialize};
use super::{AppState, Pagination, PaginationListResult, PaginationMeta};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetainMessage {
    pub topic: String,
    pub qos: u8,
    pub client_identifier: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanRetainMessage {
    pub topic: String
}

pub async fn clean_retain_message(
    State(app_state): State<AppState>,
    Path((tenant_id,topic_filter)): Path<(String,String)>,
) -> (StatusCode, ()) {
    let r = app_state
        .topic_manager
        .write()
        .await
        .clean_retain_publish_packet(tenant_id, &topic_filter);
    if let Err(err) = r {
        error!("clean retain message error: {}", err);
        return (StatusCode::INTERNAL_SERVER_ERROR, ());
    }
    (StatusCode::OK, ())
}

pub async fn retain_message_list(
    State(app_state): State<AppState>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> (StatusCode, Json<PaginationListResult<RetainMessage>>) {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let r = app_state
        .topic_manager
        .read()
        .await
        .get_retain_message_list_with_pagination(tenant_id.as_str(), offset_param, limit_param);

    if let Err(err) = r {
        error!("get retain message list error: {}", err);
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
        (StatusCode::OK, Json(result))
    }
}