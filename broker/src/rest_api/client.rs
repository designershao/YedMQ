use std::sync::Arc;

use super::{Pagination, PaginationListResult, PaginationMeta};
use crate::{
    app::YedMQApp,
    session::{
        session_actor::ActivityState, session_manager_actor::{self, ForceDisconnect, GetSessionInfoListWithPagination, SessionManagerError}
    },
};
use actix::SystemService;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use log::error;
use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Client {
    tenant_identifier: String,

    client_id: String,

    subscription_topics: Vec<String>,

    session_state: ActivityState,
}

pub async fn kickoff_client(
    State(app_state): State<Arc<YedMQApp>>,
    Path((tenant_id, client_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let session_manager_actor_addr = session_manager_actor::SessionManagerActor::from_registry();
    let kickoff_result = session_manager_actor_addr
        .send(ForceDisconnect {
            tenant_id: tenant_id.clone(),
            client_id: client_id.clone(),
        })
        .await
        .unwrap();
    if let Err(session_err) = kickoff_result {
        let response = match session_err {
            session_manager_actor::SessionManagerError::TenantNotExisted(_) => {
                let error_response = super::ErrorResponse {
                    code: 3,
                    message: format!("tenant {} not existed", tenant_id),
                };
                (StatusCode::NOT_FOUND, Json(error_response)).into_response()
            }
            session_manager_actor::SessionManagerError::SessionNotExisted(_) => {
                let error_response = super::ErrorResponse {
                    code: 4,
                    message: format!("session {} not existed", client_id),
                };
                (StatusCode::NOT_FOUND, Json(error_response)).into_response()
            }
            _ => {
                let error_response = super::ErrorResponse {
                    code: 101,
                    message: "Internal server error".to_string(),
                };
                error!("kickoff client error: {}", session_err);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
            }
        };
        return response;
    } else {
        axum::http::StatusCode::OK.into_response()
    }
}

pub async fn client_list(
    State(app_state): State<Arc<YedMQApp>>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> impl IntoResponse {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let session_manager_actor_addr = session_manager_actor::SessionManagerActor::from_registry();
    let session_list_result = session_manager_actor_addr
    .send(GetSessionInfoListWithPagination {
        tenant_id: tenant_id.clone(),
        offset_param,
        limit_param,
    })
        .await.unwrap();
    if let Ok(session_list) = session_list_result {
        let mut result = Vec::<Client>::new();

        for session in session_list.1 {
            let client = Client {
                tenant_identifier: session.tenant_identifier,
                client_id: session.client_identifier,
                subscription_topics: session.subscription_topics,
                session_state: session.session_state,
            };
            result.push(client);
        }

        let meta = PaginationMeta {
            offset: offset_param,
            limit: limit_param,
            total: session_list.0,
        };

        let result = PaginationListResult { meta, data: result };
        (StatusCode::OK, Json(result)).into_response()
    } else {
        let error = session_list_result.err();
        if let Some(session_error) = error {
            let response = match session_error {
                SessionManagerError::TenantNotExisted(_) => {
                    let error_response = super::ErrorResponse {
                        code: 3,
                        message: format!("tenant {} not existed", tenant_id),
                    };
                    (StatusCode::NOT_FOUND, Json(error_response)).into_response()
                }
                _ => {
                    let error_response = super::ErrorResponse {
                        code: 101,
                        message: "Internal server error".to_string(),
                    };
                    error!("get session list error: {}", session_error);
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
                }
            };
            return response;
        } else {
            let error_response = super::ErrorResponse {
                code: 101,
                message: "Internal server error".to_string(),
            };
            error!("get session list error: {}", error.unwrap());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}
