use axum::{
    extract::{Path, Query, State}, http::StatusCode, response::IntoResponse, Json
};
use log::error;
use serde::Serialize;
use crate::session::session_manager::{self, SessionManagerError, SessionState};
use super::{AppState, Pagination, PaginationListResult, PaginationMeta};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Client {
    tenant_identifier: String,

    client_id: String,

    subscription_topics: Vec<String>,

    session_state: SessionState,
}


pub async fn kickoff_client(
    State(app_state): State<AppState>,
    Path((tenant_id, client_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let session_manager = app_state.session_manager.clone();
    let (session_quit_sender, mut session_quit_receiver) = tokio::sync::mpsc::channel(1);
    let kickoff_result = session_manager
        .read()
        .await
        .kickoff(&tenant_id, &client_id, &session_quit_sender.clone())
        .await;
    if let Err(e) = kickoff_result {
        if let Some(session_err) = e.downcast_ref::<SessionManagerError>() {
            let response = match session_err {
                session_manager::SessionManagerError::TenantNotExisted(_) => {
                    let error_response = super::ErrorResponse {
                        code: 3,
                        message: format!("tenant {} not existed", tenant_id),
                    };
                    (StatusCode::NOT_FOUND, Json(error_response)).into_response()
                }
                session_manager::SessionManagerError::SessionNotExisted(_) => {
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
                    error!("kickoff client error: {}", e);
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
                }
            };
            return response
        } else {
            let error_response = super::ErrorResponse {
                code: 101,
                message: "Internal server error".to_string(),
            };
            error!("kickoff client error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    } else {
        session_quit_receiver.recv().await;
        axum::http::StatusCode::OK.into_response()
    }
}

pub async fn client_list(
    State(app_state): State<AppState>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> impl IntoResponse {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    let session_manager = app_state.session_manager.clone();
    let session_list_result = session_manager
        .read()
        .await
        .get_session_info_list_with_pagination(&tenant_id, offset_param, limit_param)
        .await;
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
        let error = session_list_result.err().unwrap();
        if let Some(session_error) = error.downcast_ref::<SessionManagerError>() {
            let response = match session_error {
                SessionManagerError::TenantNotExisted(_) => {
                    let error_response = super::ErrorResponse {
                        code: 3,
                        message: format!("tenant {} not existed", tenant_id),
                    };
                    (StatusCode::NOT_FOUND, Json(error_response)).into_response()
                }
                _ =>{
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
            error!("get session list error: {}", error);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}
