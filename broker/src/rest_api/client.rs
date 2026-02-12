use std::sync::Arc;

use super::{Pagination, PaginationListResult, PaginationMeta};
use crate::{
    app::YedMQApp,
    protobuf::GetSessionInfoRequest,
    raft::session_actor_map::session_actor_map_raft_actor::{
        GetClientListWithPagination, SessionActorMapRaftError,
    },
    session::{
        session_actor::ActivityState,
        session_manager_actor::{self, ForceDisconnect},
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
    pub tenant_identifier: String,

    pub client_identifier: String,

    pub subscription_topics: Vec<String>,

    pub session_state: ActivityState,

    pub created_at: u64,

    pub connected_at: Option<u64>,

    pub disconnected_at: Option<u64>,

    pub messages_received: u64,

    pub messages_sent: u64,

    pub connected: bool,

    pub ip_address: Option<String>,
}

pub async fn kickoff_client(
    State(_): State<Arc<YedMQApp>>,
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
        response
    } else {
        axum::http::StatusCode::OK.into_response()
    }
}

pub async fn client_list(
    State(app): State<Arc<YedMQApp>>,
    Path(tenant_id): Path<String>,
    pagination: Query<Pagination>,
) -> impl IntoResponse {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);

    match app
        .service_registry
        .session_map_raft
        .send(GetClientListWithPagination {
            tenant_id: tenant_id.clone(),
            offset: offset_param as usize,
            limit: limit_param as usize,
        })
        .await
        .expect("Session actor map actor not ready")
    {
        Ok(response) => {
            let mut clients: Vec<Client> = Vec::new();
            for (client_id, node_id) in response.client_list {
                let cluster_rpc_client_option = app.get_cluster_service_rpc_client(&node_id).await;
                if let Some(mut cluster_rpc_client) = cluster_rpc_client_option {
                    let get_session_info_response = cluster_rpc_client
                        .get_session_info(GetSessionInfoRequest {
                            tenant_id: tenant_id.clone(),
                            client_id,
                        })
                        .await;
                    if let Ok(response) = get_session_info_response {
                        let session_info = response.into_inner().payload.unwrap();
                        let activaty_state = match crate::protobuf::ActivityState::try_from(
                            session_info.session_state,
                        )
                        .unwrap_or(crate::protobuf::ActivityState::Inactive)
                        {
                            crate::protobuf::ActivityState::Active => ActivityState::Active,
                            crate::protobuf::ActivityState::Inactive => ActivityState::Inactive,
                            crate::protobuf::ActivityState::Unspecified => ActivityState::Inactive,
                            crate::protobuf::ActivityState::Disconnected => ActivityState::Inactive,
                        };
                        let client = Client {
                            tenant_identifier: session_info.tenant_identifier,
                            client_identifier: session_info.client_identifier,
                            subscription_topics: session_info.subscription_topics,
                            session_state: activaty_state,
                            created_at: session_info.created_at,
                            connected_at: session_info.connected_at,
                            disconnected_at: session_info.disconnected_at,
                            messages_received: session_info.messages_received,
                            messages_sent: session_info.messages_sent,
                            connected: session_info.connected,
                            ip_address: session_info.ip_address,
                        };
                        clients.push(client);
                    } else {
                        error!(
                            "get session info rpc error: {:?}",
                            get_session_info_response.err()
                        );
                    }
                }
            }

            let meta = PaginationMeta {
                offset: offset_param,
                limit: limit_param,
                total: response.total as u64,
            };

            let result = PaginationListResult {
                meta,
                data: clients,
            };

            (StatusCode::OK, Json(result)).into_response()
        }
        Err(e) => match e {
            SessionActorMapRaftError::TenantNotFound { tenant_id } => {
                let error_response = super::ErrorResponse {
                    code: 3,
                    message: format!("tenant {} not existed", tenant_id),
                };
                (StatusCode::NOT_FOUND, Json(error_response)).into_response()
            }
            _ => {
                let error_response = super::ErrorResponse {
                    code: 101,
                    message: format!("Internal server error: {}", e),
                };
                (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
            }
        },
    }
}
