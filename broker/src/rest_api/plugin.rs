use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;
use crate::app::YedMQApp;

use super::{Pagination, PaginationListResult, PaginationMeta};

#[derive(Serialize)]
pub struct Plugin {
    name: String,

    version: String,

    description: String,

    author: String,
}

pub async fn plugin_list(
    State(app_state): State<Arc<YedMQApp>>,
    pagination: Query<Pagination>,
) -> (StatusCode, Json<PaginationListResult<Plugin>>) {
    let offset_param = pagination.offset.unwrap_or(0);
    let limit_param = pagination.limit.unwrap_or(10);
    let plugin_metadata_list = app_state
        .plugin_manager
        .get_plugin_metadata_list_with_pagination(offset_param, limit_param).await;
    let mut result = Vec::<Plugin>::new();

    for plugin_metadata in plugin_metadata_list.1 {
        let plugin = Plugin {
            name: plugin_metadata.plugin.name.clone(),
            version: plugin_metadata.plugin.version.clone(),
            description: plugin_metadata.plugin.description.clone(),
            author: plugin_metadata.plugin.author.clone(),
        };
        result.push(plugin);
    }

    let meta = PaginationMeta {
        offset: offset_param,
        limit: limit_param,
        total: plugin_metadata_list.0,
    };

    let result = PaginationListResult { meta, data: result };

    (StatusCode::OK, Json(result))
}
