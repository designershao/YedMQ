use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Code, Status};

pub const ERROR_KIND_KEY: &str = "x-yedmq-error-kind";
pub const LEADER_NODE_ID_KEY: &str = "x-yedmq-leader-node-id";
pub const LEADER_ADDR_KEY: &str = "x-yedmq-leader-addr";

pub const ERROR_KIND_LEADER_REDIRECT: &str = "leader_redirect";
pub const ERROR_KIND_BUSINESS: &str = "business";
pub const ERROR_KIND_NO_LEADER: &str = "no_leader";
pub const ERROR_KIND_NOT_READY: &str = "not_ready";
pub const ERROR_KIND_FATAL: &str = "fatal";

#[derive(Debug, Clone, Default)]
pub struct ParsedStatus {
    pub error_kind: Option<String>,
    pub leader_node_id: Option<u64>,
    pub leader_addr: Option<String>,
    pub detail: Option<crate::protobuf::ErrorDetail>,
}

fn metadata_value(value: &str) -> Option<MetadataValue<tonic::metadata::Ascii>> {
    value.parse().ok()
}

fn encode_detail(detail: &crate::protobuf::ErrorDetail) -> prost::bytes::Bytes {
    prost::bytes::Bytes::from(detail.encode_to_vec())
}

fn status_with_metadata(
    code: Code,
    message: impl Into<String>,
    error_kind: Option<&str>,
    detail: Option<&crate::protobuf::ErrorDetail>,
    leader_addr: Option<&str>,
    leader_node_id: Option<u64>,
) -> Status {
    let mut metadata = tonic::metadata::MetadataMap::new();
    if let Some(error_kind) = error_kind.and_then(metadata_value) {
        metadata.insert(ERROR_KIND_KEY, error_kind);
    }

    if let Some(leader_addr) = leader_addr.and_then(metadata_value) {
        metadata.insert(LEADER_ADDR_KEY, leader_addr);
    }

    if let Some(node_id) = leader_node_id.and_then(|value| metadata_value(&value.to_string())) {
        metadata.insert(LEADER_NODE_ID_KEY, node_id);
    }

    Status::with_details_and_metadata(
        code,
        message.into(),
        detail.map(encode_detail).unwrap_or_default(),
        metadata,
    )
}

pub fn business_detail(
    code: crate::protobuf::ErrorCode,
    message: impl Into<String>,
    node: impl Into<String>,
) -> crate::protobuf::ErrorDetail {
    crate::protobuf::ErrorDetail {
        code: code as i32,
        message: message.into(),
        node: node.into(),
    }
}

pub fn business_status(status_code: Code, detail: crate::protobuf::ErrorDetail) -> Status {
    status_with_metadata(
        status_code,
        detail.message.clone(),
        Some(ERROR_KIND_BUSINESS),
        Some(&detail),
        None,
        None,
    )
}

pub fn leader_redirect_status(
    message: impl Into<String>,
    leader_addr: impl Into<String>,
    leader_node_id: Option<u64>,
) -> Status {
    let leader_addr = leader_addr.into();
    status_with_metadata(
        Code::FailedPrecondition,
        message.into(),
        Some(ERROR_KIND_LEADER_REDIRECT),
        None,
        Some(&leader_addr),
        leader_node_id,
    )
}

pub fn no_leader_status(message: impl Into<String>) -> Status {
    status_with_metadata(
        Code::Unavailable,
        message.into(),
        Some(ERROR_KIND_NO_LEADER),
        None,
        None,
        None,
    )
}

pub fn not_ready_status(message: impl Into<String>) -> Status {
    status_with_metadata(
        Code::Unavailable,
        message.into(),
        Some(ERROR_KIND_NOT_READY),
        None,
        None,
        None,
    )
}

pub fn fatal_status(code: Code, message: impl Into<String>) -> Status {
    status_with_metadata(
        code,
        message.into(),
        Some(ERROR_KIND_FATAL),
        None,
        None,
        None,
    )
}

pub fn invalid_argument_status(message: impl Into<String>, node: impl Into<String>) -> Status {
    business_status(
        Code::InvalidArgument,
        business_detail(
            crate::protobuf::ErrorCode::InvalidArgument,
            message,
            node,
        ),
    )
}

pub fn not_found_status(message: impl Into<String>, node: impl Into<String>) -> Status {
    business_status(
        Code::NotFound,
        business_detail(crate::protobuf::ErrorCode::NotFound, message, node),
    )
}

pub fn decode_status(status: &Status) -> ParsedStatus {
    let error_kind = status
        .metadata()
        .get(ERROR_KIND_KEY)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let leader_node_id = status
        .metadata()
        .get(LEADER_NODE_ID_KEY)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let leader_addr = status
        .metadata()
        .get(LEADER_ADDR_KEY)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let detail = if status.details().is_empty() {
        None
    } else {
        crate::protobuf::ErrorDetail::decode(status.details()).ok()
    };

    ParsedStatus {
        error_kind,
        leader_node_id,
        leader_addr,
        detail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_redirect_status_round_trips_metadata() {
        let status = leader_redirect_status("redirect", "10.0.0.3:9080", Some(3));
        let parsed = decode_status(&status);

        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(parsed.error_kind.as_deref(), Some(ERROR_KIND_LEADER_REDIRECT));
        assert_eq!(parsed.leader_node_id, Some(3));
        assert_eq!(parsed.leader_addr.as_deref(), Some("10.0.0.3:9080"));
        assert!(parsed.detail.is_none());
    }

    #[test]
    fn business_status_round_trips_details() {
        let status = business_status(
            Code::InvalidArgument,
            business_detail(
                crate::protobuf::ErrorCode::InvalidArgument,
                "bad request",
                "cluster_service",
            ),
        );
        let parsed = decode_status(&status);

        assert_eq!(parsed.error_kind.as_deref(), Some(ERROR_KIND_BUSINESS));
        assert_eq!(parsed.detail.as_ref().map(|detail| detail.code()), Some(crate::protobuf::ErrorCode::InvalidArgument));
        assert_eq!(parsed.detail.as_ref().map(|detail| detail.message.as_str()), Some("bad request"));
        assert_eq!(parsed.detail.as_ref().map(|detail| detail.node.as_str()), Some("cluster_service"));
    }

    #[test]
    fn unavailable_statuses_publish_expected_error_kind() {
        let no_leader = decode_status(&no_leader_status("no leader"));
        let not_ready = decode_status(&not_ready_status("not ready"));

        assert_eq!(no_leader.error_kind.as_deref(), Some(ERROR_KIND_NO_LEADER));
        assert_eq!(not_ready.error_kind.as_deref(), Some(ERROR_KIND_NOT_READY));
    }
}
