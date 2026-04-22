use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Code, Status};

pub const ERROR_KIND_KEY: &str = "x-yedmq-error-kind";
pub const LEADER_NODE_ID_KEY: &str = "x-yedmq-leader-node-id";
pub const LEADER_ADDR_KEY: &str = "x-yedmq-leader-addr";

pub const ERROR_KIND_LEADER_REDIRECT: &str = "leader_redirect";
pub const ERROR_KIND_BUSINESS: &str = "business";

#[derive(Debug, Clone, Default)]
pub struct ParsedStatus {
    pub error_kind: Option<String>,
    pub leader_addr: Option<String>,
    pub detail: Option<crate::protobuf::ErrorDetail>,
}

fn metadata_value(value: &str) -> Option<MetadataValue<tonic::metadata::Ascii>> {
    value.parse().ok()
}

fn encode_detail(detail: &crate::protobuf::ErrorDetail) -> prost::bytes::Bytes {
    prost::bytes::Bytes::from(detail.encode_to_vec())
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
    let mut metadata = tonic::metadata::MetadataMap::new();
    if let Some(value) = metadata_value(ERROR_KIND_BUSINESS) {
        metadata.insert(ERROR_KIND_KEY, value);
    }

    Status::with_details_and_metadata(
        status_code,
        detail.message.clone(),
        encode_detail(&detail),
        metadata,
    )
}

pub fn leader_redirect_status(
    message: impl Into<String>,
    leader_addr: impl Into<String>,
    leader_node_id: Option<u64>,
) -> Status {
    let mut metadata = tonic::metadata::MetadataMap::new();
    if let Some(value) = metadata_value(ERROR_KIND_LEADER_REDIRECT) {
        metadata.insert(ERROR_KIND_KEY, value);
    }

    let leader_addr = leader_addr.into();
    if let Some(value) = metadata_value(&leader_addr) {
        metadata.insert(LEADER_ADDR_KEY, value);
    }
    if let Some(node_id) = leader_node_id.and_then(|value| metadata_value(&value.to_string())) {
        metadata.insert(LEADER_NODE_ID_KEY, node_id);
    }

    Status::with_details_and_metadata(
        Code::FailedPrecondition,
        message.into(),
        prost::bytes::Bytes::new(),
        metadata,
    )
}

pub fn decode_status(status: &Status) -> ParsedStatus {
    let error_kind = status
        .metadata()
        .get(ERROR_KIND_KEY)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
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
        leader_addr,
        detail,
    }
}
