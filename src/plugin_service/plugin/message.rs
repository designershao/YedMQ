use crate::protocol::v3::{connect::ConnectPacket, publish::PublishPacket};

// Connect the client info
pub struct ConnectInfo {
    pub connect_packet: ConnectPacket,
    pub remote_addr: String,
}

// Connected the client info
pub struct ClientInfo {
    pub tenant_id: String,
    pub client_identifier: String,
    pub username: String,
    pub remote_addr: String,
}

// Plugin host request message
pub enum Request {
    OnConnectAuth(ConnectInfo, tokio::sync::oneshot::Sender<AuthenticateResponse>),
    OnPublish(ClientInfo, PublishPacket),
    OnSubscribeACLCheck(ClientInfo, String, i32, tokio::sync::oneshot::Sender<TopicPermission>),
    OnPublishACLCheck(ClientInfo, String, i32, tokio::sync::oneshot::Sender<TopicPermission>),
    Quit
}

pub struct TopicPermission {
    topic_filter: String,
    qos: i64,
    permission: PermissionType,
    operation: TopicOperation
}

pub enum TopicOperation {
    Publish,
    Subscribe,
}

pub enum PermissionType {
    Allow,
    Deny
}

pub enum AuthenticateResponse {
    Success(AuthenticateSucceed),
    Fail(AuthenticateFail)
}

pub struct AuthenticateSucceed {
    tenant_id: String,
    user_id: String,
}

pub struct AuthenticateFail {
    fail_reason: FailReason,
    details: String,
}

pub enum FailReason {
    BadUserOrPassword,
    InternalError,
}