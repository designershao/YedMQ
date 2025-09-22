pub mod manager;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Hook {
    // Event notification
    ClientConnected,
    ClientDisconnected,
    MessagePublished,
    SubscribeAdded,
    SubscribeRemoved,
    //

    // Authentication and Authorization
    Authenticate,
    Authorize,
    //

    // Message handling
    OnMessagePublish,
    OnMessageSubscribe,
    OnMessageUnsubscribe,
    //

    OnStatsRequest,
}

pub fn get_hook_from_name(name: &str) -> Option<Hook> {
    match name {
        "ClientConnected" => Some(Hook::ClientConnected),
        "ClientDisconnected" => Some(Hook::ClientDisconnected),
        "MessagePublished" => Some(Hook::MessagePublished),
        "SubscribeAdded" => Some(Hook::SubscribeAdded),
        "SubscribeRemoved" => Some(Hook::SubscribeRemoved),

        "Authenticate" => Some(Hook::Authenticate),
        "Authorize" => Some(Hook::Authorize),

        "OnMessagePublish" => Some(Hook::OnMessagePublish),
        "OnMessageSubscribe" => Some(Hook::OnMessageSubscribe),
        "OnMessageUnsubscribe" => Some(Hook::OnMessageUnsubscribe),

        "OnStatsRequest" => Some(Hook::OnStatsRequest),
        _ => None,
    }
}