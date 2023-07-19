#[derive(Eq, Hash, PartialEq)]
pub enum Hook {
    OnConnectAuth,
    OnSubscribeAclCheck,
    OnPublishAclCheck,
}

/// OnConnectAuth hook function type
/// ```
/// fn check_connect(clientId:&String, username:&String, password:&String) -> bool {
///    if clientId == "test" && username == "test" && password == "test" {
///        return true
///    }else{
///        return false
///    }
/// }
/// 
/// fn on_connect_auth(clientId:&String, username:&String, password:&String, hook_func:OnConnectAuthFunc) -> bool {
///    hook_func(clientId, username, password)
/// }
/// 
/// on_connect_auth("test", "test", "test", check_connect);
/// ```
type OnConnectAuthFunc = dyn Fn(&String, &String, &String) -> bool;


/// OnSubscribeAclCheck hook function type
/// ```
/// fn check_subscribe(clientId:&String, username:&String, topic: &String) -> bool {
///    if clientId == "test" && username == "test" && topic == "test" {
///        return true
///    }else{
///        return false
///    }
/// }
/// 
/// fn on_subscribe_acl_check(clientId:&String, username:&String, topic: &String, hook_func:OnSubscribeAclCheckFunc) -> bool {
///    hook_func(clientId, username, topic)
/// }
/// 
/// on_subscribe_acl_check("test", "test", "test", check_subscribe);
/// ```
type OnSubscribeAclCheckFunc = dyn Fn(&String, &String, &String) -> bool;


/// OnSubscribeAclCheck hook function type
/// ```
/// fn check_publish(clientId:&String, username:&String, topic: &String) -> bool {
///    if clientId == "test" && username == "test" && topic == "test" {
///        return true
///    }else{
///        return false
///    }
/// }
/// 
/// fn on_publish_acl_check(clientId:&String, username:&String, topic: &String, hook_func:OnSubscribeAclCheckFunc) -> bool {
///    hook_func(clientId, username, topic)
/// }
/// 
/// on_publish_acl_check("test", "test", "test", check_subscribe);
/// ```
type OnPublishAclCheckFunc = dyn Fn(&String, &String, &String) -> bool;