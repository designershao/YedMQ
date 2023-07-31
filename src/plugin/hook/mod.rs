pub mod on_connect_auth_hook;
pub mod on_publish_acl_check_hook;
pub mod on_subscribe_acl_check_hook;

#[derive(Eq, Hash, PartialEq)]
pub enum Hook<'lua> {
    OnConnectAuth(on_connect_auth_hook::OnConnectAuthHookFuncWrapper<'lua>),
    OnSubscribeAclCheck(on_subscribe_acl_check_hook::OnSubscribeAclCheckHookFuncWrapper<'lua>),
    OnPublishAclCheck(on_publish_acl_check_hook::OnPublishAclCheckHookFuncWrapper<'lua>),
}
