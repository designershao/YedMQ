use std::{sync::{RwLock, Arc}, fmt::{self}};
use mlua::{Lua, Function, Result, ExternalError};
use mlua::{chunk, AnyUserData, ExternalResult, UserData, UserDataMethods};


use crate::plugin::{hook_context::HookContext, hook::{on_connect_auth_hook::OnConnectAuthHookFuncWrapper, Hook, on_publish_acl_check_hook::OnPublishAclCheckHookFuncWrapper, on_subscribe_acl_check_hook::{self, OnSubscribeAclCheckHookFuncWrapper}}};

pub struct HookApi<'lua> {
    hook_context: Arc<RwLock<HookContext<'lua>>>
}

#[derive(Debug, PartialEq)]
pub enum Error {
    InvalidHookName(String),
}

impl ExternalError for Error {
    fn to_lua_err(self) -> mlua::Error {
        match self {
            Error::InvalidHookName(msg) => mlua::Error::RuntimeError(msg)
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::InvalidHookName(msg) => write!(f, "{}", msg),
        }
    }
}

impl<'lua> HookApi<'lua> {
    pub fn new(ctx:Arc<RwLock<HookContext<'lua>>>) -> Self {
        Self {
            hook_context: ctx.clone()
        }
    }

    pub fn register(&mut self, lua: &'lua Lua, hook_name: &String, func_name: &String, unique_name: &String) -> Result<()> {
        let global = lua.globals();
        let func:Function = global.get(func_name.clone())?;
        match hook_name.as_str() {
             "OnConnectAuth" => {
                let on_connect_auth_hook_func_wrapper =  OnConnectAuthHookFuncWrapper::new(func, unique_name.to_string());
                let hook_context_cloned = self.hook_context.clone();
                hook_context_cloned.write().unwrap().register_hook_function(hook_name, Hook::OnConnectAuth(on_connect_auth_hook_func_wrapper));
             }
             "OnPublishAclCheck" => {
                let on_publish_acl_check_func_wrapper = OnPublishAclCheckHookFuncWrapper::new(func, unique_name.to_string());
                let hook_context_cloned = self.hook_context.clone();
                hook_context_cloned.write().unwrap().register_hook_function(hook_name, Hook::OnPublishAclCheck(on_publish_acl_check_func_wrapper));
             }
             "OnSubscribeAclCheck" => {
                let on_subscribe_acl_check_func_wrapper = OnSubscribeAclCheckHookFuncWrapper::new(func, unique_name.to_string());
                let hook_context_cloned = self.hook_context.clone();
                hook_context_cloned.write().unwrap().register_hook_function(hook_name, Hook::OnSubscribeAclCheck(on_subscribe_acl_check_func_wrapper));
             }
             _ => {
                let err = Error::InvalidHookName(String::from("invalid hook name"));
                let lua_err = err.to_lua_err();
                return Err(lua_err);
             }
        }
        Ok(())
    }
}

