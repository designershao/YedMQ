use mlua::Function;
use mlua::Result;
use std::hash::Hash;

#[derive(Debug, PartialEq)]
pub struct OnSubscribeAclCheckHookFuncWrapper<'lua> {
    func: Function<'lua>,
    unique_name: String,
}

impl<'lua> Eq for OnSubscribeAclCheckHookFuncWrapper<'lua> { }

impl<'lua> Hash for OnSubscribeAclCheckHookFuncWrapper<'lua> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.unique_name.hash(state)
    }
}

impl<'lua> OnSubscribeAclCheckHookFuncWrapper<'lua> {
    pub fn new(func: Function<'lua>, unique_name: String) -> Self {
        Self {
            func,
            unique_name,
        }
    }

    pub fn call(&self, client_id: &String, username: &String, password: &String) -> Result<bool> {
        self.func.call((client_id.clone(), username.clone(), password.clone()))
    }
}
