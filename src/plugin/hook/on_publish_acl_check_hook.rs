use std::hash::Hash;

use mlua::Function;
use mlua::Result;


#[derive(Debug, PartialEq)]
pub struct OnPublishAclCheckHookFuncWrapper<'lua> {
    func: Function<'lua>,
    unique_name: String,
}

impl<'lua> Eq for OnPublishAclCheckHookFuncWrapper<'lua> {}

impl<'lua> Hash for OnPublishAclCheckHookFuncWrapper<'lua> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.unique_name.hash(state)
    }
}

impl<'lua> OnPublishAclCheckHookFuncWrapper<'lua> {
    pub fn new(func: Function<'lua>, unique_name: String) -> Self {
        Self {
            func,
            unique_name,
        }
    }

    pub fn call(&self, client_id: &String, username: &String, topic: &String) -> Result<bool> {
        self.func.call((client_id.clone(), username.clone(), topic.clone()))
    }
}
