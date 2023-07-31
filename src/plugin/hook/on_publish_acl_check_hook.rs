use std::hash::Hash;

use mlua::Function;
use mlua::Result;


#[derive(Debug, PartialEq)]
pub struct OnPublishAclCheckHookFuncWrapper<'lua> {
    func: Function<'lua>,
    belongs_to_plugin: String,
}

impl<'lua> Eq for OnPublishAclCheckHookFuncWrapper<'lua> {}

impl<'lua> Hash for OnPublishAclCheckHookFuncWrapper<'lua> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.belongs_to_plugin.hash(state)
    }
}

impl<'lua> OnPublishAclCheckHookFuncWrapper<'lua> {
    pub fn new(func: Function<'lua>, belongs_to_plugin: String) -> Self {
        Self {
            func,
            belongs_to_plugin,
        }
    }

    pub fn call(&self, client_id: &String, username: &String, topic: &String) -> Result<bool> {
        self.func.call((client_id.clone(), username.clone(), topic.clone()))
    }
}
