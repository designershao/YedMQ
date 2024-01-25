use std::collections::HashMap;

use rune::runtime::Function;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Hooks {
    OnConnect,
    OnConnectAuth,
    OnTopicPermissionCheck,
    OnPublish,
    OnDisconnect,
}

pub struct Context {
}

pub struct Hook {
    hook_table: HashMap<Hooks, Function>,
}

impl Hook {
    pub fn subscribe(&mut self, hook_type: Hooks, f: Function) {
        self.hook_table.insert(hook_type, f);
    }

}