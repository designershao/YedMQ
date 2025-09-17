use std::collections::HashMap;
use crate::hook::Hook;

pub struct HookManager {
    hooks: HashMap<Hook, Vec<RegisteredHook>>,
}

pub struct RegisteredHook {
    pub plugin_name: String,
    pub priority: u32,
}

impl HookManager {

    pub fn new() -> Self {
        HookManager {
            hooks: HashMap::new(),
        }
    }

    pub fn register_hook(&mut self, hook: Hook, plugin_name: String, priority: u32) {
        let registered_hook = RegisteredHook {
            plugin_name,
            priority,
        };
        match self.hooks.entry(hook) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let hooks = entry.get_mut();
                hooks.push(registered_hook);
                hooks.sort_by_key(|h| h.priority);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(vec![registered_hook]);
            }
        }
    }

    pub fn get_hooks(&self, hook: &Hook) -> Option<&Vec<RegisteredHook>> {
        self.hooks.get(hook)
    }

}