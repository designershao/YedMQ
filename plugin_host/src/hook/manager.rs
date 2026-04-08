use crate::hook::Hook;
use std::collections::HashMap;

pub struct HookManager {
    hooks: HashMap<Hook, Vec<RegisteredHook>>,
}

pub struct RegisteredHook {
    pub plugin_name: String,
    pub instance_id: String,
    pub priority: u32,
}

impl Default for HookManager {
    fn default() -> Self {
        Self::new()
    }
}

impl HookManager {
    pub fn new() -> Self {
        HookManager {
            hooks: HashMap::new(),
        }
    }

    pub fn register_hook(
        &mut self,
        hook: Hook,
        plugin_name: String,
        instance_id: String,
        priority: u32,
    ) {
        let registered_hook = RegisteredHook {
            plugin_name,
            instance_id,
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

    pub fn replace_plugin_hooks(
        &mut self,
        plugin_name: &str,
        instance_id: &str,
        hooks: &[(Hook, u32)],
    ) {
        self.remove_plugin_name(plugin_name);
        for (hook, priority) in hooks {
            self.register_hook(
                hook.clone(),
                plugin_name.to_string(),
                instance_id.to_string(),
                *priority,
            );
        }
    }

    pub fn remove_plugin_instance(&mut self, instance_id: &str) {
        for hooks in self.hooks.values_mut() {
            hooks.retain(|hook| hook.instance_id != instance_id);
        }
        self.hooks.retain(|_, hooks| !hooks.is_empty());
    }

    fn remove_plugin_name(&mut self, plugin_name: &str) {
        for hooks in self.hooks.values_mut() {
            hooks.retain(|hook| hook.plugin_name != plugin_name);
        }
        self.hooks.retain(|_, hooks| !hooks.is_empty());
    }
}
