use std::{collections::HashMap, sync::{Arc, RwLock}};

use mlua::Function;
use super::hook::Hook;

struct HookContext<'lua> {
    hook_map: HashMap<String, Vec<Arc<Hook<'lua>>>>
}

impl<'lua> HookContext<'lua> {

    pub fn register_hook_function(&mut self, hook_name: &String, hook_func_wrapper: Hook<'lua>) {
        self.hook_map.entry(hook_name.clone()).or_default().push(Arc::new(hook_func_wrapper)); 
    }
    

    pub fn unregister_hook_function(&mut self, hook_name: &String, hook_func_wrapper: Hook<'lua>) {
        todo!()
    }

}