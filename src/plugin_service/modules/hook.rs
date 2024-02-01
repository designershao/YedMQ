use rune::{ContextError, Module};
use anyhow::Result;

use crate::plugin_service::plugin::plugin_context::{Authentication, Authorization, ClientInfo, Context, Hooks, TopicInfo, TopicOperation};


pub fn module() -> Result<Module> {
    let mut module = Module::with_crate_item("samoye", ["hook"])?;
    module.ty::<Authentication>()?;
    module.ty::<Authorization>()?;

    module.ty::<Hooks>()?;
    module.ty::<Context>()?;
    module.ty::<ClientInfo>()?;
    module.ty::<TopicInfo>()?;
    module.ty::<TopicOperation>()?;

    module.function_meta(Context::hook_subscribe)?;

    Ok(module)
}