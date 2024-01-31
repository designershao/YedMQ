use rune::{ContextError, Module};
use anyhow::Result;

use crate::plugin_service::plugin::plugin_context::{Authentication, Context, Hooks};


pub fn module() -> Result<Module> {
    let mut module = Module::with_crate_item("samoye", ["hook"])?;
    module.ty::<Authentication>()?;
    module.ty::<Hooks>()?;
    module.ty::<Context>()?;

    module.function_meta(Context::hook_subscribe)?;

    Ok(module)
}