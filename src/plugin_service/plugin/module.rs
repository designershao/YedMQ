use rune::{ContextError, Module};
use anyhow::Result;

use super::plugin_context::{AuthenticateResult, Hooks};

pub fn module() -> Result<Module> {
    Module::with_crate_item(name, iter)
    let mut module = Module::with_crate_item("samoye", ["hook"])?;
    module.ty::<AuthenticateResult>()?;
    module.ty::<Hooks>()?;

    Ok(module)
}