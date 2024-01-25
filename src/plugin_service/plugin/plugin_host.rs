use std::{path::PathBuf, sync::Arc};
use anyhow::{anyhow, Error, Ok, Result};
use rune::termcolor::{ColorChoice, StandardStream};
use rune::{Diagnostics, Vm};

use super::plugin_metadata::PluginMetadata;

struct PluginHost {
    metadata: PluginMetadata,
    runtime: Vm
}

impl PluginHost {
    pub fn load(path: String) -> Result<PluginHost> {
        let metadata = PluginMetadata::new(PathBuf::from(path))?;

        let entry_absolute_path = metadata.get_entry_absolute_path();

        let plugin_entry_src = std::fs::read_to_string(entry_absolute_path.clone())?;

        let mut context: rune::Context = rune_modules::default_context()?;
    
        let runtime = Arc::new(context.runtime()?);
        
        let source = rune::Source::with_path(metadata.name.clone(),plugin_entry_src,entry_absolute_path)?;

        let mut sources  = rune::Sources::default();

        sources.insert(source)?;

        let mut diagnostics = Diagnostics::new();

        let result: Result<rune::Unit, rune::BuildError> = rune::prepare(&mut sources)
            .with_context(&context)
            .with_diagnostics(&mut diagnostics)
            .build();
    
        if !diagnostics.is_empty() {
            let mut writer = StandardStream::stderr(ColorChoice::Always);
            diagnostics.emit(&mut writer, &sources.into())?;
        }

        let unit = result?;

        let mut vm = Vm::new(runtime, Arc::new(unit));

        Ok(PluginHost {
            metadata,
            runtime: vm
        })
    }

    pub fn init(&mut self) -> Result<()> {
        self.runtime.call(["on_activate"], ())?;
        Ok(())
    }

}