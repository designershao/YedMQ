use std::{path::PathBuf, sync::Arc};
use anyhow::{anyhow, Error, Ok, Result};
use rune::termcolor::{ColorChoice, StandardStream};
use rune::{ContextError, Diagnostics, Module, Vm};

use crate::protocol::v3::publish::PublishPacket;

use super::plugin_context::{self, AuthenticateResult, Hooks, TopicInfo, TopicPermission};
use super::plugin_metadata::PluginMetadata;

pub struct PluginHost {
    metadata: PluginMetadata,
    runtime: Vm,
    context: plugin_context::Context,
}

impl PluginHost {
    pub fn get_plugin_name(&self) -> String {
        self.metadata.name.clone()
    }

    pub fn get_plugin_priority(&self) -> i64 {
        self.metadata.priority
    }

    pub fn load(path: String) -> Result<PluginHost> {
        let metadata = PluginMetadata::new(PathBuf::from(path))?;

        let entry_absolute_path = metadata.get_entry_absolute_path();

        let plugin_entry_src = std::fs::read_to_string(entry_absolute_path.clone())?;

        let module = Self::module()?;

        let mut context: rune::Context = rune::Context::with_default_modules()?;
        context.install(module)?;
    
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

        let plugin_context = plugin_context::Context::new();

        Ok(PluginHost {
            metadata,
            runtime: vm,
            context: plugin_context
        })
    }

    pub fn init(&mut self) -> Result<()> {
        self.runtime.call(["on_activate"], (&mut self.context,))?;
        Ok(())
    }

    fn module() -> Result<Module> {
        let mut module = Module::new();
        module.ty::<plugin_context::Context>()?;
        Ok(module)
    }

    pub fn on_connect_auth(&mut self, connect_info: plugin_context::ConnectInfo) -> Result<AuthenticateResult> {
        self.context.on_connect_auth(connect_info)
    }

    pub async fn on_publish(&mut self, client_info: plugin_context::ClientInfo, publish_packet: PublishPacket) -> Result<()> {
        self.context.on_publish(client_info, publish_packet).await
    }

    pub async fn on_topic_permission_check(&mut self,topic_info:TopicInfo) -> anyhow::Result<TopicPermission> {
        self.context.on_topic_permission_check(topic_info).await
    }

}