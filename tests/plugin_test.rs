use std::path::PathBuf;
use samoye::plugin_service::plugin::{plugin_host::PluginHost, plugin_metadata::PluginMetadata};

fn get_demo_plugin_path() -> PathBuf {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(crate_root_path)
        .join("tests")
        .join("plugins")
        .join("demo_plugin")
}

#[test]
pub fn test_plugin_metadata_load() {

    let plugin_path = get_demo_plugin_path();

    let plugin_metadata = PluginMetadata::new(plugin_path).unwrap();

    assert!(plugin_metadata.name == "demo_plugin");
    assert!(plugin_metadata.author == "Samoye");
    assert!(plugin_metadata.description == "Just a demo plugin");
    assert!(plugin_metadata.version == "1.0.0");
    assert!(plugin_metadata.entry == "./src/plugin.rn");
    assert!(plugin_metadata.priority == 1000);

}

#[test]
pub fn test_plugin_host_load() {
    let plugin_path = get_demo_plugin_path();
    let plugin_host = PluginHost::load(plugin_path.to_str().unwrap().to_string()).unwrap();
}