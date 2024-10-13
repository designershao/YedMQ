use std::path::PathBuf;

use samoye_plugin::plugin::AuthenticationResult;

fn get_demo_plugin_path() -> PathBuf {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(crate_root_path)
        .join("tests")
        .join("libexample_plugin.so")
}

#[test]
pub fn test_plugin_lib_load() {
}