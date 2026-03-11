// build.rs
use prost_build::Config;
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    let protoc_include_path = protoc_bin_vendored::include_path()?;
    env::set_var("PROTOC", protoc_path);

    let workspace_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is not set");
    println!("Workspace directory: {}", workspace_dir);
    let protocol_crate_dir = format!("{}/../plugin_protocol", workspace_dir);
    let proto_root = format!("{}/../plugin_protocol/proto", workspace_dir);
    let proto_files = &[format!(
        "{}/proto/yedmq_plugin_protocol.proto",
        protocol_crate_dir
    )];
    let out_dir = format!("{}/src/protocol", workspace_dir);
    println!("Output directory: {}", out_dir);

    let mut config = Config::new();
    let include_paths = vec![PathBuf::from(&proto_root), protoc_include_path];
    config
        .out_dir(PathBuf::from(&out_dir))
        .compile_protos(proto_files, &include_paths)?;

    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={}", proto_file);
    }

    Ok(())
}
