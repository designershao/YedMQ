// build.rs
use prost_build::Config;
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    println!("Workspace directory: {}", workspace_dir);
    let protocol_crate_dir = format!("{}/../plugin_protocol", workspace_dir);
    let proto_root = format!("{}/../plugin_protocol/proto", workspace_dir);
    let proto_files = &[format!("{}/proto/yedmq_plugin_protocol.proto", protocol_crate_dir)];
    let out_dir = format!("{}/src/protocol", workspace_dir);
    println!("Output directory: {}", out_dir);
    
    let mut config = Config::new();
    config
        .out_dir(PathBuf::from(&out_dir))
        .compile_protos(proto_files, &[proto_root])?;

    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={}", proto_file);
    }

    Ok(())
}