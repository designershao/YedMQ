fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/yedmq.proto", "proto/raft_protocol.proto", "proto/raft_payload.proto"], &["proto/"])?;
    Ok(())
}