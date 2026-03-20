use std::time::Duration;

use acl_file::{AclConfig, AclPlugin};
use anyhow::{Context, Result};
use clap::Parser;
use futures::{SinkExt, StreamExt};
use interprocess::local_socket::tokio::{prelude::*, Stream};
use log::{error, info};
use prost::Message as _;
use tokio::time::timeout;
use tokio_util::codec::Framed;
use yedmq_plugin_host::local_socket_name::resolve_local_socket_name;
use yedmq_plugin_host::protocol::{
    plugin_protocol::ProtocolMessage, protocol_frame::ProtocolFrameCodec,
};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(long)]
    auth_code: String,

    #[arg(long)]
    socket_path: String,

    #[arg(long)]
    acl_file: String,

    #[arg(long, default_value_t = 100)]
    authenticate_priority: u32,

    #[arg(long, default_value_t = 100)]
    authorize_priority: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let config = AclConfig::load_from_file(&args.acl_file)?;
    let plugin = AclPlugin::new(
        config,
        args.auth_code.clone(),
        args.authenticate_priority,
        args.authorize_priority,
    );

    info!("ACL file plugin starting");
    info!("Socket path: {}", args.socket_path);
    info!("ACL file: {}", args.acl_file);

    let socket_name = resolve_local_socket_name(&args.socket_path)?;

    let stream = match timeout(Duration::from_secs(5), Stream::connect(socket_name)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => return Err(error).context("failed to connect to plugin host socket"),
        Err(_) => anyhow::bail!("timed out while connecting to plugin host socket"),
    };

    let mut framed = Framed::new(stream, ProtocolFrameCodec::new());
    info!("Connected to plugin host");

    while let Some(frame) = framed.next().await {
        match frame {
            Ok(frame) => {
                let request = match ProtocolMessage::decode(frame.payload.as_ref()) {
                    Ok(request) => request,
                    Err(error) => {
                        error!("Failed to decode protocol message: {}", error);
                        continue;
                    }
                };

                match plugin.handle_request(&request) {
                    Ok(response) => {
                        if let Err(error) = framed.send(response).await {
                            return Err(error).context("failed to send plugin response");
                        }
                    }
                    Err(error) => {
                        error!("Failed to handle request: {}", error);
                    }
                }
            }
            Err(error) => {
                return Err(error).context("failed to read frame from plugin host");
            }
        }
    }

    info!("Connection closed by plugin host");
    Ok(())
}
