use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "yedmq", version, about = "YedMQ broker and operations CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Start(StartArgs),
    Version,
    Config(ConfigArgs),
    Node(NodeArgs),
    Cluster(ClusterArgs),
    Broker(BrokerArgs),
}

#[derive(Debug, Args, Clone)]
pub struct StartArgs {
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    #[arg(long)]
    pub log_level: Option<String>,
}

#[derive(Debug, Args)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub command: ConfigCommand,
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    Check(ConfigCheckArgs),
}

#[derive(Debug, Args)]
pub struct ConfigCheckArgs {
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    #[arg(long)]
    pub strict: bool,
}

#[derive(Debug, Args)]
pub struct NodeArgs {
    #[command(subcommand)]
    pub command: NodeCommand,
}

#[derive(Debug, Subcommand)]
pub enum NodeCommand {
    Status(ApiArgs),
}

#[derive(Debug, Args)]
pub struct ClusterArgs {
    #[command(subcommand)]
    pub command: ClusterCommand,
}

#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    Status(ApiArgs),
}

#[derive(Debug, Args)]
pub struct BrokerArgs {
    #[command(subcommand)]
    pub command: BrokerCommand,
}

#[derive(Debug, Subcommand)]
pub enum BrokerCommand {
    Stats(ApiArgs),
}

#[derive(Debug, Args, Clone)]
pub struct ApiArgs {
    #[arg(long, default_value = "http://127.0.0.1:3456")]
    pub admin: String,

    #[arg(long)]
    pub user: Option<String>,

    #[arg(long)]
    pub password: Option<String>,

    #[arg(long)]
    pub password_stdin: bool,

    #[arg(long, default_value = "3s")]
    pub timeout: String,

    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub output: OutputFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_no_args_as_default_start() {
        let cli = Cli::try_parse_from(["yedmq"]).unwrap();
        assert!(cli.command.is_none());
    }

    #[test]
    fn parses_start_with_config() {
        let cli = Cli::try_parse_from(["yedmq", "start", "-c", "yedmq.toml"]).unwrap();
        match cli.command {
            Some(Command::Start(args)) => {
                assert_eq!(args.config.unwrap(), PathBuf::from("yedmq.toml"));
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn parses_config_check_strict() {
        let cli = Cli::try_parse_from(["yedmq", "config", "check", "-c", "yedmq.toml", "--strict"])
            .unwrap();
        match cli.command {
            Some(Command::Config(ConfigArgs {
                command: ConfigCommand::Check(args),
            })) => {
                assert_eq!(args.config.unwrap(), PathBuf::from("yedmq.toml"));
                assert!(args.strict);
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn parses_node_status_api_args() {
        let cli = Cli::try_parse_from([
            "yedmq",
            "node",
            "status",
            "--admin",
            "http://127.0.0.1:3456",
            "--user",
            "admin",
            "--password-stdin",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Node(NodeArgs {
                command: NodeCommand::Status(args),
            })) => {
                assert_eq!(args.admin, "http://127.0.0.1:3456");
                assert_eq!(args.user.as_deref(), Some("admin"));
                assert!(args.password_stdin);
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn parses_cluster_status_json() {
        let cli = Cli::try_parse_from(["yedmq", "cluster", "status", "--output", "json"]).unwrap();
        match cli.command {
            Some(Command::Cluster(ClusterArgs {
                command: ClusterCommand::Status(args),
            })) => assert_eq!(args.output, OutputFormat::Json),
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn parses_broker_stats() {
        let cli = Cli::try_parse_from(["yedmq", "broker", "stats"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Broker(BrokerArgs {
                command: BrokerCommand::Stats(_)
            }))
        ));
    }
}
