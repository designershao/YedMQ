<p align="center">
  <img alt="YedMQ logo" src="https://github.com/designershao/YedMQ/blob/main/assets/logo.png?raw=true" width="300" />
</p>

<p align="center">
  <a href="https://www.yedmq.com"><b>Website 🌐</b></a> |
  <a href="https://www.yedmq.com/docs/overview"><b>Documentation 📚</b></a>
</p>

<p align="center">
  <a href="https://github.com/designershao/YedMQ/actions"><img src="https://github.com/designershao/YedMQ/workflows/CI/badge.svg" alt="CI Status"></a>
  <a href="https://github.com/designershao/YedMQ/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"></a>
  <a href="https://github.com/designershao/YedMQ/releases"><img src="https://img.shields.io/github/v/release/designershao/YedMQ" alt="Release"></a>
</p>

---

# YedMQ

**YedMQ** is a high-performance, distributed MQTT broker written in **Rust**, specifically designed for modern IoT infrastructure. It is built for scalability, security, and extreme efficiency.

**⚠️ This project is still under development and is not yet suitable for production use.**

## ✨ Key Features

- **MQTT v3.1.1 Support**: Fully compliant with the MQTT v3.1.1 protocol, including all QoS levels (0, 1, 2), retained messages, last will, and persistent sessions.
- **High Performance**: Leveraging Rust's memory safety and zero-cost abstractions for low latency and high throughput.
- **Multiple Tenant Support**: Built-in isolation for multiple organizations. Each tenant has its own namespace, sessions, and topics, ensuring data privacy and security.
- **Clustering & High Availability**: Distributed architecture based on the **Raft** consensus algorithm for reliable state synchronization and fault tolerance.
- **Powerful Plugin System**: Extend the broker with out-of-process plugins managed by the built-in plugin host. Plugins are started as child processes and communicate with YedMQ over the local plugin protocol, making them language-agnostic and crash-isolated.
- **Security First**: 
  - Transport Layer Security via **TLS/SSL**.
  - Secure WebSocket (**WSS**) support.
  - Fine-grained access control (ACL) via plugins.
- **RESTful Management API**: A comprehensive set of APIs for managing clients, monitoring metrics, and controlling cluster state.
- **Cross-Platform**: Native support for X86_64 and AARCH64 (ARM) architectures.

## 🚀 Quick Start

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (1.75 or later)
- Protocol Buffers compiler (`protoc`)
- Make (optional, Unix-like convenience only)

### Installation

#### From Source

```bash
git clone https://github.com/designershao/YedMQ.git
cd YedMQ
cargo build --release -p yedmq
```

#### Using Docker

```bash
docker pull yedmq/yedmq:latest
docker run -p 1883:1883 -p 3456:3456 yedmq/yedmq:latest
```

### Running YedMQ

1. **Copy the example configuration:**
   ```bash
   cp yedmq.toml.example yedmq.toml
   ```
   Windows PowerShell:
   ```powershell
   Copy-Item yedmq.toml.example yedmq.toml
   ```

2. **Edit the configuration** (optional):
   ```bash
   vim yedmq.toml
   ```

3. **Start the broker:**
   ```bash
   cd target/release
   RUST_LOG=info ./yedmq
   ```
   Windows PowerShell:
   ```powershell
   Set-Location target/release
   $env:RUST_LOG = "info"
   .\yedmq.exe
   ```

4. **Test the connection:**
   ```bash
   # Using mosquitto_pub/sub
   mosquitto_sub -h localhost -t test/topic
   mosquitto_pub -h localhost -t test/topic -m "Hello YedMQ"
   ```

## 📖 Documentation

For detailed guides, please visit our [Official Documentation](https://www.yedmq.com):

- [Getting Started](https://www.yedmq.com/docs/get-started)
- [Configuration Guide](https://www.yedmq.com/docs/mqtt-configuration)
- [Cluster Setup](https://www.yedmq.com/docs/cluster-configuration)
- [Plugin Development](https://www.yedmq.com/docs/plugin-developer-guide/introduction/)
- [REST API Reference](https://www.yedmq.com/docs/category/yedmq-management-api)

## 🏗️ Architecture

YedMQ is designed with an actor-based concurrency model (via Actix) to handle millions of concurrent connections efficiently. Its distributed state is managed by a robust implementation of the Raft consensus protocol, ensuring consistency across the cluster.

```
┌─────────────────────────────────────────────────────────┐
│                    MQTT Clients                         │
└────────────┬────────────────────────────┬───────────────┘
             │                            │
    ┌────────▼────────┐          ┌───────▼────────┐
    │  YedMQ Node 1   │◄────────►│  YedMQ Node 2  │
    │  (Raft Leader)  │   Raft   │  (Follower)    │
    └────────┬────────┘          └───────┬────────┘
             │                            │
             └────────────┬───────────────┘
                          │
                  ┌───────▼────────┐
                  │  YedMQ Node 3  │
                  │  (Follower)    │
                  └────────────────┘
```

## 🔌 Plugin System

YedMQ uses a process-based plugin system. Each plugin lives in its own directory, is described by a `plugin.toml` manifest, and is launched by the broker as a separate process. The broker and plugin communicate through a local socket-based IPC channel, which keeps plugin crashes isolated from the broker process.

```bash
# Example plugin layout
plugins/
└── my-plugin/
    ├── plugin.toml
    └── my-plugin-binary
```

Example `plugin.toml`:

```toml
[plugin]
name = "my-plugin"
version = "0.1.0"
description = "Example YedMQ plugin"
author = "Your Name"

[runtime]
type = "process"
executable = "my-plugin-binary"
working_dir = "."
timeout_secs = 12
```

Typical flow:

1. Build your plugin as an executable.
2. Place the executable and `plugin.toml` in a subdirectory under the configured plugin directory.
3. Start YedMQ and let the plugin host discover, launch, and health-check the plugin.

See the [Plugin Configuration](https://www.yedmq.com/docs/plugin-configuration) and [Plugin Development Guide](https://www.yedmq.com/docs/plugin-developer-guide/quick-start) for details.

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

- 🐛 [Report a Bug](https://github.com/designershao/YedMQ/issues/new?template=bug_report.md)
- 💡 [Request a Feature](https://github.com/designershao/YedMQ/issues/new?template=feature_request.md)
- 📖 [Improve Documentation](https://github.com/designershao/YedMQ/tree/main/YedMQ-web-site)

## 📊 Roadmap

- [x] MQTT v3.1.1 support
- [x] Raft-based clustering
- [x] Plugin system
- [ ] MQTT v5.0 support
- [ ] Shared subscriptions
- [ ] Message persistence (disk-based)
- [ ] Prometheus metrics
- [ ] WebUI dashboard

See the full [Roadmap](https://www.yedmq.com/docs/roadmap) for details.

## 📄 License

YedMQ is released under the [Apache-2.0 License](LICENSE).

## 🙏 Acknowledgments

YedMQ is built with excellent open-source projects:
- [Actix](https://actix.rs/) - Actor framework
- [OpenRaft](https://github.com/datafuselabs/openraft) - Raft consensus
- [Tokio](https://tokio.rs/) - Async runtime
- [RocksDB](https://rocksdb.org/) - Persistent storage

## 📞 Contact

- Website: [www.yedmq.com](https://www.yedmq.com)
- GitHub Issues: [github.com/designershao/YedMQ/issues](https://github.com/designershao/YedMQ/issues)
- Discussions: [github.com/designershao/YedMQ/discussions](https://github.com/designershao/YedMQ/discussions)

---

<p align="center">Made with ❤️ by the YedMQ Team</p>
