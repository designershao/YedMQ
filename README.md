<p align="center">
  <img alt="YedMQ logo" src="https://github.com/designershao/YedMQ/blob/main/assets/logo.png?raw=true" width="300" />
</p>

<p align="center">
  <a href="https://www.yedmq.com"><b>Website 🌐</b></a> |
  <a href="https://www.yedmq.com/docs/overview"><b>Documentation 📚</b></a>
</p>

<p align="center">
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
- OpenSSL development headers (`libssl-dev` on Ubuntu/Debian)
- Make (optional, Unix-like convenience only)

On Ubuntu/Debian, you can install the common system dependencies with:

```bash
sudo apt-get update
sudo apt-get install -y protobuf-compiler pkg-config libssl-dev
```

On Windows, install these build dependencies before running `cargo build`:

- `protoc`
- LLVM/Clang

Some native dependencies in this workspace rely on Clang being discoverable during the build.
In PowerShell, a typical setup looks like:

```powershell
$env:LIBCLANG_PATH = "C:\Program Files\LLVM\bin"
$env:PATH = "$env:LIBCLANG_PATH;$env:PATH"
```

If your `protoc.exe` directory is not already on `PATH`, add it as well before building.

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
```

The published image ships with the locked-down example configuration from this repository.
Mount your own `yedmq.toml` into `/opt/yedmq/yedmq.toml` when you want to enable client access
or expose the management API.

### Running YedMQ

1. **Copy the example configuration:**
   ```bash
   cp yedmq.toml.example yedmq.toml
   ```
   Windows PowerShell:
   ```powershell
   Copy-Item yedmq.toml.example yedmq.toml
   ```

2. **Edit the configuration before first start:**
   ```bash
   vim yedmq.toml
   ```

   The example file is intentionally locked down:
   - Client access is deny-by-default unless an auth plugin approves the request or you opt into the local fallback below
   - The management API binds to `127.0.0.1`
   - No management API users are created automatically

   For a local smoke test without any auth plugin, temporarily change this block:
   ```toml
   [plugin]
   default_authorize_result = true
   default_authenticate_result = true
   ```
   Do not use that fallback in shared or production environments.

   If you need the management API, also add at least one user under `[listener.api.auth]`.

3. **Start the broker:**
   ```bash
   RUST_LOG=info ./target/release/yedmq start -c yedmq.toml
   ```
   Running `./target/release/yedmq` without a subcommand is still supported and starts the broker
   with the default configuration search path.

   Windows PowerShell:
   ```powershell
   $env:RUST_LOG = "info"
   .\target\release\yedmq.exe start -c yedmq.toml
   ```

   Docker:
   ```bash
   docker run --rm \
     -p 1883:1883 \
     -v "$(pwd)/yedmq.toml:/opt/yedmq/yedmq.toml:ro" \
     yedmq/yedmq:latest
   ```

4. **Test the connection** after enabling the local development fallback above or installing an auth plugin:
   ```bash
   # Using mosquitto_pub/sub
   mosquitto_sub -h localhost -t test/topic
   mosquitto_pub -h localhost -t test/topic -m "Hello YedMQ"
   ```

### Authentication And First Run

YedMQ currently has two separate authentication surfaces:

- `[listener.api.auth].users` protects the REST management API only.
- MQTT client login and topic authorization are evaluated through the plugin hook chain.

That means adding a REST API user does **not** create a MQTT username/password login.

For the current broker behavior, the effective MQTT fallback on first run is controlled by:

```toml
[plugin]
default_authenticate_result = false
default_authorize_result = false
```

With the shipped defaults, if you start YedMQ without any authentication or ACL plugin, MQTT
clients will be rejected. This is expected and is meant to prevent accidentally exposing an
open broker.

For an initial local evaluation, pick one of these approaches:

1. Install an authentication/ACL plugin and let the plugin decide who can connect.
2. On a local machine only, temporarily set:

```toml
[plugin]
default_authenticate_result = true
default_authorize_result = true
```

Use the second option only for local smoke tests. For any shared, staged, or production
deployment, keep the defaults locked down and use a real authentication plugin.

## CLI Operations

The `yedmq` binary also provides basic operational commands:

```bash
./target/release/yedmq version
./target/release/yedmq config check -c yedmq.toml
```

Status commands call the REST management API, which listens on `127.0.0.1:3456` by default and
requires a user under `[listener.api.auth].users`.

```bash
export YEDMQ_API_USER=admin
export YEDMQ_API_PASSWORD=replace_me

./target/release/yedmq node status
./target/release/yedmq cluster status
./target/release/yedmq broker stats --output json
```

For scripts that should avoid putting the password in process arguments:

```bash
printf '%s' 'replace_me' | \
  ./target/release/yedmq cluster status --user admin --password-stdin
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

Bug reports, feature requests, documentation fixes, and pull requests are welcome.
For larger changes, open a GitHub issue or discussion first so the scope is clear before implementation.

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

---

<p align="center">Made with ❤️ by the YedMQ Team</p>
