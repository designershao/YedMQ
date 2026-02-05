<p align="center">
  <img alt="YedMQ logo" src="https://github.com/designershao/YedMQ/blob/main/assets/logo.png?raw=true" width="300" />
</p>

<p align="center">
  <a href="https://www.yedmq.com"><b>Website 🌐</b></a> |
  <a href="https://www.yedmq.com/docs/overview"><b>Documentation 📚</b></a>
</p>

---

# YedMQ

**YedMQ** is a high-performance, distributed MQTT broker written in **Rust**, specifically designed for modern IoT infrastructure. It is built for scalability, security, and extreme efficiency.

## Key Features

- **MQTT v3.1.1 Support**: Fully compliant with the MQTT v3.1.1 protocol, including all QoS levels (0, 1, 2), retained messages, last will, and persistent sessions.
- **High Performance**: Leveraging Rust's memory safety and zero-cost abstractions for low latency and high throughput.
- **Multiple Tenant Support**: Built-in isolation for multiple organizations. Each tenant has its own namespace, sessions, and topics, ensuring data privacy and security.
- **Clustering & High Availability**: Distributed architecture based on the **Raft** consensus algorithm for reliable state synchronization and fault tolerance.
- **Powerful Plugin System**: Extend the broker's functionality using the gRPC-based plugin protocol. Easily implement custom authentication, authorization, and message processing logic.
- **Security First**: 
  - Transport Layer Security via **TLS/SSL**.
  - Secure WebSocket (**WSS**) support.
  - Fine-grained access control (ACL) via plugins.
- **RESTful Management API**: A comprehensive set of APIs for managing clients, monitoring metrics, and controlling cluster state.
- **Cross-Platform**: Native support for X86_64 and AARCH64 (ARM) architectures.

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (latest stable version)
- Make (optional, for build shortcuts)

### Build from Source

```bash
git clone https://github.com/designershao/YedMQ.git
cd YedMQ
# Build debug version
make build-all
```

To build the release version for production:

```bash
make build-all-release
```

### Run the Broker

```bash
cd target/debug
# Optionally set the log level
RUST_LOG=info ./yedmq
```

By default, YedMQ will look for `yedmq.toml` in its current directory.

## Documentation

For detailed configuration guides, plugin development, and architectural details, please visit our [Official Documentation](https://www.yedmq.com).

## Architecture

YedMQ is designed with an actor-based concurrency model (via Actix) to handle millions of concurrent connections efficiently. Its distributed state is managed by a robust implementation of the Raft consensus protocol, ensuring consistency across the cluster.

## License

YedMQ is released under the [Apache-2.0 License](LICENSE).