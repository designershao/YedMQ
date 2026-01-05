<img alt="YedMQ logo" src="https://github.com/designershao/YedMQ/blob/main/assets/logo.png?raw=true"/>
<br>
<a href="https://www.yedmq.com"><b>Visit the site 🌐</b></a>

# YedMQ

**YedMQ** is a fully compliant，high-performance MQTT (v3.1.1) broker.

YedMQ is a fully compliant MQTT broker server written in Rust, designed for internet-of-things infrastructure.

# Feature

* MQTT v3.1.1 Protocol：
    * Plus all the original MQTT features of YedMQ, such as Full QoS support, $SYS topics, retained messages, session
      persistent, etc.
* Mutiple Tenant Support
    * The system provides a tenant isolation feature, which distinguishes related tenants based on different tenant IDs.
    * Each tenant's subscription information and data forwarding are completely isolated from one another.
* Plugin System：
    * The system provides a secondary development toolkit, which allows for special extensions to be made for specific
      scenarios, including but not limited to: authentication, authorization, message forwarding, and more.
* Websocket And Websocket TLS Support。
* Management API
    * The system provide management API based on REST API.
* Built-In Plugins：
    * ACL_MySQL : Integrate with MySQL, security plugin.
    * ACL_Postgresql: Integrate with Postgresql, security plugin.
    * ACL_FILE: Integrate with local file, security plugin。
* CPU Architecture Support：X86/X64, AARCH64

# Get Started

Compile from source code

```bash
git clone https://github.com/designershao/YedMQ.git
cd YedMQ
make build-all
```

Run the binary file

```bash
cd target/debug
RUST_LOG=INFO ./yedmq
```
