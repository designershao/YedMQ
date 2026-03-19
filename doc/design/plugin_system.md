# Plugin System Design

This document is based on the current implementation in `plugin_host`, `plugin_protocol`, and `broker`. The current plugin system is not a "dynamic library + Rust trait registration" model. It is a "process-based plugin + local socket + Protobuf protocol" model.

## 1. Goals

The current plugin system is designed to:

- isolate plugins from the Broker main process so plugin code does not run in-process;
- extend authentication, authorization, message handling, and event notification through a unified protocol and hook model;
- allow multiple plugins to form a hook chain ordered by priority;
- let the Broker observe plugin state, heartbeat status, and basic metadata at runtime.

## 2. Architecture Overview

The current implementation consists of four parts:

1. `broker`
   Creates `PluginManager` during startup and invokes plugin hooks during authentication, authorization, disconnect, and message publish flows.
2. `plugin_host`
   Handles plugin discovery, process startup, local socket listening, protocol I/O, request timeout tracking, hook registration, and lifecycle management.
3. `plugin_protocol`
   Defines the Protobuf message schema used for plugin communication.
4. plugin process
   Each plugin is an independent executable started by `plugin_host` and connected to the host over a local socket.

High-level flow:

```text
broker
  -> PluginManager::new() scans plugin directories
  -> start_listener() starts the local socket listener
  -> start_heartbeat_check_task() starts heartbeat checks
  -> start_all_plugins() starts all plugin processes

plugin process
  -> connects to the host using --socket-path
  -> receives InitializeRequest
  -> returns InitializeResponse(auth_code, hooks)

plugin_host
  -> identifies the plugin instance by auth_code
  -> registers hooks
  -> marks the plugin state as Running
```

## 3. Plugin Discovery and Package Layout

The plugin directory is configured by `plugin.dir`, with the default shown in `broker/yedmq.toml`:

```toml
[plugin]
dir = "./plugins"
local_socket_path = "/tmp/yedmq_plugin.sock"
default_authorize_result = true
default_authenticate_result = true
```

`PluginLoader` scans only first-level subdirectories under `plugin.dir`. Each plugin directory must contain a `plugin.toml`.

Recommended layout:

```text
plugins/
  my_plugin/
    plugin.toml
    my_plugin
```

Notes:

- The current implementation resolves plugin paths using `[plugin].name`, so in practice the directory name needs to match `plugin.name`.
- Only directory-based plugins are supported.
- Only `process` runtime is supported.
- Dynamic library loading is not part of the current implementation.

## 4. Plugin Manifest

The actual manifest format currently used is:

```toml
[plugin]
name = "mock_plugin_harness"
version = "0.1.0"
description = "A test plugin"
author = "Test Author"
license = "MIT"
homepage = "http://test.com"
repository = "https://github.com"

[runtime]
type = "process"
executable = "mock_plugin_harness"
args = ["--config", "{\"initialize\":{\"status\":\"ready\"}}"]
env = {}
working_dir = "."
timeout_secs = 12
```

### 4.1 `plugin` section

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | yes | Plugin name and the key used to resolve its runtime directory |
| `version` | string | yes | Plugin version |
| `description` | string | yes | Plugin description |
| `author` | string | yes | Plugin author |
| `license` | string | no | License |
| `homepage` | string | no | Homepage |
| `repository` | string | no | Repository URL |

### 4.2 `runtime` section

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `type` | string | yes | Only `process` is currently supported |
| `executable` | string | yes | Executable path, resolved relative to the plugin directory |
| `args` | string array | no | Startup arguments |
| `env` | map<string, string> | no | Extra environment variables |
| `working_dir` | string | no | Working directory, resolved relative to the plugin directory |
| `timeout_secs` | u64 | no | Defined in the manifest, but not used by the current runtime logic |

## 5. Startup and Lifecycle

### 5.1 Broker startup phase

The Broker initializes the plugin system in `broker/src/app.rs`:

1. build `PluginHostConfig` from Broker settings;
2. create `PluginManager`, which scans the plugin directory during construction;
3. start the local socket listener;
4. start the heartbeat task;
5. start all discovered plugins.

### 5.2 Plugin startup phase

The real behavior of `PluginManager::start_plugin()` is:

1. load the plugin manifest;
2. generate a random `auth_code`;
3. build the child process command and append two fixed arguments:
   - `--auth-code <generated_code>`
   - `--socket-path <local_socket_path>`
4. spawn the plugin process and mark its state as `Starting`;
5. create tasks to wait for process exit and collect process logs.

### 5.3 Initialization handshake

After the plugin process connects to the local socket, the host immediately sends an `InitializeRequest`. The current request includes:

- `broker_info.version`
- `broker_info.node_id`
- `broker_info.cluster_name`

In the current implementation:

- `plugin_config` is always `None`
- `required_capabilities` is always an empty array

The plugin must return an `InitializeResponse`. The host currently uses only two fields from that response:

- `auth_code`
- `hooks`

The host uses `auth_code` to match the socket connection with the process that was previously started. After a successful match, it:

- stores the IPC sender;
- changes the plugin state to `Running`;
- registers the declared hooks into `HookManager`.

Although `status`, `capabilities`, and `plugin_info` exist in the protocol, the current host implementation does not use them for readiness checks or capability negotiation.

### 5.4 Lifecycle states

The state enum currently defines:

- `Discovered`
- `Starting`
- `Running`
- `Stopping`
- `Stopped`
- `Failed`

The states actually used today mainly follow this path:

```text
scan -> Starting -> Running -> Stopped
                    \-> Failed
```

Notes:

- If process startup fails, the plugin is recorded as `Failed`.
- If heartbeat fails three times in a row, the plugin is marked as `Failed`.
- `restart_plugin()` is currently implemented as stop followed by start.
- `max_restart_attempts` exists in config but is not part of any active auto-restart logic.

## 6. Communication Protocol

### 6.1 Transport layer

Plugins communicate with the host over a local socket using a custom binary frame:

- Magic Number: `0x5514`
- Protocol Version: `0x01`
- Header size: 9 bytes
  - 4 bytes magic
  - 1 byte version
  - 4 bytes payload length
- Max frame size: 4 MB

The frame payload is a Protobuf-encoded `ProtocolMessage`.

### 6.2 Message model

`ProtocolMessage` contains the following key fields:

- `version`
- `type`
- `id`
- `timestamp`
- `source`
- `target`
- `method`
- `params`
- `result`
- `error`
- `metadata`

Requests and responses are correlated by `id`. Internally, `PluginManager` uses `InflightManager` to track request-response pairs.

### 6.3 Timeout policy

The most important timeouts are currently hardcoded to 5 seconds:

- initialization response timeout: 5 seconds;
- request-style hook timeout: 5 seconds;
- heartbeat ping timeout: 5 seconds.

`runtime.timeout_secs` is not wired into these runtime checks.

## 7. Hook Model

### 7.1 Hook registration

Plugins declare hooks in `InitializeResponse.hooks`, for example:

```text
Hook {
  name: "Authenticate",
  priority: 1
}
```

`HookManager` sorts hooks in ascending `priority`. A smaller number means higher priority.

### 7.2 Current hook support matrix

| Hook | Implemented in `plugin_host` | Wired into Broker |
| --- | --- | --- |
| `Authenticate` | yes | yes |
| `Authorize` | yes | yes |
| `MessagePublished` | yes | yes |
| `ClientDisconnected` | yes | yes |
| `ClientConnected` | yes | no |
| `SubscribeAdded` | yes | no |
| `SubscribeRemoved` | yes | no |
| `OnMessagePublish` | yes | no |
| `OnMessageSubscribe` | yes | no |
| `OnMessageUnsubscribe` | no | no |
| `OnStatsRequest` | no | no |

Notes:

- `OnMessageUnsubscribe` and `OnStatsRequest` are defined only in the hook enum today. They do not have real call paths.
- The Broker currently integrates mainly authentication, authorization, message-published event, and client-disconnected event flows.

### 7.3 Request-style hook semantics

#### Authenticate

Invocation point:

- MQTT CONNECT authentication

Chain rules:

- plugins are called in priority order;
- if any plugin returns `authenticated = false`, the chain stops immediately and access is denied;
- if any plugin times out, the chain stops immediately and access is denied;
- if multiple plugins return success, their `tenant_id` values must match, otherwise access is denied;
- if no `Authenticate` hook is registered, the host returns `default_authenticate_result`.

Fields defined in the response but not propagated further by the Broker today:

- `permissions`
- `session_data`

#### Authorize

Invocation points:

- PUBLISH authorization
- SUBSCRIBE authorization

Chain rules:

- plugins are called in priority order;
- if any plugin returns `authorized = false`, the chain stops immediately and access is denied;
- if any plugin times out, the chain stops immediately and access is denied;
- if no `Authorize` hook is registered, the host returns `default_authorize_result`.

`AuthorizeResponse.modified_context` exists in the protocol, but the host does not currently expose it upstream. The effective result is always an empty map.

#### OnMessagePublish

Invocation point:

- implemented in `plugin_host`, but not currently wired into the Broker

Chain rules:

- a plugin may return a modified message;
- if `continue_chain = false`, no lower-priority plugin is called;
- if no plugin handles it, the default behavior is allow plus original message.

#### OnMessageSubscribe

Invocation point:

- implemented in `plugin_host`, but not currently wired into the Broker

Chain rules:

- the initial result allows all requested subscriptions;
- each plugin may override `allowed` and `granted_qos` for individual topics;
- if `continue_chain = false`, no lower-priority plugin is called.

### 7.4 Event-style hook semantics

The following hooks are fire-and-forget events and do not require a response:

- `ClientConnected`
- `ClientDisconnected`
- `MessagePublished`
- `SubscribeAdded`
- `SubscribeRemoved`

Events that the Broker actually emits today:

- `MessagePublished`
- `ClientDisconnected`

## 8. Current Broker Integration Points

### 8.1 Authentication

`broker/src/connection.rs` builds `AuthenticateRequest` while handling MQTT CONNECT. The request includes:

- `client_id`
- `username`
- `password`
- `client_ip`
- `client_cert`
- `protocol_version`

If authentication succeeds, the Broker continues session creation. If the plugin returns `tenant_id`, the Broker uses it; otherwise it falls back to `"public"`.

### 8.2 Authorization

`broker/src/session/session_actor.rs` currently invokes `Authorize` in these cases:

- when a client publishes a message;
- when a client subscribes to a topic.

### 8.3 Event notification

The Broker currently emits:

- `MessagePublished`
  - sent after publish authorization succeeds;
- `ClientDisconnected`
  - sent when a session disconnects normally.

### 8.4 REST management API

The current REST endpoint `GET /api/v1/plugins` returns basic metadata for plugins managed by `PluginManager`:

- `name`
- `version`
- `description`
- `author`

The data source is `running_plugins`, so this is closer to "plugins started and managed by the host" than "all plugin manifests discovered on disk".

## 9. Heartbeat, State, and Logs

### 9.1 Heartbeat

`start_heartbeat_check_task()` periodically sends `Ping` to plugins in `Running` state according to `health_check_interval_secs`.

Rules:

- one ping timeout counts as one failure;
- after three consecutive failures, the plugin is marked as `Failed`;
- the current implementation marks the state only and does not auto-restart the plugin.

### 9.2 Process exit

The host keeps a dedicated wait task for each plugin process:

- when the process exits normally or is killed, the state becomes `Stopped`;
- `stop_plugin()` stops a plugin by killing the child process;
- `restart_plugin()` is stop + start.

### 9.3 Log collection

The host caches plugin logs in memory and keeps at most the latest 1000 lines.

Important detail:

- the current implementation actually reads plugin `stderr`;
- it does not collect `stdout`.

So if a plugin wants its logs to be visible through the host-side log buffer, it should write structured logs to standard error.

## 10. Current Limitations and Incomplete Areas

The following items are present in code as fields or design intent, but are not fully implemented yet:

1. Only `process` runtime is supported. Dynamic library plugins are not supported.
2. `runtime.timeout_secs` does not participate in runtime timeout control.
3. `PluginHostConfig.max_restart_attempts` is not active, so failed plugins are not auto-restarted.
4. `PluginHostConfig.shutdown_signal` is currently unused.
5. `InitializeRequest.plugin_config` is always empty, so custom plugin config is not passed through to plugin processes.
6. `InitializeResponse.status`, `capabilities`, and `plugin_info` are not consumed by the host.
7. `AuthenticateResponse.permissions` and `session_data` are not propagated into Broker logic.
8. `AuthorizeResponse.modified_context` is not propagated into Broker logic.
9. `OnMessagePublish` and `OnMessageSubscribe` are implemented in `plugin_host`, but not wired into the Broker yet.
10. `OnMessageUnsubscribe` and `OnStatsRequest` only exist as hook names today and do not have real invocation paths.
11. `SubscribeRemoved` has a host-side call function, but it currently sends method `SubscriptionAdded`, so it should not be documented as a stable external behavior yet.

## 11. Practical Constraints for Plugin Authors

Based on the current implementation, plugin authors need to follow these rules:

1. A plugin must be an executable, not a dynamic library.
2. The executable must live inside the plugin directory and be declared by `runtime.executable` in `plugin.toml`.
3. The process must accept and use `--auth-code` and `--socket-path`.
4. The plugin must actively connect to the host local socket.
5. The plugin must understand `ProtocolMessage` and return a valid `InitializeResponse`.
6. If the plugin declares hooks, it must handle the corresponding Protobuf request and response types correctly.

These constraints describe the plugin model that is actually implemented today and should be treated as the baseline for future evolution.
