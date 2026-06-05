# Release Gate

YedMQ has two practical validation tiers: a fast main-repository release gate and an optional extended cluster smoke gate. Use the fast gate before ordinary commits and pull requests. Use the extended gate before release candidates, cluster changes, routing changes, session-state changes, or any work that could affect failover behavior.

## Fast Gate

Run from the `YedMQ` repository root:

```bash
./scripts/run_release_gate.sh
```

The script resolves the repository root from its own path, so it can also be called from another current directory. It runs:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets`
- `cargo test -p yedmq-mqtt`
- `cargo test -p yedmq --lib -- --test-threads=1`
- `cargo test -p yedmq-plugin-host`
- `cargo test -p acl_file`

The script prints each command before running it and exits on the first failure. A successful fast gate ends with:

```text
Cluster smoke skipped. Set RUN_CLUSTER_SMOKE=1 to run the sibling stability harness.
```

## Extended Cluster Smoke

The cluster smoke gate uses the sibling stability harness at `../YedMQ-stability-harness`. It is intentionally not part of the default fast gate because it starts embedded clusters, injects failover and restart scenarios, and takes longer than normal compile/test checks.

Run:

```bash
RUN_CLUSTER_SMOKE=1 ./scripts/run_release_gate.sh
```

When the sibling harness is present, this delegates to:

```bash
../YedMQ-stability-harness/scripts/run_cluster_smoke.sh
```

That harness runs:

- `cluster-soak`
- `cluster-persistent-session-replay`
- `cluster-follower-restart`

It writes a timestamped summary under `../YedMQ-stability-harness/artifacts/cluster-smoke/`. Start with the generated `summary.txt`, then inspect each `.key.log` if a scenario reports a warning or failure.

## Dependencies

The fast gate requires the normal Rust build dependencies for YedMQ:

- Rust toolchain with Cargo
- Protocol Buffers compiler, `protoc`
- OpenSSL development headers on platforms that need them
- LLVM/Clang on Windows when native dependencies require it

The extended cluster smoke gate additionally needs permission to bind local TCP ports, create local plugin IPC sockets, start child processes, and write artifacts in the sibling harness directory.

## Interpreting Failures

Treat formatting, compile, unit-test, and package-test failures as release blockers.

Some integration-style failures can be environmental in restricted sandboxes. Common environmental symptoms include local socket creation failures such as `Operation not permitted`, local TCP bind failures, or child-process startup failures when the environment forbids process spawning. When that happens, record the exact command and error, then rerun the same command in an environment with local socket and process permissions.

Do not use the fast gate as proof of cluster failover stability. It proves that the main workspace compiles and core package tests pass. The extended cluster smoke gate is the release-candidate check for cluster routing, session replay, leader failover, and follower restart behavior.
