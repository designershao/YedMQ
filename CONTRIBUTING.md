# Contributing to YedMQ

Thanks for contributing to YedMQ.

YedMQ is still under active development and is not yet suitable for production use. Small, focused contributions are the easiest to review and merge.

## Before you start

- For typo fixes, small documentation updates, or isolated bug fixes, feel free to open a pull request directly.
- For larger changes, new features, protocol changes, API changes, or refactors, open a GitHub issue or discussion first so the scope is agreed before implementation.
- Keep pull requests focused. Avoid mixing unrelated refactors with functional changes.

## Development setup

Prerequisites:

- Rust 1.75 or later
- `protoc`
- OpenSSL development headers (`libssl-dev` on Ubuntu/Debian)
- `make` is optional and only used for convenience targets

On Ubuntu/Debian, a typical setup is:

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

If your `protoc.exe` directory is not already on `PATH`, add it before building.

Common commands:

```bash
cargo build --workspace
cargo test --workspace --all-targets
```

If you want a local build that stages the bundled example plugin:

```bash
make build-all
```

For a release build with the staged example plugin:

```bash
make build-all-release
```

## Local configuration

To run the broker locally, copy the example configuration:

```bash
cp yedmq.toml.example yedmq.toml
```

- Treat `yedmq.toml` as a local development file.
- Do not commit passwords, certificates, private keys, or environment-specific endpoints.
- The example configuration is locked down by default. If you temporarily relax authentication or authorization for a local smoke test, do not reuse that configuration in shared or production environments.

## Code and test expectations

Before opening a pull request, please run:

```bash
cargo fmt --all
cargo test --workspace --all-targets
```

If you have `clippy` installed, running it is also encouraged:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

When making changes:

- Add or update tests for behavior changes or bug fixes.
- Update documentation when configuration, behavior, or APIs change.
- Preserve secure-by-default behavior unless the change explicitly requires otherwise.
- Avoid committing generated artifacts or machine-local files.

## Pull request checklist

Please include:

- A short description of the problem and the approach you took
- Links to the related issue or discussion when applicable
- Notes about testing performed
- Notes about configuration, compatibility, or security impact when relevant

## Review process

- Maintainers may ask for scope reduction before reviewing a large pull request.
- Behavior-changing pull requests should usually include tests.
- Security-sensitive changes may require additional review before merge.

## Licensing

By submitting code, documentation, or other content to this repository, you agree that your contribution will be licensed under the Apache-2.0 license used by this project.
