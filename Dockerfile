FROM rust:1.85-bookworm AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    cmake \
    libclang-dev \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY broker ./broker
COPY mqtt ./mqtt
COPY plugin_host ./plugin_host
COPY mock_plugin_harness ./mock_plugin_harness
COPY plugin_protocol ./plugin_protocol

RUN cargo build --release -p yedmq

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/yedmq

COPY --from=builder /build/target/release/yedmq /usr/local/bin/yedmq
COPY broker/yedmq.toml /opt/yedmq/yedmq.toml

RUN mkdir -p /opt/yedmq/plugins /var/lib/yedmq/store /var/lib/yedmq/clock

EXPOSE 1883 3456 3457 8083 8084 8883

CMD ["/usr/local/bin/yedmq"]
