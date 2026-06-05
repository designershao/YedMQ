#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CARGO_BIN="${CARGO_BIN:-cargo}"

run() {
  echo
  echo "==> $*"
  (cd "$ROOT_DIR" && "$@")
}

run "$CARGO_BIN" fmt --all -- --check
run "$CARGO_BIN" check --workspace --all-targets
run "$CARGO_BIN" test -p yedmq-mqtt
run "$CARGO_BIN" test -p yedmq --lib -- --test-threads=1
run "$CARGO_BIN" test -p yedmq-plugin-host
run "$CARGO_BIN" test -p acl_file

if [[ "${RUN_CLUSTER_SMOKE:-0}" == "1" ]]; then
  HARNESS_SCRIPT="$ROOT_DIR/../YedMQ-stability-harness/scripts/run_cluster_smoke.sh"
  if [[ ! -x "$HARNESS_SCRIPT" ]]; then
    echo "cluster smoke requested but harness script is missing or not executable: $HARNESS_SCRIPT" >&2
    exit 2
  fi

  echo
  echo "==> $HARNESS_SCRIPT"
  (cd "$(dirname "$HARNESS_SCRIPT")" && ./run_cluster_smoke.sh)
else
  echo
  echo "Cluster smoke skipped. Set RUN_CLUSTER_SMOKE=1 to run the sibling stability harness."
fi
