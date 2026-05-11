#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID_ROOT="${RUN_ID_ROOT:-$(date -u +%Y%m%dT%H%M%SZ)}"

common_env=(
  "OBJECT_COUNT=${OBJECT_COUNT:-200}"
  "OBJECT_SIZE_BYTES=${OBJECT_SIZE_BYTES:-1048576}"
  "WORKLOAD_DURATION_SEC=${WORKLOAD_DURATION_SEC:-120}"
  "WORKLOAD_CONCURRENCY=${WORKLOAD_CONCURRENCY:-8}"
  "GET_PERCENT=${GET_PERCENT:-70}"
)

run_one() {
  local name="$1"
  local script="$2"
  shift 2
  echo "=== ${name} ==="
  env "${common_env[@]}" "RUN_ID=${RUN_ID_ROOT}-${name}" "${SCRIPT_DIR}/${script}" "$@"
}

run_one baseline baseline_no_migration.sh
run_one strategy-a strategy_a_age_based.sh
run_one strategy-b strategy_b_throttled.sh
run_one strategy-c strategy_c_pressure_aware.sh
