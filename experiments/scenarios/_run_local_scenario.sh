#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

require_python3
ensure_result_dir >/dev/null

RESET_STACK="${RESET_STACK:-true}"
START_TIERING_WORKER="${START_TIERING_WORKER:-true}"
PRELOAD_OBJECTS="${PRELOAD_OBJECTS:-true}"
PRELOAD_AGE_WAIT_SEC="${PRELOAD_AGE_WAIT_SEC:-0}"
OBJECT_COUNT="${OBJECT_COUNT:-200}"
OBJECT_SIZE_BYTES="${OBJECT_SIZE_BYTES:-1048576}"
KEY_PREFIX="${KEY_PREFIX:-exp-${RUN_ID}}"
WORKLOAD_DURATION_SEC="${WORKLOAD_DURATION_SEC:-120}"
WORKLOAD_CONCURRENCY="${WORKLOAD_CONCURRENCY:-8}"
GET_PERCENT="${GET_PERCENT:-70}"
PRESSURE_PROFILE="${PRESSURE_PROFILE:-none}"
PRESSURE_DELAY_SEC="${PRESSURE_DELAY_SEC:-0}"
PRESSURE_DURATION_SEC="${PRESSURE_DURATION_SEC:-0}"
PRESSURE_WARMUP_SEC="${PRESSURE_WARMUP_SEC:-0}"
METRICS_INTERVAL_SEC="${METRICS_INTERVAL_SEC:-5}"
COLLECT_DURATION_SEC="${COLLECT_DURATION_SEC:-$((WORKLOAD_DURATION_SEC + PRESSURE_DURATION_SEC + PRESSURE_DELAY_SEC + 30))}"
SUMMARY_FILE="${SUMMARY_FILE:-${RESULT_DIR}/summary.csv}"

write_run_env

if [[ "${RESET_STACK}" == "true" ]]; then
  reset_stack
fi

start_core_stack
wait_api_health
wait_node_discovery_ready "${MIN_HEALTHY_NODES}"

if [[ "${PRELOAD_OBJECTS}" == "true" ]]; then
  OBJECT_COUNT="${OBJECT_COUNT}" \
  OBJECT_SIZE_BYTES="${OBJECT_SIZE_BYTES}" \
  KEY_PREFIX="${KEY_PREFIX}" \
  "${SCRIPT_DIR}/../workloads/prepare_objects.sh"

  if (( PRELOAD_AGE_WAIT_SEC > 0 )); then
    exp_log "Wait preload aging: ${PRELOAD_AGE_WAIT_SEC}s"
    sleep "${PRELOAD_AGE_WAIT_SEC}"
  fi
fi

DURATION_SEC="${COLLECT_DURATION_SEC}" \
INTERVAL_SEC="${METRICS_INTERVAL_SEC}" \
OUT_FILE="${RESULT_DIR}/metrics.csv" \
RAW_FILE="${RESULT_DIR}/admin_samples.jsonl" \
"${SCRIPT_DIR}/../collect/collect_admin_metrics.sh" &
metrics_pid="$!"

pressure_pid=""
case "${PRESSURE_PROFILE}" in
  none|"")
    ;;
  cpu)
    DELAY_SEC="${PRESSURE_DELAY_SEC}" DURATION_SEC="${PRESSURE_DURATION_SEC}" \
      "${SCRIPT_DIR}/../pressure/cpu_spike.sh" &
    pressure_pid="$!"
    ;;
  io)
    DELAY_SEC="${PRESSURE_DELAY_SEC}" DURATION_SEC="${PRESSURE_DURATION_SEC}" \
      "${SCRIPT_DIR}/../pressure/io_spike.sh" &
    pressure_pid="$!"
    ;;
  *)
    exp_log "ERROR: unknown PRESSURE_PROFILE=${PRESSURE_PROFILE}"
    exit 1
    ;;
esac

if (( PRESSURE_WARMUP_SEC > 0 )); then
  exp_log "Wait pressure warmup: ${PRESSURE_WARMUP_SEC}s"
  sleep "${PRESSURE_WARMUP_SEC}"
fi

if [[ "${START_TIERING_WORKER}" == "true" ]]; then
  start_tiering_worker
fi

DURATION_SEC="${WORKLOAD_DURATION_SEC}" \
CONCURRENCY="${WORKLOAD_CONCURRENCY}" \
GET_PERCENT="${GET_PERCENT}" \
PRELOAD_COUNT="${OBJECT_COUNT}" \
PUT_SIZE_BYTES="${OBJECT_SIZE_BYTES}" \
KEY_PREFIX="${KEY_PREFIX}" \
RESULT_FILE="${RESULT_DIR}/latency.csv" \
"${SCRIPT_DIR}/../workloads/mixed_put_get.sh"

if [[ -n "${pressure_pid}" ]]; then
  wait "${pressure_pid}" || true
fi
wait "${metrics_pid}" || true

"${SCRIPT_DIR}/../collect/summarize_latency.py" "${RESULT_DIR}/latency.csv" --out "${SUMMARY_FILE}" | tee "${RESULT_DIR}/summary.stdout.csv"

exp_log "Scenario complete: ${SCENARIO}"
exp_log "Result dir: ${RESULT_DIR}"
