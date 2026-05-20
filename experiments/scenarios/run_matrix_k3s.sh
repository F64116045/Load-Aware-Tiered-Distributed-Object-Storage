#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID_ROOT="${RUN_ID_ROOT:-$(date -u +%Y%m%dT%H%M%SZ)}"
MATRIX_AGE_THRESHOLD_SEC="${MATRIX_AGE_THRESHOLD_SEC:-${AGE_THRESHOLD_SEC:-60}}"
MATRIX_PRELOAD_AGE_WAIT_SEC="${MATRIX_PRELOAD_AGE_WAIT_SEC:-${PRELOAD_AGE_WAIT_SEC:-70}}"
MATRIX_PRESSURE_PROFILE="${MATRIX_PRESSURE_PROFILE:-${PRESSURE_PROFILE:-none}}"
MATRIX_PRESSURE_DURATION_SEC="${MATRIX_PRESSURE_DURATION_SEC:-${PRESSURE_DURATION_SEC:-60}}"
MATRIX_PRESSURE_DELAY_SEC="${MATRIX_PRESSURE_DELAY_SEC:-${PRESSURE_DELAY_SEC:-0}}"
MATRIX_PRESSURE_WARMUP_SEC="${MATRIX_PRESSURE_WARMUP_SEC:-${PRESSURE_WARMUP_SEC:-0}}"
MATRIX_PRESSURE_CPUS="${MATRIX_PRESSURE_CPUS:-${PRESSURE_CPUS:-2}}"
MATRIX_HDD_WORKERS="${MATRIX_HDD_WORKERS:-${HDD_WORKERS:-2}}"
MATRIX_HDD_BYTES="${MATRIX_HDD_BYTES:-${HDD_BYTES:-512M}}"
MATRIX_METRICS_INTERVAL_SEC="${MATRIX_METRICS_INTERVAL_SEC:-${METRICS_INTERVAL_SEC:-5}}"

if [[ -z "${IMAGE:-}" ]]; then
  echo "ERROR: IMAGE is required, for example IMAGE=123456789012.dkr.ecr.ap-northeast-1.amazonaws.com/rec-store:exp-001" >&2
  exit 1
fi

common_env=(
  "IMAGE=${IMAGE}"
  "K8S_NAMESPACE=${K8S_NAMESPACE:-rec-store}"
  "COMPOSE_FILES=k3s"
  "PRELOAD_OBJECTS=${PRELOAD_OBJECTS:-true}"
  "OBJECT_COUNT=${OBJECT_COUNT:-200}"
  "OBJECT_SIZE_BYTES=${OBJECT_SIZE_BYTES:-1048576}"
  "WORKLOAD_DURATION_SEC=${WORKLOAD_DURATION_SEC:-120}"
  "WORKLOAD_CONCURRENCY=${WORKLOAD_CONCURRENCY:-8}"
  "GET_PERCENT=${GET_PERCENT:-70}"
  "AGE_THRESHOLD_SEC=${MATRIX_AGE_THRESHOLD_SEC}"
  "PRELOAD_AGE_WAIT_SEC=${MATRIX_PRELOAD_AGE_WAIT_SEC}"
  "PRESSURE_PROFILE=${MATRIX_PRESSURE_PROFILE}"
  "PRESSURE_DURATION_SEC=${MATRIX_PRESSURE_DURATION_SEC}"
  "PRESSURE_DELAY_SEC=${MATRIX_PRESSURE_DELAY_SEC}"
  "PRESSURE_WARMUP_SEC=${MATRIX_PRESSURE_WARMUP_SEC}"
  "PRESSURE_CPUS=${MATRIX_PRESSURE_CPUS}"
  "HDD_WORKERS=${MATRIX_HDD_WORKERS}"
  "HDD_BYTES=${MATRIX_HDD_BYTES}"
  "METRICS_INTERVAL_SEC=${MATRIX_METRICS_INTERVAL_SEC}"
)

run_one() {
  local name="$1"
  local scenario="$2"
  shift 2
  echo "=== ${name} ==="
  env "${common_env[@]}" "SCENARIO=${scenario}" "RUN_ID=${RUN_ID_ROOT}-${name}" "$@" "${SCRIPT_DIR}/_run_k3s_scenario.sh"
}

run_one baseline baseline_no_migration \
  START_TIERING_WORKER=false AGE_THRESHOLD_SEC="${MATRIX_AGE_THRESHOLD_SEC}"

run_one strategy-a strategy_a_age_based \
  START_TIERING_WORKER=true \
  TIERING_POLICY_VARIANT=A TIERING_TRIGGER_MODE=periodic TIERING_POLICY_PERIOD_SEC=5 \
  MAX_OBJECTS_PER_ROUND=200 WORKER_BW_LIMIT_MBPS=0

run_one strategy-b strategy_b_throttled \
  START_TIERING_WORKER=true \
  TIERING_POLICY_VARIANT=B TIERING_TRIGGER_MODE=periodic TIERING_POLICY_PERIOD_SEC=5 \
  MAX_OBJECTS_PER_ROUND=25 MAX_BYTES_PER_ROUND=33554432 WORKER_BW_LIMIT_MBPS=8

run_one strategy-c strategy_c_pressure_aware \
  START_TIERING_WORKER=true \
  TIERING_POLICY_VARIANT=C TIERING_TRIGGER_MODE=threshold \
  TIERING_THRESHOLD_CHECK_SEC=5 TIERING_THRESHOLD_COOLDOWN_SEC=5 \
  TIERING_IDLE_STABLE_ROUNDS=3 TIERING_IDLE_CPU_PCT=70 TIERING_IDLE_MEMORY_PCT=90 \
  TIERING_IDLE_IOWAIT_PCT=20 TIERING_IDLE_QUEUE_DEPTH=16 \
  TIERING_IDLE_MIN_NODE_RATIO=0.8 TIERING_IDLE_MIN_NODE_COUNT=4 \
  MAX_OBJECTS_PER_ROUND=25 MAX_BYTES_PER_ROUND=33554432 WORKER_BW_LIMIT_MBPS=8

comparison_file="${RESULT_ROOT:-${SCRIPT_DIR}/../results}/matrix-${RUN_ID_ROOT}-comparison.csv"
migration_file="${RESULT_ROOT:-${SCRIPT_DIR}/../results}/matrix-${RUN_ID_ROOT}-migration.csv"
fairness_file="${RESULT_ROOT:-${SCRIPT_DIR}/../results}/matrix-${RUN_ID_ROOT}-fairness.txt"
if ! python3 "${SCRIPT_DIR}/../collect/verify_matrix_fairness.py" \
  --result-root "${RESULT_ROOT:-${SCRIPT_DIR}/../results}" \
  --run-id-root "${RUN_ID_ROOT}" \
  --out "${fairness_file}"; then
  echo "=== fairness check ==="
  cat "${fairness_file}"
  echo "Fairness report: ${fairness_file}"
  exit 1
fi
python3 "${SCRIPT_DIR}/../collect/compare_summaries.py" \
  --result-root "${RESULT_ROOT:-${SCRIPT_DIR}/../results}" \
  --run-id-root "${RUN_ID_ROOT}" \
  --out "${comparison_file}"
python3 "${SCRIPT_DIR}/../collect/summarize_migration.py" \
  --result-root "${RESULT_ROOT:-${SCRIPT_DIR}/../results}" \
  --run-id-root "${RUN_ID_ROOT}" \
  --out "${migration_file}"

echo "=== fairness check ==="
cat "${fairness_file}"
echo "Fairness report: ${fairness_file}"

echo "=== latency comparison ==="
cat "${comparison_file}"
echo "Comparison CSV: ${comparison_file}"

echo "=== migration summary ==="
cat "${migration_file}"
echo "Migration CSV: ${migration_file}"
