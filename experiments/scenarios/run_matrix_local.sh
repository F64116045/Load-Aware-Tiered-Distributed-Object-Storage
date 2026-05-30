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

common_env=(
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
  "STORAGE_DURABILITY_MODE=${STORAGE_DURABILITY_MODE:-sync}"
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

comparison_file="${RESULT_ROOT:-${SCRIPT_DIR}/../results}/matrix-${RUN_ID_ROOT}-comparison.csv"
migration_file="${RESULT_ROOT:-${SCRIPT_DIR}/../results}/matrix-${RUN_ID_ROOT}-migration.csv"
fairness_file="${RESULT_ROOT:-${SCRIPT_DIR}/../results}/matrix-${RUN_ID_ROOT}-fairness.txt"
phase_file="${RESULT_ROOT:-${SCRIPT_DIR}/../results}/matrix-${RUN_ID_ROOT}-phase-latency.csv"
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
python3 "${SCRIPT_DIR}/../collect/analyze_phase_latency.py" \
  --result-root "${RESULT_ROOT:-${SCRIPT_DIR}/../results}" \
  --run-id-root "${RUN_ID_ROOT}" \
  --out "${phase_file}"

echo "=== fairness check ==="
cat "${fairness_file}"
echo "Fairness report: ${fairness_file}"

echo "=== latency comparison ==="
cat "${comparison_file}"
echo "Comparison CSV: ${comparison_file}"

echo "=== migration summary ==="
cat "${migration_file}"
echo "Migration CSV: ${migration_file}"

echo "=== phase latency summary ==="
cat "${phase_file}"
echo "Phase latency CSV: ${phase_file}"
