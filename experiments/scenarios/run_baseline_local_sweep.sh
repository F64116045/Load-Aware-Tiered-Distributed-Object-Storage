#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_ROOT="${RESULT_ROOT:-${SCRIPT_DIR}/../results}"
RUN_ID_ROOT="${RUN_ID_ROOT:-$(date -u +%Y%m%dT%H%M%SZ)-baseline-sweep}"

SWEEP_STORAGE_IO_WORKERS="${SWEEP_STORAGE_IO_WORKERS:-1 2 4}"
SWEEP_DURABILITY_MODES="${SWEEP_DURABILITY_MODES:-sync write}"
SWEEP_CONCURRENCIES="${SWEEP_CONCURRENCIES:-1 2 4}"

OBJECT_COUNT="${OBJECT_COUNT:-50}"
OBJECT_SIZE_BYTES="${OBJECT_SIZE_BYTES:-1048576}"
WORKLOAD_DURATION_SEC="${WORKLOAD_DURATION_SEC:-30}"
GET_PERCENT="${GET_PERCENT:-70}"
METRICS_INTERVAL_SEC="${METRICS_INTERVAL_SEC:-5}"
COLLECT_DURATION_SEC="${COLLECT_DURATION_SEC:-$((WORKLOAD_DURATION_SEC + 30))}"
PRELOAD_OBJECTS="${PRELOAD_OBJECTS:-true}"
PRELOAD_AGE_WAIT_SEC="${PRELOAD_AGE_WAIT_SEC:-0}"
BUILD_STACK_FIRST="${BUILD_STACK_FIRST:-true}"
BUILD_STACK_REST="${BUILD_STACK_REST:-false}"

index_file="${RESULT_ROOT}/sweep-${RUN_ID_ROOT}-index.csv"
comparison_file="${RESULT_ROOT}/sweep-${RUN_ID_ROOT}-comparison.csv"
phase_file="${RESULT_ROOT}/sweep-${RUN_ID_ROOT}-phase-latency.csv"
bottleneck_file="${RESULT_ROOT}/sweep-${RUN_ID_ROOT}-phase-bottlenecks.csv"

mkdir -p "${RESULT_ROOT}"
printf 'run_id,storage_io_workers,storage_durability_mode,workload_concurrency,result_dir\n' >"${index_file}"

first_run=true
for workers in ${SWEEP_STORAGE_IO_WORKERS}; do
  for durability in ${SWEEP_DURABILITY_MODES}; do
    for concurrency in ${SWEEP_CONCURRENCIES}; do
      run_id="${RUN_ID_ROOT}-w${workers}-${durability}-c${concurrency}"
      build_stack="${BUILD_STACK_REST}"
      if [[ "${first_run}" == "true" ]]; then
        build_stack="${BUILD_STACK_FIRST}"
        first_run=false
      fi

      echo "=== baseline sweep: workers=${workers} durability=${durability} concurrency=${concurrency} ==="
      env \
        RUN_ID="${run_id}" \
        SCENARIO=baseline_no_migration \
        RESET_STACK=true \
        BUILD_STACK="${build_stack}" \
        START_TIERING_WORKER=false \
        PRELOAD_OBJECTS="${PRELOAD_OBJECTS}" \
        PRELOAD_AGE_WAIT_SEC="${PRELOAD_AGE_WAIT_SEC}" \
        AGE_THRESHOLD_SEC=86400 \
        PRESSURE_PROFILE=none \
        STORAGE_IO_WORKERS="${workers}" \
        STORAGE_DURABILITY_MODE="${durability}" \
        OBJECT_COUNT="${OBJECT_COUNT}" \
        OBJECT_SIZE_BYTES="${OBJECT_SIZE_BYTES}" \
        WORKLOAD_DURATION_SEC="${WORKLOAD_DURATION_SEC}" \
        WORKLOAD_CONCURRENCY="${concurrency}" \
        GET_PERCENT="${GET_PERCENT}" \
        METRICS_INTERVAL_SEC="${METRICS_INTERVAL_SEC}" \
        COLLECT_DURATION_SEC="${COLLECT_DURATION_SEC}" \
        "${SCRIPT_DIR}/baseline_no_migration.sh"

      result_dir="${RESULT_ROOT}/baseline_no_migration/${run_id}"
      printf '%s,%s,%s,%s,%s\n' \
        "${run_id}" "${workers}" "${durability}" "${concurrency}" "${result_dir}" >>"${index_file}"
    done
  done
done

python3 "${SCRIPT_DIR}/../collect/compare_summaries.py" \
  --result-root "${RESULT_ROOT}" \
  --run-id-root "${RUN_ID_ROOT}" \
  --out "${comparison_file}"
python3 "${SCRIPT_DIR}/../collect/analyze_phase_latency.py" \
  --result-root "${RESULT_ROOT}" \
  --run-id-root "${RUN_ID_ROOT}" \
  --out "${phase_file}"
python3 "${SCRIPT_DIR}/../collect/summarize_phase_bottlenecks.py" \
  --phase-csv "${phase_file}" \
  --latency-csv "${comparison_file}" \
  --operation PUT \
  --out "${bottleneck_file}"

echo "=== baseline sweep complete ==="
echo "Sweep index: ${index_file}"
echo "Latency comparison CSV: ${comparison_file}"
echo "Phase latency CSV: ${phase_file}"
echo "PUT bottleneck CSV: ${bottleneck_file}"

echo "=== latency comparison ==="
cat "${comparison_file}"

echo "=== PUT phase bottleneck summary ==="
cat "${bottleneck_file}"
