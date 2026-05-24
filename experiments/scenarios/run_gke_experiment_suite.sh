#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_ROOT="${RESULT_ROOT:-${SCRIPT_DIR}/../results}"
SUITE_RUN_ID_ROOT="${SUITE_RUN_ID_ROOT:-$(date -u +%Y%m%dT%H%M%SZ)-gke-suite}"
GKE_SUITE_PROFILES="${GKE_SUITE_PROFILES:-none cpu io}"

if [[ -z "${IMAGE:-}" ]]; then
  echo "ERROR: IMAGE is required, for example IMAGE=asia-east1-docker.pkg.dev/<project>/<repo>/rec-store:gke-exp-001" >&2
  exit 1
fi

common_env=(
  "IMAGE=${IMAGE}"
  "AGE_THRESHOLD_SEC=${AGE_THRESHOLD_SEC:-60}"
  "PRELOAD_AGE_WAIT_SEC=${PRELOAD_AGE_WAIT_SEC:-90}"
  "OBJECT_COUNT=${OBJECT_COUNT:-50}"
  "OBJECT_SIZE_BYTES=${OBJECT_SIZE_BYTES:-1048576}"
  "WORKLOAD_DURATION_SEC=${WORKLOAD_DURATION_SEC:-60}"
  "WORKLOAD_CONCURRENCY=${WORKLOAD_CONCURRENCY:-2}"
  "GET_PERCENT=${GET_PERCENT:-70}"
  "METRICS_INTERVAL_SEC=${METRICS_INTERVAL_SEC:-5}"
)

suite_index="${RESULT_ROOT}/suite-${SUITE_RUN_ID_ROOT}-index.csv"
combined_latency="${RESULT_ROOT}/suite-${SUITE_RUN_ID_ROOT}-latency.csv"
combined_migration="${RESULT_ROOT}/suite-${SUITE_RUN_ID_ROOT}-migration.csv"

mkdir -p "${RESULT_ROOT}"
printf 'profile,run_id_root,fairness_csv,comparison_csv,migration_csv\n' >"${suite_index}"
: >"${combined_latency}"
: >"${combined_migration}"

append_csv_with_profile() {
  local profile="$1"
  local input="$2"
  local output="$3"
  if [[ ! -s "${input}" ]]; then
    echo "ERROR: expected CSV not found: ${input}" >&2
    return 1
  fi
  if [[ ! -s "${output}" ]]; then
    awk -v profile="${profile}" 'NR == 1 { print "profile," $0; next } { print profile "," $0 }' "${input}" >>"${output}"
  else
    awk -v profile="${profile}" 'NR > 1 { print profile "," $0 }' "${input}" >>"${output}"
  fi
}

run_profile() {
  local profile="$1"
  local run_id_root="${SUITE_RUN_ID_ROOT}-${profile}"
  local fairness_file="${RESULT_ROOT}/matrix-${run_id_root}-fairness.txt"
  local comparison_file="${RESULT_ROOT}/matrix-${run_id_root}-comparison.csv"
  local migration_file="${RESULT_ROOT}/matrix-${run_id_root}-migration.csv"

  echo "=== GKE suite profile: ${profile} ==="
  case "${profile}" in
    none)
      env "${common_env[@]}" \
        RUN_ID_ROOT="${run_id_root}" \
        MATRIX_PRESSURE_PROFILE=none \
        MATRIX_PRESSURE_DURATION_SEC=0 \
        MATRIX_PRESSURE_WARMUP_SEC=0 \
        "${SCRIPT_DIR}/run_matrix_gke.sh"
      ;;
    cpu)
      env "${common_env[@]}" \
        RUN_ID_ROOT="${run_id_root}" \
        MATRIX_PRESSURE_PROFILE=cpu \
        MATRIX_PRESSURE_CPUS="${MATRIX_PRESSURE_CPUS:-2}" \
        MATRIX_PRESSURE_DURATION_SEC="${MATRIX_PRESSURE_DURATION_SEC:-60}" \
        MATRIX_PRESSURE_WARMUP_SEC="${MATRIX_PRESSURE_WARMUP_SEC:-10}" \
        "${SCRIPT_DIR}/run_matrix_gke.sh"
      ;;
    io)
      env "${common_env[@]}" \
        RUN_ID_ROOT="${run_id_root}" \
        MATRIX_PRESSURE_PROFILE=io \
        MATRIX_HDD_WORKERS="${MATRIX_HDD_WORKERS:-2}" \
        MATRIX_HDD_BYTES="${MATRIX_HDD_BYTES:-512M}" \
        MATRIX_PRESSURE_DURATION_SEC="${MATRIX_PRESSURE_DURATION_SEC:-60}" \
        MATRIX_PRESSURE_WARMUP_SEC="${MATRIX_PRESSURE_WARMUP_SEC:-10}" \
        "${SCRIPT_DIR}/run_matrix_gke.sh"
      ;;
    *)
      echo "ERROR: unknown GKE suite profile: ${profile}" >&2
      return 1
      ;;
  esac

  printf '%s,%s,%s,%s,%s\n' "${profile}" "${run_id_root}" "${fairness_file}" "${comparison_file}" "${migration_file}" >>"${suite_index}"
  append_csv_with_profile "${profile}" "${comparison_file}" "${combined_latency}"
  append_csv_with_profile "${profile}" "${migration_file}" "${combined_migration}"
}

for profile in ${GKE_SUITE_PROFILES}; do
  run_profile "${profile}"
done

echo "=== GKE suite complete ==="
echo "Suite index: ${suite_index}"
echo "Combined latency CSV: ${combined_latency}"
echo "Combined migration CSV: ${combined_migration}"
echo
echo "=== Combined latency ==="
cat "${combined_latency}"
echo
echo "=== Combined migration ==="
cat "${combined_migration}"
