#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_ROOT="${RESULT_ROOT:-${SCRIPT_DIR}/../results}"
SUITE_RUN_ID_ROOT="${SUITE_RUN_ID_ROOT:-$(date -u +%Y%m%dT%H%M%SZ)-gke-suite}"
GKE_SUITE_PROFILES="${GKE_SUITE_PROFILES:-none cpu io}"
GKE_SUITE_PRINT_FULL="${GKE_SUITE_PRINT_FULL:-false}"

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
  "K8S_IN_CLUSTER_WORKLOAD=${K8S_IN_CLUSTER_WORKLOAD:-true}"
  "K8S_WORKLOAD_API_BASE=${K8S_WORKLOAD_API_BASE:-}"
  "K8S_WORKLOAD_IMAGE=${K8S_WORKLOAD_IMAGE:-alpine:3.18}"
  "K8S_WORKLOAD_INSTALL_CMD=${K8S_WORKLOAD_INSTALL_CMD:-}"
  "K8S_WORKLOAD_NODE_SELECTOR_KEY=${K8S_WORKLOAD_NODE_SELECTOR_KEY:-rec-store-role}"
  "K8S_WORKLOAD_NODE_SELECTOR_VALUE=${K8S_WORKLOAD_NODE_SELECTOR_VALUE:-system}"
  "K8S_STORAGE_NODE_SELECTOR_KEY=${K8S_STORAGE_NODE_SELECTOR_KEY:-rec-store-role}"
  "K8S_STORAGE_NODE_SELECTOR_VALUE=${K8S_STORAGE_NODE_SELECTOR_VALUE:-storage}"
  "K8S_REQUIRE_STORAGE_PLACEMENT=${K8S_REQUIRE_STORAGE_PLACEMENT:-true}"
)

suite_index="${RESULT_ROOT}/suite-${SUITE_RUN_ID_ROOT}-index.csv"
combined_latency="${RESULT_ROOT}/suite-${SUITE_RUN_ID_ROOT}-latency.csv"
combined_migration="${RESULT_ROOT}/suite-${SUITE_RUN_ID_ROOT}-migration.csv"
combined_phase_latency="${RESULT_ROOT}/suite-${SUITE_RUN_ID_ROOT}-phase-latency.csv"
combined_phase_bottlenecks="${RESULT_ROOT}/suite-${SUITE_RUN_ID_ROOT}-phase-bottlenecks.csv"

mkdir -p "${RESULT_ROOT}"
printf 'profile,run_id_root,fairness_csv,comparison_csv,migration_csv,phase_latency_csv,phase_bottleneck_csv\n' >"${suite_index}"
: >"${combined_latency}"
: >"${combined_migration}"
: >"${combined_phase_latency}"
: >"${combined_phase_bottlenecks}"

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
  local phase_file="${RESULT_ROOT}/matrix-${run_id_root}-phase-latency.csv"
  local bottleneck_file="${RESULT_ROOT}/matrix-${run_id_root}-phase-bottlenecks.csv"

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

  printf '%s,%s,%s,%s,%s,%s,%s\n' "${profile}" "${run_id_root}" "${fairness_file}" "${comparison_file}" "${migration_file}" "${phase_file}" "${bottleneck_file}" >>"${suite_index}"
  append_csv_with_profile "${profile}" "${comparison_file}" "${combined_latency}"
  append_csv_with_profile "${profile}" "${migration_file}" "${combined_migration}"
  append_csv_with_profile "${profile}" "${phase_file}" "${combined_phase_latency}"
  append_csv_with_profile "${profile}" "${bottleneck_file}" "${combined_phase_bottlenecks}"
}

for profile in ${GKE_SUITE_PROFILES}; do
  run_profile "${profile}"
done

echo "=== GKE suite complete ==="
echo "Suite index: ${suite_index}"
echo "Combined latency CSV: ${combined_latency}"
echo "Combined migration CSV: ${combined_migration}"
echo "Combined phase latency CSV: ${combined_phase_latency}"
echo "Combined phase bottleneck CSV: ${combined_phase_bottlenecks}"

if [[ "${GKE_SUITE_PRINT_FULL}" == "true" ]]; then
  echo
  echo "=== Combined latency ==="
  cat "${combined_latency}"
  echo
  echo "=== Combined migration ==="
  cat "${combined_migration}"
  echo
  echo "=== Combined phase latency ==="
  cat "${combined_phase_latency}"
  echo
  echo "=== Combined PUT phase bottlenecks ==="
  cat "${combined_phase_bottlenecks}"
else
  echo
  echo "=== Combined latency preview ==="
  awk -F, 'NR == 1 || $4 == "GET" || $4 == "PUT" { print }' "${combined_latency}"
  echo
  echo "=== Combined migration preview ==="
  cat "${combined_migration}"
  echo
  echo "Set GKE_SUITE_PRINT_FULL=true to print full phase latency and bottleneck CSVs."
fi
