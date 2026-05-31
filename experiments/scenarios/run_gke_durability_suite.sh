#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_ROOT="${RESULT_ROOT:-${SCRIPT_DIR}/../results}"
DURABILITY_SUITE_RUN_ID_ROOT="${DURABILITY_SUITE_RUN_ID_ROOT:-$(date -u +%Y%m%dT%H%M%SZ)-gke-durability}"
DURABILITY_MODES="${DURABILITY_MODES:-sync write}"

if [[ -z "${IMAGE:-}" ]]; then
  echo "ERROR: IMAGE is required, for example IMAGE=asia-east1-docker.pkg.dev/<project>/<repo>/rec-store:gke-exp-001" >&2
  exit 1
fi

suite_index="${RESULT_ROOT}/durability-${DURABILITY_SUITE_RUN_ID_ROOT}-index.csv"
combined_latency="${RESULT_ROOT}/durability-${DURABILITY_SUITE_RUN_ID_ROOT}-latency.csv"
combined_migration="${RESULT_ROOT}/durability-${DURABILITY_SUITE_RUN_ID_ROOT}-migration.csv"
combined_phase_latency="${RESULT_ROOT}/durability-${DURABILITY_SUITE_RUN_ID_ROOT}-phase-latency.csv"
combined_phase_bottlenecks="${RESULT_ROOT}/durability-${DURABILITY_SUITE_RUN_ID_ROOT}-phase-bottlenecks.csv"

mkdir -p "${RESULT_ROOT}"
printf 'durability_mode,suite_run_id_root,index_csv,latency_csv,migration_csv,phase_latency_csv,phase_bottleneck_csv\n' >"${suite_index}"
: >"${combined_latency}"
: >"${combined_migration}"
: >"${combined_phase_latency}"
: >"${combined_phase_bottlenecks}"

append_csv_with_mode() {
  local mode="$1"
  local input="$2"
  local output="$3"
  if [[ ! -s "${input}" ]]; then
    echo "ERROR: expected CSV not found: ${input}" >&2
    return 1
  fi
  if [[ ! -s "${output}" ]]; then
    awk -v mode="${mode}" 'NR == 1 { print "durability_mode," $0; next } { print mode "," $0 }' "${input}" >>"${output}"
  else
    awk -v mode="${mode}" 'NR > 1 { print mode "," $0 }' "${input}" >>"${output}"
  fi
}

run_mode() {
  local mode="$1"
  case "${mode}" in
    sync|group_sync|write)
      ;;
    *)
      echo "ERROR: unsupported STORAGE_DURABILITY_MODE=${mode}; expected sync, group_sync, or write" >&2
      return 1
      ;;
  esac

  local suite_run_id_root="${DURABILITY_SUITE_RUN_ID_ROOT}-${mode}"
  local index_file="${RESULT_ROOT}/suite-${suite_run_id_root}-index.csv"
  local latency_file="${RESULT_ROOT}/suite-${suite_run_id_root}-latency.csv"
  local migration_file="${RESULT_ROOT}/suite-${suite_run_id_root}-migration.csv"
  local phase_file="${RESULT_ROOT}/suite-${suite_run_id_root}-phase-latency.csv"
  local bottleneck_file="${RESULT_ROOT}/suite-${suite_run_id_root}-phase-bottlenecks.csv"

  echo "=== GKE durability suite: mode=${mode} ==="
  env \
    "IMAGE=${IMAGE}" \
    "RESULT_ROOT=${RESULT_ROOT}" \
    "SUITE_RUN_ID_ROOT=${suite_run_id_root}" \
    "STORAGE_DURABILITY_MODE=${mode}" \
    "${SCRIPT_DIR}/run_gke_experiment_suite.sh"

  printf '%s,%s,%s,%s,%s,%s,%s\n' "${mode}" "${suite_run_id_root}" "${index_file}" "${latency_file}" "${migration_file}" "${phase_file}" "${bottleneck_file}" >>"${suite_index}"
  append_csv_with_mode "${mode}" "${latency_file}" "${combined_latency}"
  append_csv_with_mode "${mode}" "${migration_file}" "${combined_migration}"
  append_csv_with_mode "${mode}" "${phase_file}" "${combined_phase_latency}"
  append_csv_with_mode "${mode}" "${bottleneck_file}" "${combined_phase_bottlenecks}"
}

for mode in ${DURABILITY_MODES}; do
  run_mode "${mode}"
done

echo "=== GKE durability suite complete ==="
echo "Durability index: ${suite_index}"
echo "Combined latency CSV: ${combined_latency}"
echo "Combined migration CSV: ${combined_migration}"
echo "Combined phase latency CSV: ${combined_phase_latency}"
echo "Combined phase bottleneck CSV: ${combined_phase_bottlenecks}"
echo
echo "=== Combined latency by durability mode ==="
cat "${combined_latency}"
echo
echo "=== Combined phase latency by durability mode ==="
cat "${combined_phase_latency}"
echo
echo "=== Combined PUT phase bottlenecks by durability mode ==="
cat "${combined_phase_bottlenecks}"
