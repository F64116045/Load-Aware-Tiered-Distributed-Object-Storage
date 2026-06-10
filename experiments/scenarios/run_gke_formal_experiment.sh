#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RESULT_ROOT="${RESULT_ROOT:-${SCRIPT_DIR}/../results}"
TERRAFORM_DIR="${REPO_ROOT}/infra/gcp/gke-experiment"

for bin in kubectl tee python3; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "ERROR: ${bin} is required" >&2
    exit 1
  fi
done

if [[ "${GKE_FORMAL_GET_CREDENTIALS:-true}" == "true" && -d "${TERRAFORM_DIR}" ]] && command -v terraform >/dev/null 2>&1; then
  get_credentials_cmd="$(terraform -chdir="${TERRAFORM_DIR}" output -raw get_credentials_command 2>/dev/null || true)"
  if [[ -n "${get_credentials_cmd}" ]]; then
    echo "=== Configure kubectl credentials from Terraform output ==="
    eval "${get_credentials_cmd}"
  fi
fi

if [[ -z "${IMAGE:-}" && -d "${TERRAFORM_DIR}" ]] && command -v terraform >/dev/null 2>&1; then
  IMAGE="$(terraform -chdir="${TERRAFORM_DIR}" output -raw image_example 2>/dev/null || true)"
  export IMAGE
fi

if [[ -z "${IMAGE:-}" ]]; then
  echo "ERROR: IMAGE is required; export IMAGE or run from a Terraform-created checkout" >&2
  exit 1
fi

REPEATS="${GKE_FORMAL_REPEATS:-3}"
PROFILES="${GKE_FORMAL_PROFILES:-none cpu io}"
SELECTOR_PREFIX="${GKE_FORMAL_SELECTOR_PREFIX:-gke-formal}"
PRESSURE_NODE_COUNT="${GKE_FORMAL_PRESSURE_NODE_COUNT:-2}"

mkdir -p "${RESULT_ROOT}/logs"

echo "=== Formal GKE experiment ==="
echo "profiles=${PROFILES}"
echo "repeats=${REPEATS}"
echo "pressure_target_node_count=${PRESSURE_NODE_COUNT}"
echo "workload=50 objects, 1 MiB, 60s, concurrency=2, GET=70%"
echo "pressure=cpu/io profiles target storage-only nodes selected after each namespace reset"
echo

for profile in ${PROFILES}; do
  for i in $(seq 1 "${REPEATS}"); do
    run_root="$(date -u +%Y%m%dT%H%M%SZ)-${SELECTOR_PREFIX}-${profile}-r${i}"
    log_file="${RESULT_ROOT}/logs/${run_root}.log"
    echo "=== Formal GKE ${profile} repeat ${i}/${REPEATS}: ${run_root} ==="
    SUITE_RUN_ID_ROOT="${run_root}" \
    GKE_SUITE_PROFILES="${profile}" \
    K8S_PRESSURE_TARGET_NODE_COUNT="${K8S_PRESSURE_TARGET_NODE_COUNT:-${PRESSURE_NODE_COUNT}}" \
    IMAGE="${IMAGE}" \
    AGE_THRESHOLD_SEC="${AGE_THRESHOLD_SEC:-60}" \
    PRELOAD_AGE_WAIT_SEC="${PRELOAD_AGE_WAIT_SEC:-90}" \
    OBJECT_COUNT="${OBJECT_COUNT:-50}" \
    OBJECT_SIZE_BYTES="${OBJECT_SIZE_BYTES:-1048576}" \
    WORKLOAD_DURATION_SEC="${WORKLOAD_DURATION_SEC:-60}" \
    WORKLOAD_CONCURRENCY="${WORKLOAD_CONCURRENCY:-2}" \
    GET_PERCENT="${GET_PERCENT:-70}" \
    MATRIX_PRESSURE_DURATION_SEC="${MATRIX_PRESSURE_DURATION_SEC:-60}" \
    MATRIX_PRESSURE_WARMUP_SEC="${MATRIX_PRESSURE_WARMUP_SEC:-10}" \
    "${SCRIPT_DIR}/run_gke_experiment_suite.sh" 2>&1 | tee "${log_file}"
  done

  echo
  echo "=== Summary for ${profile} over latest ${REPEATS} repeats ==="
  python3 "${REPO_ROOT}/experiments/collect/summarize_gke_suite_repeats.py" \
    --result-root "${RESULT_ROOT}" \
    --selector "${SELECTOR_PREFIX}-${profile}-r" \
    --latest "${REPEATS}"
  echo
done

echo "=== Formal GKE experiment complete ==="
