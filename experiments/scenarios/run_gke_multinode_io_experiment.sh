#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
K8S_NAMESPACE="${K8S_NAMESPACE:-rec-store}"
RESULT_ROOT="${RESULT_ROOT:-${SCRIPT_DIR}/../results}"
TERRAFORM_DIR="${REPO_ROOT}/infra/gcp/gke-experiment"

for bin in kubectl awk find grep sort tee python3; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "ERROR: ${bin} is required" >&2
    exit 1
  fi
done

if [[ "${GKE_MULTI_IO_GET_CREDENTIALS:-true}" == "true" && -d "${TERRAFORM_DIR}" ]] && command -v terraform >/dev/null 2>&1; then
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

choose_storage_only_nodes() {
  local count="$1"
  local rows forbidden_apps storage_nodes blocked_nodes node selected=()
  forbidden_apps="${K8S_PRESSURE_AVOID_APPS:-pd,tikv,meta-service,api}"
  rows="$(kubectl -n "${K8S_NAMESPACE}" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.app}{"\t"}{.spec.nodeName}{"\n"}{end}')"
  if [[ -z "${rows}" ]]; then
    echo "ERROR: no pods found in namespace ${K8S_NAMESPACE}" >&2
    return 1
  fi

  storage_nodes="$(awk -F '\t' '$2 == "storage-node" && $3 != "" { print $3 }' <<<"${rows}" | sort -u)"
  blocked_nodes="$(awk -F '\t' -v apps="${forbidden_apps}" '
    BEGIN {
      n = split(apps, app_list, ",")
      for (i = 1; i <= n; i++) {
        gsub(/^ +| +$/, "", app_list[i])
        blocked_app[app_list[i]] = 1
      }
    }
    $2 in blocked_app && $3 != "" { print $3 }
  ' <<<"${rows}" | sort -u)"

  while IFS= read -r node; do
    [[ -n "${node}" ]] || continue
    if ! grep -Fxq "${node}" <<<"${blocked_nodes}"; then
      selected+=("${node}")
      if ((${#selected[@]} >= count)); then
        (IFS=,; printf '%s\n' "${selected[*]}")
        return 0
      fi
    fi
  done <<<"${storage_nodes}"

  echo "ERROR: need ${count} storage-only pressure nodes, found ${#selected[@]}." >&2
  echo "Pod placement:" >&2
  printf '%s\n' "${rows}" >&2
  return 1
}

PRESSURE_NODE_COUNT="${GKE_MULTI_IO_PRESSURE_NODE_COUNT:-2}"
REPEATS="${GKE_MULTI_IO_REPEATS:-3}"
SELECTOR_PREFIX="${GKE_MULTI_IO_SELECTOR_PREFIX:-gke-multinode-io}"

echo "=== Current pod placement ==="
kubectl -n "${K8S_NAMESPACE}" get pods -o wide

if [[ -z "${K8S_PRESSURE_TARGET_NODES:-}" ]]; then
  K8S_PRESSURE_TARGET_NODES="$(choose_storage_only_nodes "${PRESSURE_NODE_COUNT}")"
  export K8S_PRESSURE_TARGET_NODES
fi

echo
echo "=== Multi-node I/O pressure targets ==="
echo "K8S_PRESSURE_TARGET_NODES=${K8S_PRESSURE_TARGET_NODES}"
echo "This is the main C-aligned pressure model: 2/6 busy nodes should violate the default idle ratio 0.8."
echo

mkdir -p "${RESULT_ROOT}/logs"

for i in $(seq 1 "${REPEATS}"); do
  run_root="$(date -u +%Y%m%dT%H%M%SZ)-${SELECTOR_PREFIX}-r${i}"
  log_file="${RESULT_ROOT}/logs/${run_root}.log"
  echo "=== Multi-node I/O repeat ${i}/${REPEATS}: ${run_root} ==="
  SUITE_RUN_ID_ROOT="${run_root}" \
  GKE_SUITE_PROFILES="io" \
  K8S_PRESSURE_TARGET_NODES="${K8S_PRESSURE_TARGET_NODES}" \
  K8S_PRESSURE_TARGET_NODE="" \
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
echo "=== Multi-node I/O repeat summary ==="
python3 "${REPO_ROOT}/experiments/collect/summarize_gke_suite_repeats.py" \
  --result-root "${RESULT_ROOT}" \
  --selector "${SELECTOR_PREFIX}-r" \
  --latest "${REPEATS}"
