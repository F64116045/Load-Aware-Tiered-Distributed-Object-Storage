#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
K8S_NAMESPACE="${K8S_NAMESPACE:-rec-store}"
RESULT_ROOT="${RESULT_ROOT:-${SCRIPT_DIR}/../results}"

for bin in kubectl awk find grep sort; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "ERROR: ${bin} is required" >&2
    exit 1
  fi
done

TERRAFORM_DIR="${REPO_ROOT}/infra/gcp/gke-experiment"

if [[ "${GKE_FIXED_IO_GET_CREDENTIALS:-true}" == "true" && -d "${TERRAFORM_DIR}" ]] && command -v terraform >/dev/null 2>&1; then
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

choose_storage_only_node() {
  local rows forbidden_apps storage_nodes blocked_nodes node app
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
      printf '%s\n' "${node}"
      return 0
    fi
  done <<<"${storage_nodes}"

  echo "ERROR: no storage-only node found." >&2
  echo "Pod placement:" >&2
  printf '%s\n' "${rows}" >&2
  return 1
}

echo "=== Current pod placement ==="
kubectl -n "${K8S_NAMESPACE}" get pods -o wide

if [[ -z "${K8S_PRESSURE_TARGET_NODE:-}" ]]; then
  K8S_PRESSURE_TARGET_NODE="$(choose_storage_only_node)"
  export K8S_PRESSURE_TARGET_NODE
fi

echo
echo "=== Fixed I/O pressure target ==="
echo "K8S_PRESSURE_TARGET_NODE=${K8S_PRESSURE_TARGET_NODE}"
echo

SUITE_RUN_ID_ROOT="${SUITE_RUN_ID_ROOT:-$(date -u +%Y%m%dT%H%M%SZ)-gke-fixed-io-sanity}"
export SUITE_RUN_ID_ROOT
export GKE_SUITE_PROFILES="${GKE_SUITE_PROFILES:-io}"
export AGE_THRESHOLD_SEC="${AGE_THRESHOLD_SEC:-5}"
export PRELOAD_AGE_WAIT_SEC="${PRELOAD_AGE_WAIT_SEC:-10}"
export OBJECT_COUNT="${OBJECT_COUNT:-10}"
export OBJECT_SIZE_BYTES="${OBJECT_SIZE_BYTES:-1048576}"
export WORKLOAD_DURATION_SEC="${WORKLOAD_DURATION_SEC:-20}"
export WORKLOAD_CONCURRENCY="${WORKLOAD_CONCURRENCY:-2}"
export GET_PERCENT="${GET_PERCENT:-70}"
export MATRIX_PRESSURE_DURATION_SEC="${MATRIX_PRESSURE_DURATION_SEC:-20}"
export MATRIX_PRESSURE_WARMUP_SEC="${MATRIX_PRESSURE_WARMUP_SEC:-5}"
export K8S_IN_CLUSTER_WORKLOAD="${K8S_IN_CLUSTER_WORKLOAD:-true}"

"${SCRIPT_DIR}/run_gke_experiment_suite.sh"

echo
echo "=== Fixed pressure placement check ==="
placement_files=()
while IFS= read -r file; do
  placement_files+=("${file}")
done < <(find "${RESULT_ROOT}" -path "*${SUITE_RUN_ID_ROOT}*k8s-pressure*placement.txt" | sort)

if ((${#placement_files[@]} == 0)); then
  echo "ERROR: no pressure placement files found for ${SUITE_RUN_ID_ROOT}" >&2
  exit 1
fi

placement_ok=true
for file in "${placement_files[@]}"; do
  echo "--- ${file} ---"
  cat "${file}"
  actual_node="$(awk -F '=' '$1 == "node" { print $2; exit }' "${file}")"
  target_node="$(awk -F '=' '$1 == "target_node" { print $2; exit }' "${file}")"
  if [[ "${target_node}" != "${K8S_PRESSURE_TARGET_NODE}" || "${actual_node}" != "${K8S_PRESSURE_TARGET_NODE}" ]]; then
    placement_ok=false
  fi
done

if [[ "${placement_ok}" != "true" ]]; then
  echo "ERROR: pressure placement did not match K8S_PRESSURE_TARGET_NODE=${K8S_PRESSURE_TARGET_NODE}" >&2
  exit 1
fi

echo
echo "PASS: all pressure jobs were pinned to ${K8S_PRESSURE_TARGET_NODE}"
echo "Run root: ${SUITE_RUN_ID_ROOT}"
