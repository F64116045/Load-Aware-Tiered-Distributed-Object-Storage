#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

gke_auth_preflight() {
  if [[ "${GKE_AUTH_PREFLIGHT:-true}" != "true" ]]; then
    return 0
  fi
  if ! command -v gcloud >/dev/null 2>&1 || ! command -v kubectl >/dev/null 2>&1; then
    echo "WARN: gcloud/kubectl not found; skip GKE auth preflight" >&2
    return 0
  fi

  echo "=== GKE auth preflight ==="
  echo "If Cloud Shell asks for authorization, click Authorize now before the long experiment starts." >&2
  gcloud auth list --filter=status:ACTIVE --format='value(account)' >/dev/null
  gcloud auth print-access-token >/dev/null
  kubectl get namespace default >/dev/null
}

export KUSTOMIZE_DIR="${KUSTOMIZE_DIR:-deploy/gke/standard}"
export MATRIX_COMPOSE_LABEL="${MATRIX_COMPOSE_LABEL:-gke}"
export K8S_DISCOVER_API_BASE="${K8S_DISCOVER_API_BASE:-true}"
export K8S_API_SERVICE_NAME="${K8S_API_SERVICE_NAME:-api}"
export K8S_API_SERVICE_PORT="${K8S_API_SERVICE_PORT:-8000}"

gke_auth_preflight

exec "${SCRIPT_DIR}/run_matrix_k3s.sh" "$@"
