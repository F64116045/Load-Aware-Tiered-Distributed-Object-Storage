#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export KUSTOMIZE_DIR="${KUSTOMIZE_DIR:-deploy/gke/standard}"
export MATRIX_COMPOSE_LABEL="${MATRIX_COMPOSE_LABEL:-gke}"
export K8S_DISCOVER_API_BASE="${K8S_DISCOVER_API_BASE:-true}"
export K8S_API_SERVICE_NAME="${K8S_API_SERVICE_NAME:-api}"
export K8S_API_SERVICE_PORT="${K8S_API_SERVICE_PORT:-8000}"

exec "${SCRIPT_DIR}/run_matrix_k3s.sh" "$@"
