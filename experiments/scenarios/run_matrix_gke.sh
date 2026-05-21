#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export KUSTOMIZE_DIR="${KUSTOMIZE_DIR:-deploy/gke/standard}"
export MATRIX_COMPOSE_LABEL="${MATRIX_COMPOSE_LABEL:-gke}"

exec "${SCRIPT_DIR}/run_matrix_k3s.sh" "$@"
