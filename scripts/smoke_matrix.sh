#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml}"

dc() {
  local args=()
  local f
  for f in ${COMPOSE_FILES}; do
    args+=(-f "${f}")
  done
  docker compose "${args[@]}" "$@"
}

echo "=== Smoke (TiKV metadata path) ==="
dc down -v >/dev/null 2>&1 || true
COMPOSE_FILES="${COMPOSE_FILES}" START_STACK=true "${SCRIPT_DIR}/smoke_e2e_v2.sh"
COMPOSE_FILES="${COMPOSE_FILES}" START_STACK=false "${SCRIPT_DIR}/smoke_leader_failover.sh"
dc down -v >/dev/null 2>&1 || true

echo "Smoke matrix passed (TiKV-only)."

