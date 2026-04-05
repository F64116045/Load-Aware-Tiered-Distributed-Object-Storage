#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml}"
SMOKE_META_BACKEND="${SMOKE_META_BACKEND:-rocksdb}"

dc() {
  local args=()
  local f
  for f in ${COMPOSE_FILES}; do
    args+=(-f "${f}")
  done
  docker compose "${args[@]}" "$@"
}

run_one_backend() {
  local backend="$1"
  local compose_files="$2"
  COMPOSE_FILES="${compose_files}"

  echo "=== Smoke (${backend} metadata path) ==="
  dc down -v >/dev/null 2>&1 || true
  case "${backend}" in
    rocksdb)
      COMPOSE_FILES="${compose_files}" START_STACK=true "${SCRIPT_DIR}/smoke_e2e_v2.sh"
      COMPOSE_FILES="${compose_files}" START_STACK=false "${SCRIPT_DIR}/smoke_leader_failover.sh"
      ;;
    tikv)
      COMPOSE_FILES="${compose_files}" START_STACK=true "${SCRIPT_DIR}/smoke_e2e_v2_tikv.sh"
      COMPOSE_FILES="${compose_files}" START_STACK=false "${SCRIPT_DIR}/smoke_leader_failover_tikv.sh"
      ;;
    *)
      echo "ERROR: unsupported SMOKE_META_BACKEND=${backend}" >&2
      exit 1
      ;;
  esac
  dc down -v >/dev/null 2>&1 || true
}

case "${SMOKE_META_BACKEND}" in
  rocksdb)
    run_one_backend "rocksdb" "${COMPOSE_FILES}"
    ;;
  tikv)
    run_one_backend "tikv" "${COMPOSE_FILES} docker-compose.tikv.yaml"
    ;;
  all)
    run_one_backend "rocksdb" "${COMPOSE_FILES}"
    run_one_backend "tikv" "${COMPOSE_FILES} docker-compose.tikv.yaml"
    ;;
  *)
    echo "ERROR: unsupported SMOKE_META_BACKEND=${SMOKE_META_BACKEND} (use rocksdb|tikv|all)" >&2
    exit 1
    ;;
esac

echo "Smoke matrix passed (SMOKE_META_BACKEND=${SMOKE_META_BACKEND})."
