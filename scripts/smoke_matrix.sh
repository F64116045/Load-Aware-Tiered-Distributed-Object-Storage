#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND="${BACKEND:-both}" # postgres|rocks|both

dc() {
  local files="$1"
  shift
  local args=()
  local f
  for f in ${files}; do
    args+=(-f "${f}")
  done
  docker compose "${args[@]}" "$@"
}

run_postgres() {
  local files="docker-compose.yaml"
  echo "=== Smoke (postgres) ==="
  dc "${files}" down -v >/dev/null 2>&1 || true
  COMPOSE_FILES="${files}" RUN_META_MIGRATE=true START_STACK=true "${SCRIPT_DIR}/smoke_e2e_v2.sh"
  COMPOSE_FILES="${files}" RUN_META_MIGRATE=false START_STACK=false "${SCRIPT_DIR}/smoke_leader_failover.sh"
  dc "${files}" down -v >/dev/null 2>&1 || true
}

run_rocks() {
  local files="docker-compose.yaml docker-compose.rocks.yaml"
  echo "=== Smoke (rocks) ==="
  dc "${files}" down -v >/dev/null 2>&1 || true
  COMPOSE_FILES="${files}" RUN_META_MIGRATE=false START_STACK=true "${SCRIPT_DIR}/smoke_e2e_v2.sh"
  COMPOSE_FILES="${files}" RUN_META_MIGRATE=false START_STACK=false "${SCRIPT_DIR}/smoke_leader_failover.sh"
  dc "${files}" down -v >/dev/null 2>&1 || true
}

case "${BACKEND}" in
  postgres)
    run_postgres
    ;;
  rocks)
    run_rocks
    ;;
  both)
    run_postgres
    run_rocks
    ;;
  *)
    echo "ERROR: unsupported BACKEND=${BACKEND} (use postgres|rocks|both)" >&2
    exit 1
    ;;
esac

echo "Smoke matrix passed (BACKEND=${BACKEND})."
