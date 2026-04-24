#!/usr/bin/env bash

set -euo pipefail

COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml}"
STARTUP_TIMEOUT_SEC="${STARTUP_TIMEOUT_SEC:-300}"
API_BASE="${API_BASE:-http://127.0.0.1:8000}"

dc() {
  local args=()
  local f
  for f in ${COMPOSE_FILES}; do
    args+=(-f "${f}")
  done
  docker compose "${args[@]}" "$@"
}

wait_tikv_ready() {
  local deadline=$((SECONDS + STARTUP_TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    # Prefer probing TiKV's status endpoint; startup log wording is not stable across TiKV versions.
    if dc exec -T tikv wget -q -O /dev/null http://127.0.0.1:20180/status >/dev/null 2>&1; then
      return 0
    fi
    # Keep legacy log-match fallback for older images.
    if dc logs --no-color --tail 200 tikv 2>&1 | grep -q "TiKV is ready to serve"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_meta_health() {
  local svc="$1"
  local deadline=$((SECONDS + STARTUP_TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    if dc exec -T "${svc}" wget -q -O /dev/null http://127.0.0.1:8091/health >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_api_health() {
  local deadline=$((SECONDS + STARTUP_TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    if curl -sS -f "${API_BASE}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

echo "[1/6] Start TiKV core (PD + TiKV)"
dc up -d --build --remove-orphans pd tikv >/dev/null

echo "[2/6] Wait TiKV readiness"
if ! wait_tikv_ready; then
  echo "ERROR: TiKV did not become ready in ${STARTUP_TIMEOUT_SEC}s" >&2
  exit 1
fi

echo "[3/6] Start metadata services"
dc up -d --build meta_service_1 meta_service_2 meta_service_3 meta_service >/dev/null

echo "[4/6] Wait metadata service liveness"
for svc in meta_service_1 meta_service_2 meta_service_3; do
  if ! wait_meta_health "${svc}"; then
    echo "ERROR: ${svc} did not become healthy in ${STARTUP_TIMEOUT_SEC}s" >&2
    exit 1
  fi
done

echo "[5/6] Start data and API services"
dc up -d --build nginx storage_node_1 storage_node_2 storage_node_3 storage_node_4 storage_node_5 storage_node_6 api tiering_worker >/dev/null

echo "[6/6] Wait API health"
if ! wait_api_health; then
  echo "ERROR: API did not become healthy at ${API_BASE}/health" >&2
  exit 1
fi

echo "Stack is up."
echo "API: ${API_BASE}"
