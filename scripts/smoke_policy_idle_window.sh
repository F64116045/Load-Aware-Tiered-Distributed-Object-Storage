#!/usr/bin/env bash

set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8000}"
TIMEOUT_SEC="${TIMEOUT_SEC:-90}"
START_STACK="${START_STACK:-true}"
COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml docker-compose.policy-idle.yaml}"

KEY="idle-smoke-$(date +%s)"
PAYLOAD_FILE="$(mktemp)"
PAYLOAD="idle-window-smoke-$(date +%s)-payload"

cleanup() {
  rm -f "${PAYLOAD_FILE}"
}
trap cleanup EXIT

dc() {
  local args=()
  local f
  for f in ${COMPOSE_FILES}; do
    args+=(-f "${f}")
  done
  docker compose "${args[@]}" "$@"
}

wait_api_health() {
  local deadline=$((SECONDS + TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    if curl -sS -f "${API_BASE}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_node_discovery_ready() {
  local deadline=$((SECONDS + TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    local body
    body="$(curl -sS -f "${API_BASE}/v2/admin/nodes?limit=10" || true)"
    local count
    count="$(printf "%s" "${body}" | sed -n 's/.*"count":\([0-9]\+\).*/\1/p' | head -n1)"
    if [[ -n "${count}" ]] && (( count >= 3 )); then
      return 0
    fi
    sleep 2
  done
  return 1
}

find_repl2ec_task_id() {
  local object_id="$1"
  local resp
  resp="$(curl -sS -f "${API_BASE}/v2/admin/tasks?task_type=REPL_TO_EC&object_id=${object_id}&limit=1000")"
  printf "%s" "${resp}" | grep -o '"task_id":"[^"]*"' | head -n1 | cut -d'"' -f4 || true
}

echo "[1/7] Start stack (policy idle-window mode)"
if [[ "${START_STACK}" == "true" ]]; then
  dc up -d --build --remove-orphans --force-recreate meta_service storage_node_1 storage_node_2 storage_node_3 storage_node_4 storage_node_5 storage_node_6 api tiering_worker nginx >/dev/null
else
  echo "skip stack startup (START_STACK=false)"
fi

echo "[2/7] Wait API health"
if ! wait_api_health; then
  echo "ERROR: API health check timeout at ${API_BASE}/health" >&2
  exit 1
fi

echo "[3/7] Wait node discovery ready"
if ! wait_node_discovery_ready; then
  echo "ERROR: node discovery did not reach quorum in time" >&2
  exit 1
fi

printf "%s" "${PAYLOAD}" >"${PAYLOAD_FILE}"
echo "[4/7] PUT /v2/objects/${KEY}"
curl -sS -f -X PUT \
  "${API_BASE}/v2/objects/${KEY}" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @"${PAYLOAD_FILE}" >/dev/null

echo "[5/7] Observe whether scanner is already warm"
TASK_ID="$(find_repl2ec_task_id "${KEY}")"
if [[ -n "${TASK_ID}" ]]; then
  echo "scanner already warm: task enqueued immediately (${TASK_ID})"
fi

echo "[6/7] Wait scanner to enqueue after idle stable rounds"
deadline=$((SECONDS + TIMEOUT_SEC))
while (( SECONDS < deadline )); do
  TASK_ID="$(find_repl2ec_task_id "${KEY}")"
  if [[ -n "${TASK_ID}" ]]; then
    break
  fi
  sleep 1
done
if [[ -z "${TASK_ID}" ]]; then
  echo "ERROR: REPL_TO_EC task was not enqueued by idle-window scanner" >&2
  exit 1
fi

echo "[7/7] Idle-window policy smoke passed"
echo "task=${TASK_ID}"
echo "key=${KEY}"
