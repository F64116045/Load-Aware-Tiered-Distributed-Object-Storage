#!/usr/bin/env bash

set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8000}"
TIMEOUT_SEC="${TIMEOUT_SEC:-120}"
START_STACK="${START_STACK:-true}"
FORCE_TASK_NOW="${FORCE_TASK_NOW:-true}"
COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml}"

KEY="v2-smoke-$(date +%s)"
PAYLOAD_FILE="$(mktemp)"
READ_FILE="$(mktemp)"
PAYLOAD="v2-tiering-smoke-$(date +%s)-payload"

cleanup() {
  rm -f "${PAYLOAD_FILE}" "${READ_FILE}"
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

find_task_id_by_object() {
  local task_type="$1"
  local object_id="$2"
  local resp
  resp="$(curl -sS -f "${API_BASE}/v2/admin/tasks?task_type=${task_type}&object_id=${object_id}&limit=1000")"
  printf "%s" "${resp}" | grep -o '"task_id":"[^"]*"' | head -n1 | cut -d'"' -f4
}

find_task_state_by_object() {
  local task_type="$1"
  local object_id="$2"
  local resp
  resp="$(curl -sS -f "${API_BASE}/v2/admin/tasks?task_type=${task_type}&object_id=${object_id}&limit=1000")"
  printf "%s" "${resp}" | grep -o '"task_state":"[^"]*"' | head -n1 | cut -d'"' -f4
}

echo "[0/9] Metadata schema migration is not required (TiKV keyspace model)"

if [[ "${START_STACK}" == "true" ]]; then
  echo "[1/9] Start stack"
  dc up -d --build --remove-orphans --force-recreate meta_service storage_node_1 storage_node_2 storage_node_3 storage_node_4 storage_node_5 storage_node_6 api tiering_worker nginx >/dev/null
else
  echo "[1/9] Skip stack startup (START_STACK=false)"
fi

echo "[2/9] Wait API health"
if ! wait_api_health; then
  echo "ERROR: API health check timeout at ${API_BASE}/health" >&2
  exit 1
fi

echo "[3/9] Wait node discovery ready"
if ! wait_node_discovery_ready; then
  echo "ERROR: node discovery did not reach quorum in time" >&2
  exit 1
fi

printf "%s" "${PAYLOAD}" >"${PAYLOAD_FILE}"

echo "[4/9] PUT /v2/objects/${KEY}"
curl -sS -f -X PUT \
  "${API_BASE}/v2/objects/${KEY}" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @"${PAYLOAD_FILE}" >/dev/null

echo "[5/9] Verify tiering task exists"
TASK_ID=""
deadline=$((SECONDS + TIMEOUT_SEC))
while (( SECONDS < deadline )); do
  TASK_ID="$(find_task_id_by_object "REPL_TO_EC" "${KEY}")"
  if [[ -n "${TASK_ID}" ]]; then
    break
  fi
  sleep 2
done
if [[ -z "${TASK_ID}" ]]; then
  echo "ERROR: tiering task not found for object ${KEY}" >&2
  exit 1
fi
echo "Task: ${TASK_ID}"

echo "[6/9] Verify admin tasks endpoint sees object"
tasks_resp="$(curl -sS -f "${API_BASE}/v2/admin/tasks?task_type=REPL_TO_EC&object_id=${KEY}&limit=1000")"
if ! printf "%s" "${tasks_resp}" | grep -q "\"object_id\":\"${KEY}\""; then
  echo "ERROR: /v2/admin/tasks does not contain object ${KEY}" >&2
  exit 1
fi

if [[ "${FORCE_TASK_NOW}" == "true" ]]; then
  echo "[7/9] Force task runnable now via admin retry-now"
  curl -sS -f -X POST "${API_BASE}/v2/admin/tasks/${TASK_ID}/retry-now" >/dev/null
else
  echo "[7/9] Skip force scheduling (FORCE_TASK_NOW=false)"
fi

echo "[8/9] Wait object promotion to EC_ACTIVE"
deadline=$((SECONDS + TIMEOUT_SEC))
while (( SECONDS < deadline )); do
  obj_resp="$(curl -sS -f "${API_BASE}/v2/admin/objects/${KEY}" || true)"
  task_state="$(find_task_state_by_object "REPL_TO_EC" "${KEY}")"

  if printf "%s" "${obj_resp}" | grep -q "\"state\":\"EC_ACTIVE\"" &&
    printf "%s" "${obj_resp}" | grep -q "\"tier\":\"EC\"" &&
    [[ "${task_state}" == "DONE" ]]; then
    echo "Promotion done: state=EC_ACTIVE tier=EC task_state=${task_state}"
    break
  fi
  sleep 2
done

obj_resp="$(curl -sS -f "${API_BASE}/v2/admin/objects/${KEY}")"
task_state="$(find_task_state_by_object "REPL_TO_EC" "${KEY}")"
if ! printf "%s" "${obj_resp}" | grep -q "\"state\":\"EC_ACTIVE\"" ||
  ! printf "%s" "${obj_resp}" | grep -q "\"tier\":\"EC\"" ||
  [[ "${task_state}" != "DONE" ]]; then
  echo "ERROR: promotion timeout/failure. task_state=${task_state} obj=${obj_resp}" >&2
  exit 1
fi

echo "[9/9] GET /v2/objects/${KEY} and verify payload"
curl -sS -f "${API_BASE}/v2/objects/${KEY}" -o "${READ_FILE}"
if ! cmp -s "${PAYLOAD_FILE}" "${READ_FILE}"; then
  echo "ERROR: payload mismatch after readback" >&2
  echo "expected: $(cat "${PAYLOAD_FILE}")" >&2
  echo "actual  : $(cat "${READ_FILE}")" >&2
  exit 1
fi

echo "[10/9] Verify GC task done and replica locations marked DELETED"
deadline=$((SECONDS + TIMEOUT_SEC))
while (( SECONDS < deadline )); do
  gc_task_state="$(find_task_state_by_object "GC" "${KEY}")"
  obj_resp="$(curl -sS -f "${API_BASE}/v2/admin/objects/${KEY}" || true)"
  if [[ "${gc_task_state}" == "DONE" ]] && printf "%s" "${obj_resp}" | grep -q "\"status\":\"DELETED\""; then
    echo "GC done: task_state=${gc_task_state}"
    break
  fi
  sleep 2
done

gc_task_state="$(find_task_state_by_object "GC" "${KEY}")"
obj_resp="$(curl -sS -f "${API_BASE}/v2/admin/objects/${KEY}" || true)"
if [[ "${gc_task_state}" != "DONE" ]] || ! printf "%s" "${obj_resp}" | grep -q "\"status\":\"DELETED\""; then
  echo "ERROR: GC verification failed. gc_task_state=${gc_task_state}" >&2
  exit 1
fi

echo "Smoke passed: v2 put -> task enqueue -> EC promotion -> GC cleanup -> v2 get"
echo "key=${KEY}"
