#!/usr/bin/env bash

set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8000}"
TIMEOUT_SEC="${TIMEOUT_SEC:-120}"
START_STACK="${START_STACK:-true}"
FORCE_TASK_NOW="${FORCE_TASK_NOW:-true}"
COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml docker-compose.smoke.yaml}"
TASK_SCAN_LIMIT="${TASK_SCAN_LIMIT:-200000}"

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
  resp="$(curl -sS -f "${API_BASE}/v2/admin/tasks?task_type=${task_type}&object_id=${object_id}&limit=${TASK_SCAN_LIMIT}")"
  printf "%s" "${resp}" | grep -o '"task_id":"[^"]*"' | head -n1 | cut -d'"' -f4 || true
}

find_task_state_by_object() {
  local task_type="$1"
  local object_id="$2"
  local resp
  resp="$(curl -sS -f "${API_BASE}/v2/admin/tasks?task_type=${task_type}&object_id=${object_id}&limit=${TASK_SCAN_LIMIT}")"
  printf "%s" "${resp}" | grep -o '"task_state":"[^"]*"' | head -n1 | cut -d'"' -f4 || true
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

echo "[5/9] Try to discover REPL_TO_EC task (best effort)"
TASK_ID=""
deadline=$((SECONDS + 15))
while (( SECONDS < deadline )); do
  TASK_ID="$(find_task_id_by_object "REPL_TO_EC" "${KEY}")"
  if [[ -n "${TASK_ID}" ]]; then
    break
  fi
  sleep 2
done
if [[ -n "${TASK_ID}" ]]; then
  echo "Task: ${TASK_ID}"
else
  echo "Task not found in admin window yet; continue with state-based validation"
fi

if [[ "${FORCE_TASK_NOW}" == "true" && -n "${TASK_ID}" ]]; then
  echo "[6/9] Force task runnable now via admin retry-now"
  retry_http_code="$(
    curl -sS -o /dev/null -w "%{http_code}" -X POST \
      "${API_BASE}/v2/admin/tasks/${TASK_ID}/retry-now" || true
  )"
  if [[ "${retry_http_code}" == "200" ]]; then
    echo "retry-now accepted (task requeued immediately)"
  elif [[ "${retry_http_code}" == "404" ]]; then
    echo "retry-now skipped: task already non-requeueable (likely claimed/running/done)"
  elif [[ -n "${retry_http_code}" ]]; then
    echo "retry-now returned HTTP ${retry_http_code}; continue with state-based validation"
  fi
else
  echo "[6/9] Skip force scheduling (task id unavailable or FORCE_TASK_NOW=false)"
fi

echo "[7/9] Wait object promotion to EC_ACTIVE"
deadline=$((SECONDS + TIMEOUT_SEC))
while (( SECONDS < deadline )); do
  obj_resp="$(curl -sS -f "${API_BASE}/v2/admin/objects/${KEY}" || true)"

  if printf "%s" "${obj_resp}" | grep -q "\"state\":\"EC_ACTIVE\"" &&
    printf "%s" "${obj_resp}" | grep -q "\"tier\":\"EC\""; then
    task_state="$(find_task_state_by_object "REPL_TO_EC" "${KEY}")"
    if [[ -n "${task_state}" ]]; then
      echo "Promotion done: state=EC_ACTIVE tier=EC task_state=${task_state}"
    else
      echo "Promotion done: state=EC_ACTIVE tier=EC"
    fi
    break
  fi
  sleep 2
done

obj_resp="$(curl -sS -f "${API_BASE}/v2/admin/objects/${KEY}")"
if ! printf "%s" "${obj_resp}" | grep -q "\"state\":\"EC_ACTIVE\"" ||
  ! printf "%s" "${obj_resp}" | grep -q "\"tier\":\"EC\""; then
  task_state="$(find_task_state_by_object "REPL_TO_EC" "${KEY}")"
  if [[ -z "${task_state}" ]]; then
    echo "HINT: REPL_TO_EC task not observed for this object within timeout." >&2
    echo "HINT: If stack was started by scripts/up_stack.sh (without smoke override)," >&2
    echo "HINT: AGE_THRESHOLD_SEC may still be 3600 and scanner period 300s." >&2
    echo "HINT: Run START_STACK=true ./scripts/smoke_e2e_v2_tikv.sh once, then retry with START_STACK=false." >&2
  fi
  echo "ERROR: promotion timeout/failure. task_state=${task_state} obj=${obj_resp}" >&2
  exit 1
fi

echo "[8/9] GET /v2/objects/${KEY} and verify payload"
curl -sS -f "${API_BASE}/v2/objects/${KEY}" -o "${READ_FILE}"
if ! cmp -s "${PAYLOAD_FILE}" "${READ_FILE}"; then
  echo "ERROR: payload mismatch after readback" >&2
  echo "expected: $(cat "${PAYLOAD_FILE}")" >&2
  echo "actual  : $(cat "${READ_FILE}")" >&2
  exit 1
fi

echo "[9/9] Verify GC task done and replica locations marked DELETED"
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

echo "Smoke passed: v2 put -> scanner/worker promotion -> GC cleanup -> v2 get"
echo "key=${KEY}"
