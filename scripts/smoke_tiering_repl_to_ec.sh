#!/usr/bin/env bash

set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8000}"
TIMEOUT_SEC="${TIMEOUT_SEC:-90}"

KEY="tiering-smoke-$(date +%s)"
PAYLOAD='{"hello":"tiering","n":1}'

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

echo "[1/6] Write replication object: ${KEY}"
curl -sS -f -X POST \
  "${API_BASE}/write?key=${KEY}&strategy=replication" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}" >/dev/null

echo "[2/6] Verify tiering task enqueued"
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

echo "[3/6] Force task runnable now"
curl -sS -f -X POST "${API_BASE}/v2/admin/tasks/${TASK_ID}/retry-now" >/dev/null

echo "[4/6] Wait worker to promote object to EC_ACTIVE"
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

obj_resp="$(curl -sS -f "${API_BASE}/v2/admin/objects/${KEY}" || true)"
task_state="$(find_task_state_by_object "REPL_TO_EC" "${KEY}")"
if ! printf "%s" "${obj_resp}" | grep -q "\"state\":\"EC_ACTIVE\"" ||
  ! printf "%s" "${obj_resp}" | grep -q "\"tier\":\"EC\"" ||
  [[ "${task_state}" != "DONE" ]]; then
  echo "ERROR: promotion timeout/failure. task_state=${task_state} obj=${obj_resp}" >&2
  exit 1
fi

echo "[5/6] Read object back through API"
read_resp="$(curl -sS -f "${API_BASE}/read/${KEY}")"
if ! echo "${read_resp}" | grep -q '"hello":"tiering"'; then
  echo "ERROR: read payload validation failed: ${read_resp}" >&2
  exit 1
fi

echo "[6/6] Smoke passed: write -> enqueue -> EC promotion -> read"
echo "key=${KEY}"
