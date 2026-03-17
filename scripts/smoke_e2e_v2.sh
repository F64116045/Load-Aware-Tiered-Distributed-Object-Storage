#!/usr/bin/env bash

set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8000}"
PG_CONTAINER="${PG_CONTAINER:-postgres}"
PG_USER="${PG_USER:-metadata}"
PG_DB="${PG_DB:-metadata}"
TIMEOUT_SEC="${TIMEOUT_SEC:-120}"
START_STACK="${START_STACK:-true}"
FORCE_TASK_NOW="${FORCE_TASK_NOW:-true}"

KEY="v2-smoke-$(date +%s)"
PAYLOAD_FILE="$(mktemp)"
READ_FILE="$(mktemp)"
PAYLOAD="v2-tiering-smoke-$(date +%s)-payload"

cleanup() {
  rm -f "${PAYLOAD_FILE}" "${READ_FILE}"
}
trap cleanup EXIT

sql() {
  local q="$1"
  docker compose exec -T "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -t -A -c "${q}"
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

echo "[0/8] Prepare metadata schema"
docker compose run --rm meta_migrate >/dev/null

if [[ "${START_STACK}" == "true" ]]; then
  echo "[1/8] Start stack"
  docker compose up -d --build postgres storage_node_1 storage_node_2 storage_node_3 storage_node_4 storage_node_5 storage_node_6 api tiering_worker nginx >/dev/null
else
  echo "[1/8] Skip stack startup (START_STACK=false)"
fi

echo "[2/8] Wait API health"
if ! wait_api_health; then
  echo "ERROR: API health check timeout at ${API_BASE}/health" >&2
  exit 1
fi

printf "%s" "${PAYLOAD}" >"${PAYLOAD_FILE}"

echo "[3/8] PUT /v2/objects/${KEY}"
curl -sS -f -X PUT \
  "${API_BASE}/v2/objects/${KEY}" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @"${PAYLOAD_FILE}" >/dev/null

echo "[4/8] Verify tiering task exists"
TASK_ID="$(sql "SELECT task_id FROM tiering_tasks WHERE object_id='${KEY}' ORDER BY scheduled_at DESC LIMIT 1;" | tr -d '[:space:]')"
if [[ -z "${TASK_ID}" ]]; then
  echo "ERROR: tiering task not found for object ${KEY}" >&2
  exit 1
fi
echo "Task: ${TASK_ID}"

echo "[5/8] Verify admin tasks endpoint sees object"
tasks_resp="$(curl -sS -f "${API_BASE}/v2/admin/tasks?task_type=REPL_TO_EC&limit=200")"
if ! printf "%s" "${tasks_resp}" | grep -q "\"object_id\":\"${KEY}\""; then
  echo "ERROR: /v2/admin/tasks does not contain object ${KEY}" >&2
  exit 1
fi

if [[ "${FORCE_TASK_NOW}" == "true" ]]; then
  echo "[6/8] Force task runnable now"
  sql "UPDATE tiering_tasks SET scheduled_at=NOW()-INTERVAL '1 second' WHERE task_id='${TASK_ID}';" >/dev/null
else
  echo "[6/8] Skip force scheduling (FORCE_TASK_NOW=false)"
fi

echo "[7/8] Wait object promotion to EC_ACTIVE"
deadline=$((SECONDS + TIMEOUT_SEC))
while (( SECONDS < deadline )); do
  obj_resp="$(curl -sS -f "${API_BASE}/v2/admin/objects/${KEY}" || true)"
  task_state="$(sql "SELECT task_state FROM tiering_tasks WHERE task_id='${TASK_ID}';" | tr -d '[:space:]')"

  if printf "%s" "${obj_resp}" | grep -q "\"state\":\"EC_ACTIVE\"" &&
    printf "%s" "${obj_resp}" | grep -q "\"tier\":\"EC\"" &&
    [[ "${task_state}" == "DONE" ]]; then
    echo "Promotion done: state=EC_ACTIVE tier=EC task_state=${task_state}"
    break
  fi
  sleep 2
done

obj_resp="$(curl -sS -f "${API_BASE}/v2/admin/objects/${KEY}")"
task_state="$(sql "SELECT task_state FROM tiering_tasks WHERE task_id='${TASK_ID}';" | tr -d '[:space:]')"
if ! printf "%s" "${obj_resp}" | grep -q "\"state\":\"EC_ACTIVE\"" ||
  ! printf "%s" "${obj_resp}" | grep -q "\"tier\":\"EC\"" ||
  [[ "${task_state}" != "DONE" ]]; then
  echo "ERROR: promotion timeout/failure. task_state=${task_state} obj=${obj_resp}" >&2
  exit 1
fi

echo "[8/8] GET /v2/objects/${KEY} and verify payload"
curl -sS -f "${API_BASE}/v2/objects/${KEY}" -o "${READ_FILE}"
if ! cmp -s "${PAYLOAD_FILE}" "${READ_FILE}"; then
  echo "ERROR: payload mismatch after readback" >&2
  echo "expected: $(cat "${PAYLOAD_FILE}")" >&2
  echo "actual  : $(cat "${READ_FILE}")" >&2
  exit 1
fi

echo "Smoke passed: v2 put -> task enqueue -> admin visibility -> EC promotion -> v2 get"
echo "key=${KEY}"
