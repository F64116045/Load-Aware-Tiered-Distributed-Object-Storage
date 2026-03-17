#!/usr/bin/env bash

set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8000}"
PG_CONTAINER="${PG_CONTAINER:-postgres}"
PG_USER="${PG_USER:-metadata}"
PG_DB="${PG_DB:-metadata}"
TIMEOUT_SEC="${TIMEOUT_SEC:-90}"

KEY="tiering-smoke-$(date +%s)"
PAYLOAD='{"hello":"tiering","n":1}'

sql() {
  local q="$1"
  docker compose exec -T "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -t -A -c "${q}"
}

echo "[1/6] Write replication object: ${KEY}"
curl -sS -f -X POST \
  "${API_BASE}/write?key=${KEY}&strategy=replication" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}" >/dev/null

echo "[2/6] Verify tiering task enqueued"
TASK_ID="$(sql "SELECT task_id FROM tiering_tasks WHERE object_id='${KEY}' ORDER BY scheduled_at DESC LIMIT 1;" | tr -d '[:space:]')"
if [[ -z "${TASK_ID}" ]]; then
  echo "ERROR: tiering task not found for object ${KEY}" >&2
  exit 1
fi
echo "Task: ${TASK_ID}"

echo "[3/6] Force task runnable now (bypass AGE_THRESHOLD_SEC wait)"
sql "UPDATE tiering_tasks SET scheduled_at=NOW()-INTERVAL '1 second' WHERE task_id='${TASK_ID}';" >/dev/null

echo "[4/6] Wait worker to promote object to EC_ACTIVE"
deadline=$((SECONDS + TIMEOUT_SEC))
while (( SECONDS < deadline )); do
  state="$(sql "SELECT state FROM objects WHERE object_id='${KEY}';" | tr -d '[:space:]')"
  tier="$(sql "SELECT tier FROM object_versions WHERE object_id='${KEY}' ORDER BY version DESC LIMIT 1;" | tr -d '[:space:]')"
  task_state="$(sql "SELECT task_state FROM tiering_tasks WHERE task_id='${TASK_ID}';" | tr -d '[:space:]')"
  if [[ "${state}" == "EC_ACTIVE" && "${tier}" == "EC" && "${task_state}" == "DONE" ]]; then
    echo "Promotion done: state=${state}, tier=${tier}, task_state=${task_state}"
    break
  fi
  sleep 2
done

state="$(sql "SELECT state FROM objects WHERE object_id='${KEY}';" | tr -d '[:space:]')"
tier="$(sql "SELECT tier FROM object_versions WHERE object_id='${KEY}' ORDER BY version DESC LIMIT 1;" | tr -d '[:space:]')"
task_state="$(sql "SELECT task_state FROM tiering_tasks WHERE task_id='${TASK_ID}';" | tr -d '[:space:]')"
if [[ "${state}" != "EC_ACTIVE" || "${tier}" != "EC" || "${task_state}" != "DONE" ]]; then
  echo "ERROR: promotion timeout/failure. state=${state}, tier=${tier}, task_state=${task_state}" >&2
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
