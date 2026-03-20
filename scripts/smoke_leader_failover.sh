#!/usr/bin/env bash

set -euo pipefail

PG_CONTAINER="${PG_CONTAINER:-postgres}"
PG_USER="${PG_USER:-metadata}"
PG_DB="${PG_DB:-metadata}"
LOCK_KEY="${LOCK_KEY:-42042}"
TIMEOUT_SEC="${TIMEOUT_SEC:-120}"
START_STACK="${START_STACK:-true}"

sql() {
  local q="$1"
  docker compose exec -T "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${PG_DB}" -t -A -c "${q}"
}

wait_leader() {
  local deadline=$((SECONDS + TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    local row
    row="$(sql "SELECT leader_id || '|' || scanner_status FROM tiering_leader_state WHERE lock_key=${LOCK_KEY} LIMIT 1;" | tr -d '\r' | tr -d '\n')"
    if [[ -n "${row}" ]]; then
      echo "${row}"
      return 0
    fi
    sleep 2
  done
  return 1
}

echo "[0/6] Prepare metadata schema"
docker compose run --rm meta_migrate >/dev/null

if [[ "${START_STACK}" == "true" ]]; then
  echo "[1/6] Start stack with 2 tiering workers"
  docker compose up -d --build postgres storage_node_1 storage_node_2 storage_node_3 storage_node_4 storage_node_5 storage_node_6 api --scale tiering_worker=2 tiering_worker >/dev/null
else
  echo "[1/6] Skip stack startup (START_STACK=false)"
fi

echo "[2/6] Wait scanner leader appears"
leader_row="$(wait_leader)"
if [[ -z "${leader_row}" ]]; then
  echo "ERROR: scanner leader not found for lock_key=${LOCK_KEY}" >&2
  exit 1
fi
leader_id="${leader_row%%|*}"
leader_status="${leader_row##*|}"
echo "Leader: ${leader_id} status=${leader_status}"

echo "[3/6] Verify only one worker reports LEADING for this lock_key"
leading_count="$(sql "SELECT COUNT(*) FROM tiering_leader_state WHERE lock_key=${LOCK_KEY} AND scanner_status='LEADING';" | tr -d '[:space:]')"
if [[ "${leading_count}" != "1" ]]; then
  echo "ERROR: expected exactly one LEADING row, got ${leading_count}" >&2
  exit 1
fi

echo "[4/6] Stop current leader container: ${leader_id}"
docker stop "${leader_id}" >/dev/null

echo "[5/6] Wait failover leader takeover"
deadline=$((SECONDS + TIMEOUT_SEC))
new_leader=""
while (( SECONDS < deadline )); do
  row="$(sql "SELECT leader_id || '|' || scanner_status FROM tiering_leader_state WHERE lock_key=${LOCK_KEY} LIMIT 1;" | tr -d '\r' | tr -d '\n')"
  if [[ -n "${row}" ]]; then
    candidate_id="${row%%|*}"
    candidate_status="${row##*|}"
    if [[ "${candidate_status}" == "LEADING" ]] && [[ "${candidate_id}" != "${leader_id}" ]]; then
      new_leader="${candidate_id}"
      break
    fi
  fi
  sleep 2
done
if [[ -z "${new_leader}" ]]; then
  echo "ERROR: failover not observed. old_leader=${leader_id}" >&2
  exit 1
fi
echo "New leader: ${new_leader}"

echo "[6/6] Verify leader heartbeat is fresh"
leader_age="$(sql "SELECT EXTRACT(EPOCH FROM (NOW() - last_heartbeat_at))::INT FROM tiering_leader_state WHERE lock_key=${LOCK_KEY} LIMIT 1;" | tr -d '[:space:]')"
if [[ -z "${leader_age}" ]] || (( leader_age > 15 )); then
  echo "ERROR: leader heartbeat stale age=${leader_age}" >&2
  exit 1
fi

echo "Leader failover smoke passed."
echo "old_leader=${leader_id} new_leader=${new_leader}"
