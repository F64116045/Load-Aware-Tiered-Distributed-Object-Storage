#!/usr/bin/env bash

set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8000}"
TIMEOUT_SEC="${TIMEOUT_SEC:-120}"
START_STACK="${START_STACK:-true}"
COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml}"
RUN_META_MIGRATE="${RUN_META_MIGRATE:-auto}" # auto|true|false

dc() {
  local args=()
  local f
  for f in ${COMPOSE_FILES}; do
    args+=(-f "${f}")
  done
  docker compose "${args[@]}" "$@"
}

auto_should_migrate() {
  if [[ "${COMPOSE_FILES}" == *"rocks"* ]]; then
    return 1
  fi
  return 0
}

leader_json() {
  curl -sS -f "${API_BASE}/v2/admin/leader"
}

extract_leader_id() {
  local body="$1"
  printf "%s" "${body}" | grep -o '"leader_id":"[^"]*"' | head -n1 | cut -d'"' -f4
}

extract_scanner_status() {
  local body="$1"
  printf "%s" "${body}" | grep -o '"scanner_status":"[^"]*"' | head -n1 | cut -d'"' -f4
}

extract_heartbeat_age_sec() {
  local body="$1"
  printf "%s" "${body}" | sed -n 's/.*"last_heartbeat_ago_sec":\([0-9]\+\).*/\1/p' | head -n1
}

wait_leader() {
  local deadline=$((SECONDS + TIMEOUT_SEC))
  while (( SECONDS < deadline )); do
    local body
    body="$(leader_json || true)"
    local leader_id
    local scanner_status
    leader_id="$(extract_leader_id "${body}")"
    scanner_status="$(extract_scanner_status "${body}")"
    if [[ -n "${leader_id}" && "${scanner_status}" == "LEADING" ]]; then
      printf "%s|%s\n" "${leader_id}" "${scanner_status}"
      return 0
    fi
    sleep 2
  done
  return 1
}

if [[ "${RUN_META_MIGRATE}" == "true" ]] || ([[ "${RUN_META_MIGRATE}" == "auto" ]] && auto_should_migrate); then
  echo "[0/6] Prepare metadata schema"
  dc run --rm meta_migrate >/dev/null
else
  echo "[0/6] Skip metadata migrate"
fi

if [[ "${START_STACK}" == "true" ]]; then
  echo "[1/6] Start stack with 2 tiering workers"
  dc up -d --build meta_service storage_node_1 storage_node_2 storage_node_3 storage_node_4 storage_node_5 storage_node_6 api --scale tiering_worker=2 tiering_worker >/dev/null
else
  echo "[1/6] Skip stack startup (START_STACK=false)"
fi

echo "[2/6] Wait scanner leader appears"
leader_row="$(wait_leader)"
if [[ -z "${leader_row}" ]]; then
  echo "ERROR: scanner leader not found" >&2
  exit 1
fi
leader_id="${leader_row%%|*}"
leader_status="${leader_row##*|}"
echo "Leader: ${leader_id} status=${leader_status}"

echo "[3/6] Verify leader endpoint reports LEADING"
body="$(leader_json)"
status="$(extract_scanner_status "${body}")"
if [[ "${status}" != "LEADING" ]]; then
  echo "ERROR: expected LEADING status, got ${status}" >&2
  exit 1
fi

echo "[4/6] Stop current leader container: ${leader_id}"
docker stop "${leader_id}" >/dev/null

echo "[5/6] Wait failover leader takeover"
deadline=$((SECONDS + TIMEOUT_SEC))
new_leader=""
while (( SECONDS < deadline )); do
  body="$(leader_json || true)"
  candidate_id="$(extract_leader_id "${body}")"
  candidate_status="$(extract_scanner_status "${body}")"
  if [[ -n "${candidate_id}" && "${candidate_status}" == "LEADING" && "${candidate_id}" != "${leader_id}" ]]; then
    new_leader="${candidate_id}"
    break
  fi
  sleep 2
done
if [[ -z "${new_leader}" ]]; then
  echo "ERROR: failover not observed. old_leader=${leader_id}" >&2
  exit 1
fi
echo "New leader: ${new_leader}"

echo "[6/6] Verify leader heartbeat is fresh"
body="$(leader_json)"
leader_age="$(extract_heartbeat_age_sec "${body}")"
if [[ -z "${leader_age}" ]] || (( leader_age > 15 )); then
  echo "ERROR: leader heartbeat stale age=${leader_age}" >&2
  exit 1
fi

echo "Leader failover smoke passed."
echo "old_leader=${leader_id} new_leader=${new_leader}"
