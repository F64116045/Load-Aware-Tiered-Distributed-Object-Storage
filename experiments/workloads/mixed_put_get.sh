#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

ensure_result_dir >/dev/null

DURATION_SEC="${DURATION_SEC:-120}"
CONCURRENCY="${CONCURRENCY:-8}"
GET_PERCENT="${GET_PERCENT:-70}"
PRELOAD_COUNT="${PRELOAD_COUNT:-${OBJECT_COUNT:-200}}"
PUT_SIZE_BYTES="${PUT_SIZE_BYTES:-${OBJECT_SIZE_BYTES:-1048576}}"
KEY_PREFIX="${KEY_PREFIX:-exp-${RUN_ID}}"
RESULT_FILE="${RESULT_FILE:-${RESULT_DIR}/latency.csv}"
TMP_DIR="$(mktemp -d)"
PAYLOAD_FILE="$(mktemp)"

cleanup() {
  rm -rf "${TMP_DIR}"
  rm -f "${PAYLOAD_FILE}"
}
trap cleanup EXIT

head -c "${PUT_SIZE_BYTES}" /dev/urandom >"${PAYLOAD_FILE}"

printf 'timestamp_unix_ms,scenario,run_id,worker,operation,object_id,http_code,latency_ms,bytes\n' >"${RESULT_FILE}"

worker_loop() {
  local worker_id="$1"
  local out_file="${TMP_DIR}/worker-${worker_id}.csv"
  local end_ts=$(( $(date +%s) + DURATION_SEC ))
  local seq_no=0
  local op object_id curl_out http_code time_total latency_ms ts_ms bytes

  : >"${out_file}"
  while (( $(date +%s) < end_ts )); do
    seq_no=$((seq_no + 1))
    if (( PRELOAD_COUNT > 0 && (RANDOM % 100) < GET_PERCENT )); then
      op="GET"
      object_id="$(printf '%s-preload-%04d' "${KEY_PREFIX}" "$(( (RANDOM % PRELOAD_COUNT) + 1 ))")"
      curl_out="$(
        curl -sS -o /dev/null -w '%{http_code} %{time_total}' \
          "${API_BASE}/v2/objects/${object_id}" || true
      )"
      bytes=0
    else
      op="PUT"
      object_id="$(printf '%s-live-%02d-%06d-%s' "${KEY_PREFIX}" "${worker_id}" "${seq_no}" "$(date +%s%N)")"
      curl_out="$(
        curl -sS -o /dev/null -w '%{http_code} %{time_total}' \
          -X PUT "${API_BASE}/v2/objects/${object_id}" \
          -H 'Content-Type: application/octet-stream' \
          --data-binary @"${PAYLOAD_FILE}" || true
      )"
      bytes="${PUT_SIZE_BYTES}"
    fi

    http_code="${curl_out%% *}"
    time_total="${curl_out##* }"
    latency_ms="$(awk -v t="${time_total}" 'BEGIN { printf "%.3f", t * 1000 }')"
    ts_ms="$(date +%s%3N)"
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "${ts_ms}" "${SCENARIO}" "${RUN_ID}" "${worker_id}" "${op}" "${object_id}" "${http_code}" "${latency_ms}" "${bytes}" >>"${out_file}"
  done
}

exp_log "Run mixed workload: duration=${DURATION_SEC}s concurrency=${CONCURRENCY} get_percent=${GET_PERCENT}"

pids=()
for worker_id in $(seq 1 "${CONCURRENCY}"); do
  worker_loop "${worker_id}" &
  pids+=("$!")
done

for pid in "${pids[@]}"; do
  wait "${pid}"
done

cat "${TMP_DIR}"/worker-*.csv >>"${RESULT_FILE}"
exp_log "Latency CSV: ${RESULT_FILE}"
