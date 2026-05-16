#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

ensure_result_dir >/dev/null

OBJECT_COUNT="${OBJECT_COUNT:-200}"
OBJECT_SIZE_BYTES="${OBJECT_SIZE_BYTES:-1048576}"
KEY_PREFIX="${KEY_PREFIX:-exp-${RUN_ID}}"
MANIFEST_FILE="${MANIFEST_FILE:-${RESULT_DIR}/objects.csv}"
PAYLOAD_FILE="$(mktemp)"

cleanup() {
  rm -f "${PAYLOAD_FILE}"
}
trap cleanup EXIT

head -c "${OBJECT_SIZE_BYTES}" /dev/urandom >"${PAYLOAD_FILE}"

printf 'object_id,size_bytes,http_code,latency_ms\n' >"${MANIFEST_FILE}"
exp_log "Preload objects: count=${OBJECT_COUNT} size=${OBJECT_SIZE_BYTES} prefix=${KEY_PREFIX}"

for i in $(seq 1 "${OBJECT_COUNT}"); do
  object_id="$(printf '%s-preload-%04d' "${KEY_PREFIX}" "${i}")"
  curl_out="$(
    curl -sS -o /dev/null -w '%{http_code} %{time_total}' \
      -X PUT "${API_BASE}/v2/objects/${object_id}" \
      -H 'Content-Type: application/octet-stream' \
      --data-binary @"${PAYLOAD_FILE}" || true
  )"
  http_code="${curl_out%% *}"
  time_total="${curl_out##* }"
  latency_ms="$(awk -v t="${time_total}" 'BEGIN { printf "%.3f", t * 1000 }')"
  printf '%s,%s,%s,%s\n' "${object_id}" "${OBJECT_SIZE_BYTES}" "${http_code}" "${latency_ms}" >>"${MANIFEST_FILE}"

  if [[ "${http_code}" != 2* ]]; then
    exp_log "ERROR: preload failed object=${object_id} http=${http_code}"
    exit 1
  fi
done

exp_log "Preload manifest: ${MANIFEST_FILE}"
