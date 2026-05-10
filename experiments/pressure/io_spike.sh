#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

DELAY_SEC="${DELAY_SEC:-0}"
DURATION_SEC="${DURATION_SEC:-60}"
HDD_WORKERS="${HDD_WORKERS:-2}"
HDD_BYTES="${HDD_BYTES:-512M}"
CONTAINER_NAME="${CONTAINER_NAME:-exp-io-spike-${RUN_ID}}"
PRESSURE_DIR="${PRESSURE_DIR:-${REPO_ROOT}/.experiment-pressure}"

mkdir -p "${PRESSURE_DIR}"

if (( DELAY_SEC > 0 )); then
  exp_log "I/O pressure waits ${DELAY_SEC}s"
  sleep "${DELAY_SEC}"
fi

exp_log "Start I/O pressure: hdd_workers=${HDD_WORKERS} hdd_bytes=${HDD_BYTES} duration=${DURATION_SEC}s"
"${DOCKER_BIN}" rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
"${DOCKER_BIN}" run --rm --name "${CONTAINER_NAME}" -v "${PRESSURE_DIR}:/pressure" alpine:3.18 sh -c \
  "apk add --no-cache stress-ng >/dev/null && stress-ng --hdd ${HDD_WORKERS} --hdd-bytes ${HDD_BYTES} --temp-path /pressure --timeout ${DURATION_SEC}s --metrics-brief"
