#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

DELAY_SEC="${DELAY_SEC:-0}"
DURATION_SEC="${DURATION_SEC:-60}"
PRESSURE_CPUS="${PRESSURE_CPUS:-2}"
CONTAINER_NAME="${CONTAINER_NAME:-exp-cpu-spike-${RUN_ID}}"

if (( DELAY_SEC > 0 )); then
  exp_log "CPU pressure waits ${DELAY_SEC}s"
  sleep "${DELAY_SEC}"
fi

exp_log "Start CPU pressure: cpus=${PRESSURE_CPUS} duration=${DURATION_SEC}s"
"${DOCKER_BIN}" rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
"${DOCKER_BIN}" run --rm --name "${CONTAINER_NAME}" alpine:3.18 sh -c \
  "apk add --no-cache stress-ng >/dev/null && stress-ng --cpu ${PRESSURE_CPUS} --cpu-method matrixprod --timeout ${DURATION_SEC}s --metrics-brief"
