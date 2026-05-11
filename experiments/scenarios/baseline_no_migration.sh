#!/usr/bin/env bash

set -euo pipefail

export SCENARIO="${SCENARIO:-baseline_no_migration}"
export AGE_THRESHOLD_SEC="${AGE_THRESHOLD_SEC:-86400}"
export START_TIERING_WORKER="${START_TIERING_WORKER:-false}"
export PRESSURE_PROFILE="${PRESSURE_PROFILE:-none}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/_run_local_scenario.sh" "$@"
