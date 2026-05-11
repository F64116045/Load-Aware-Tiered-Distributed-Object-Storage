#!/usr/bin/env bash

set -euo pipefail

export SCENARIO="${SCENARIO:-strategy_a_age_based}"
export TIERING_POLICY_VARIANT="${TIERING_POLICY_VARIANT:-A}"
export TIERING_TRIGGER_MODE="${TIERING_TRIGGER_MODE:-periodic}"
export TIERING_POLICY_PERIOD_SEC="${TIERING_POLICY_PERIOD_SEC:-5}"
export AGE_THRESHOLD_SEC="${AGE_THRESHOLD_SEC:-0}"
export MAX_OBJECTS_PER_ROUND="${MAX_OBJECTS_PER_ROUND:-200}"
export WORKER_BW_LIMIT_MBPS="${WORKER_BW_LIMIT_MBPS:-0}"
export PRESSURE_PROFILE="${PRESSURE_PROFILE:-none}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/_run_local_scenario.sh" "$@"
