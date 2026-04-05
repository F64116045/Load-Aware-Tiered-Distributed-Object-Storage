#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml docker-compose.rocks.yaml}"
export RUN_META_MIGRATE="${RUN_META_MIGRATE:-false}"

exec "${SCRIPT_DIR}/smoke_e2e_v2.sh" "$@"
