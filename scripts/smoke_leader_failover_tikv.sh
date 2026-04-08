#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export META_DSN="${META_DSN:-pd:2379}"
export COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml}"

exec "${SCRIPT_DIR}/smoke_leader_failover.sh" "$@"
