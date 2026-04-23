#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export META_DSN="${META_DSN:-pd:2379,pd2:2379,pd3:2379}"
export COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml docker-compose.ha.yaml docker-compose.smoke.yaml}"

exec "${SCRIPT_DIR}/smoke_e2e_v2.sh" "$@"
