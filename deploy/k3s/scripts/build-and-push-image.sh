#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

IMAGE="${1:-${IMAGE:-}}"
PUSH="${PUSH:-true}"

if [[ -z "${IMAGE}" ]]; then
  echo "usage: IMAGE=<registry>/<repo>:<tag> $0" >&2
  exit 1
fi

docker build -t "${IMAGE}" "${REPO_ROOT}"

if [[ "${PUSH}" == "true" ]]; then
  docker push "${IMAGE}"
fi
