#!/usr/bin/env bash

repo_root_from_common() {
  local src
  src="${BASH_SOURCE[0]}"
  cd "$(dirname "${src}")/../.." && pwd
}

export REPO_ROOT="${REPO_ROOT:-$(repo_root_from_common)}"
export EXPERIMENTS_DIR="${EXPERIMENTS_DIR:-${REPO_ROOT}/experiments}"
export API_BASE="${API_BASE:-http://127.0.0.1:8000}"
export META_DSN="${META_DSN:-pd:2379}"
export COMPOSE_FILES="${COMPOSE_FILES:-docker-compose.yaml experiments/compose/local-experiment.yaml}"
export SCENARIO="${SCENARIO:-manual}"
export RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
export RESULT_ROOT="${RESULT_ROOT:-${EXPERIMENTS_DIR}/results}"
export RESULT_DIR="${RESULT_DIR:-${RESULT_ROOT}/${SCENARIO}/${RUN_ID}}"
export TIMEOUT_SEC="${TIMEOUT_SEC:-180}"
export MIN_HEALTHY_NODES="${MIN_HEALTHY_NODES:-6}"

CORE_SERVICES_DEFAULT="meta_service storage_node_1 storage_node_2 storage_node_3 storage_node_4 storage_node_5 storage_node_6 api nginx"

detect_docker_bin() {
  if [[ -n "${DOCKER_BIN:-}" ]]; then
    printf '%s\n' "${DOCKER_BIN}"
    return 0
  fi
  local candidate
  for candidate in docker docker.exe; do
    if command -v "${candidate}" >/dev/null 2>&1 && "${candidate}" info >/dev/null 2>&1; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  printf 'docker\n'
}

export DOCKER_BIN="${DOCKER_BIN:-$(detect_docker_bin)}"

exp_log() {
  printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2
}

ensure_result_dir() {
  mkdir -p "${RESULT_DIR}"
  printf '%s\n' "${RESULT_DIR}"
}

compose_args() {
  local args=()
  local f
  for f in ${COMPOSE_FILES}; do
    args+=(-f "${f}")
  done
  printf '%s\n' "${args[@]}"
}

dc() {
  local args=()
  local f
  for f in ${COMPOSE_FILES}; do
    args+=(-f "${f}")
  done
  (cd "${REPO_ROOT}" && "${DOCKER_BIN}" compose "${args[@]}" "$@")
}

check_docker_available() {
  if ! command -v "${DOCKER_BIN}" >/dev/null 2>&1; then
    exp_log "ERROR: Docker CLI not found (${DOCKER_BIN})"
    return 1
  fi
  if ! "${DOCKER_BIN}" info >/dev/null 2>&1; then
    exp_log "ERROR: Docker engine is not reachable from this shell"
    exp_log "HINT: start Docker Desktop and enable WSL integration, or run with DOCKER_BIN=docker.exe if that is the working CLI"
    return 1
  fi
}

reset_stack() {
  check_docker_available
  exp_log "Reset docker compose stack and volumes"
  dc down -v --remove-orphans
}

start_core_stack() {
  check_docker_available
  local services
  local build_flag=()
  if [[ "${BUILD_STACK:-true}" == "true" ]]; then
    build_flag=(--build)
  fi
  services="${CORE_SERVICES:-${CORE_SERVICES_DEFAULT}}"
  exp_log "Start core stack: ${services}"
  dc up -d "${build_flag[@]}" --remove-orphans --force-recreate ${services}
}

start_tiering_worker() {
  check_docker_available
  local build_flag=()
  if [[ "${BUILD_STACK:-true}" == "true" ]]; then
    build_flag=(--build)
  fi
  exp_log "Start tiering worker: variant=${TIERING_POLICY_VARIANT:-A} trigger=${TIERING_TRIGGER_MODE:-periodic}"
  dc up -d --no-deps "${build_flag[@]}" --remove-orphans --force-recreate tiering_worker
}

stop_stack() {
  check_docker_available
  exp_log "Stop docker compose stack"
  dc down --remove-orphans
}

wait_api_health() {
  local deadline=$((SECONDS + TIMEOUT_SEC))
  exp_log "Wait API health at ${API_BASE}/health"
  while (( SECONDS < deadline )); do
    if curl -sS -f "${API_BASE}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  exp_log "ERROR: API health check timeout"
  return 1
}

wait_node_discovery_ready() {
  local min_nodes="${1:-${MIN_HEALTHY_NODES}}"
  local deadline=$((SECONDS + TIMEOUT_SEC))
  exp_log "Wait node discovery: min_nodes=${min_nodes}"
  while (( SECONDS < deadline )); do
    local body count
    body="$(curl -sS -f "${API_BASE}/v2/admin/nodes?limit=1000" || true)"
    count="$(printf '%s' "${body}" | sed -n 's/.*"count":\([0-9]\+\).*/\1/p' | head -n1)"
    if [[ -n "${count}" ]] && (( count >= min_nodes )); then
      return 0
    fi
    sleep 2
  done
  exp_log "ERROR: node discovery did not reach ${min_nodes}"
  return 1
}

require_python3() {
  if ! command -v python3 >/dev/null 2>&1; then
    exp_log "ERROR: python3 is required for experiment CSV summarization"
    return 1
  fi
}

write_run_env() {
  ensure_result_dir >/dev/null
  cat >"${RESULT_DIR}/run.env" <<EOF
SCENARIO=${SCENARIO}
RUN_ID=${RUN_ID}
API_BASE=${API_BASE}
COMPOSE_FILES=${COMPOSE_FILES}
OBJECT_COUNT=${OBJECT_COUNT:-}
OBJECT_SIZE_BYTES=${OBJECT_SIZE_BYTES:-}
PRELOAD_OBJECTS=${PRELOAD_OBJECTS:-}
PRELOAD_AGE_WAIT_SEC=${PRELOAD_AGE_WAIT_SEC:-}
WORKLOAD_DURATION_SEC=${WORKLOAD_DURATION_SEC:-}
WORKLOAD_CONCURRENCY=${WORKLOAD_CONCURRENCY:-}
GET_PERCENT=${GET_PERCENT:-}
PRESSURE_PROFILE=${PRESSURE_PROFILE:-}
PRESSURE_CPUS=${PRESSURE_CPUS:-}
PRESSURE_DURATION_SEC=${PRESSURE_DURATION_SEC:-}
PRESSURE_DELAY_SEC=${PRESSURE_DELAY_SEC:-}
PRESSURE_WARMUP_SEC=${PRESSURE_WARMUP_SEC:-}
HDD_WORKERS=${HDD_WORKERS:-}
HDD_BYTES=${HDD_BYTES:-}
METRICS_INTERVAL_SEC=${METRICS_INTERVAL_SEC:-}
COLLECT_DURATION_SEC=${COLLECT_DURATION_SEC:-}
TIERING_POLICY_VARIANT=${TIERING_POLICY_VARIANT:-}
TIERING_TRIGGER_MODE=${TIERING_TRIGGER_MODE:-}
AGE_THRESHOLD_SEC=${AGE_THRESHOLD_SEC:-}
MAX_OBJECTS_PER_ROUND=${MAX_OBJECTS_PER_ROUND:-}
MAX_BYTES_PER_ROUND=${MAX_BYTES_PER_ROUND:-}
WORKER_BW_LIMIT_MBPS=${WORKER_BW_LIMIT_MBPS:-}
TIERING_IDLE_STABLE_ROUNDS=${TIERING_IDLE_STABLE_ROUNDS:-}
TIERING_IDLE_CPU_PCT=${TIERING_IDLE_CPU_PCT:-}
TIERING_IDLE_MEMORY_PCT=${TIERING_IDLE_MEMORY_PCT:-}
TIERING_IDLE_IOWAIT_PCT=${TIERING_IDLE_IOWAIT_PCT:-}
TIERING_IDLE_QUEUE_DEPTH=${TIERING_IDLE_QUEUE_DEPTH:-}
TIERING_IDLE_MIN_NODE_RATIO=${TIERING_IDLE_MIN_NODE_RATIO:-}
TIERING_IDLE_MIN_NODE_COUNT=${TIERING_IDLE_MIN_NODE_COUNT:-}
EOF
}
