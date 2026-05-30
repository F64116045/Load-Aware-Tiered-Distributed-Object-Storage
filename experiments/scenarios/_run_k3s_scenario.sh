#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

require_python3
ensure_result_dir >/dev/null

K8S_NAMESPACE="${K8S_NAMESPACE:-rec-store}"
K8S_DEPLOY_SCRIPT="${K8S_DEPLOY_SCRIPT:-${REPO_ROOT}/deploy/k3s/scripts/deploy.sh}"
K8S_WAIT_SCRIPT="${K8S_WAIT_SCRIPT:-${REPO_ROOT}/deploy/k3s/scripts/wait-ready.sh}"
RESET_STACK="${RESET_STACK:-true}"
START_TIERING_WORKER="${START_TIERING_WORKER:-true}"
PRELOAD_OBJECTS="${PRELOAD_OBJECTS:-true}"
PRELOAD_AGE_WAIT_SEC="${PRELOAD_AGE_WAIT_SEC:-0}"
OBJECT_COUNT="${OBJECT_COUNT:-200}"
OBJECT_SIZE_BYTES="${OBJECT_SIZE_BYTES:-1048576}"
KEY_PREFIX="${KEY_PREFIX:-exp-${RUN_ID}}"
WORKLOAD_DURATION_SEC="${WORKLOAD_DURATION_SEC:-120}"
WORKLOAD_CONCURRENCY="${WORKLOAD_CONCURRENCY:-8}"
GET_PERCENT="${GET_PERCENT:-70}"
PRESSURE_PROFILE="${PRESSURE_PROFILE:-none}"
PRESSURE_DELAY_SEC="${PRESSURE_DELAY_SEC:-0}"
PRESSURE_DURATION_SEC="${PRESSURE_DURATION_SEC:-0}"
PRESSURE_WARMUP_SEC="${PRESSURE_WARMUP_SEC:-0}"
PRESSURE_CPUS="${PRESSURE_CPUS:-2}"
HDD_WORKERS="${HDD_WORKERS:-2}"
HDD_BYTES="${HDD_BYTES:-512M}"
METRICS_INTERVAL_SEC="${METRICS_INTERVAL_SEC:-5}"
COLLECT_DURATION_SEC="${COLLECT_DURATION_SEC:-$((WORKLOAD_DURATION_SEC + PRESSURE_DURATION_SEC + PRESSURE_DELAY_SEC + 30))}"
SUMMARY_FILE="${SUMMARY_FILE:-${RESULT_DIR}/summary.csv}"
TIERING_WORKER_REPLICAS="${TIERING_WORKER_REPLICAS:-1}"
K8S_DISCOVER_API_BASE="${K8S_DISCOVER_API_BASE:-false}"
K8S_API_SERVICE_NAME="${K8S_API_SERVICE_NAME:-api}"
K8S_API_SERVICE_PORT="${K8S_API_SERVICE_PORT:-8000}"

write_k8s_run_env() {
  write_run_env
  cat >>"${RESULT_DIR}/run.env" <<EOF
K8S_NAMESPACE=${K8S_NAMESPACE}
KUSTOMIZE_DIR=${KUSTOMIZE_DIR:-}
IMAGE=${IMAGE:-}
TIERING_WORKER_REPLICAS=${TIERING_WORKER_REPLICAS}
K8S_DISCOVER_API_BASE=${K8S_DISCOVER_API_BASE}
K8S_API_SERVICE_NAME=${K8S_API_SERVICE_NAME}
K8S_API_SERVICE_PORT=${K8S_API_SERVICE_PORT}
EOF
}

deploy_k8s_stack() {
  if [[ "${RESET_STACK}" == "true" ]]; then
    if [[ -z "${IMAGE:-}" ]]; then
      exp_log "ERROR: IMAGE is required when RESET_STACK=true"
      return 1
    fi
    exp_log "Reset and deploy k3s stack: namespace=${K8S_NAMESPACE}"
    NAMESPACE="${K8S_NAMESPACE}" RESET_NAMESPACE=true IMAGE="${IMAGE}" "${K8S_DEPLOY_SCRIPT}"
  else
    exp_log "Wait existing k3s stack: namespace=${K8S_NAMESPACE}"
    NAMESPACE="${K8S_NAMESPACE}" "${K8S_WAIT_SCRIPT}"
  fi
}

configure_k8s_experiment_env() {
  local changed_deployments=false
  local changed_storage=false

  if [[ -n "${AGE_THRESHOLD_SEC:-}" ]]; then
    exp_log "Configure k8s experiment env: AGE_THRESHOLD_SEC=${AGE_THRESHOLD_SEC}"
    kubectl -n "${K8S_NAMESPACE}" set env deployment/meta-service deployment/api \
      AGE_THRESHOLD_SEC="${AGE_THRESHOLD_SEC}" >/dev/null
    kubectl -n "${K8S_NAMESPACE}" set env deployment/tiering-worker \
      AGE_THRESHOLD_SEC="${AGE_THRESHOLD_SEC}" >/dev/null
    changed_deployments=true
  fi

  if [[ -n "${STORAGE_DURABILITY_MODE:-}" ]]; then
    exp_log "Configure storage durability mode: STORAGE_DURABILITY_MODE=${STORAGE_DURABILITY_MODE}"
    kubectl -n "${K8S_NAMESPACE}" set env statefulset/storage-node \
      STORAGE_DURABILITY_MODE="${STORAGE_DURABILITY_MODE}" >/dev/null
    changed_storage=true
  fi

  if [[ "${changed_deployments}" == "true" ]]; then
    kubectl -n "${K8S_NAMESPACE}" rollout status deployment/meta-service --timeout=180s
    kubectl -n "${K8S_NAMESPACE}" rollout status deployment/api --timeout=180s
  fi
  if [[ "${changed_storage}" == "true" ]]; then
    kubectl -n "${K8S_NAMESPACE}" rollout status statefulset/storage-node --timeout=300s
  fi
}

discover_k8s_api_base() {
  if [[ "${K8S_DISCOVER_API_BASE}" != "true" ]]; then
    return 0
  fi

  local deadline=$((SECONDS + TIMEOUT_SEC))
  local endpoint=""
  exp_log "Discover Kubernetes API endpoint: service=${K8S_API_SERVICE_NAME} port=${K8S_API_SERVICE_PORT}"
  while (( SECONDS < deadline )); do
    endpoint="$(kubectl -n "${K8S_NAMESPACE}" get service "${K8S_API_SERVICE_NAME}" \
      -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
    if [[ -z "${endpoint}" ]]; then
      endpoint="$(kubectl -n "${K8S_NAMESPACE}" get service "${K8S_API_SERVICE_NAME}" \
        -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)"
    fi
    if [[ -n "${endpoint}" ]]; then
      export API_BASE="http://${endpoint}:${K8S_API_SERVICE_PORT}"
      exp_log "Use discovered API_BASE=${API_BASE}"
      return 0
    fi
    sleep 2
  done

  exp_log "ERROR: Kubernetes service ${K8S_API_SERVICE_NAME} did not get a load balancer endpoint"
  return 1
}

start_k8s_tiering_worker() {
  exp_log "Start k3s tiering worker: variant=${TIERING_POLICY_VARIANT:-A} trigger=${TIERING_TRIGGER_MODE:-periodic}"
  kubectl -n "${K8S_NAMESPACE}" scale deployment/tiering-worker --replicas=0 >/dev/null
  kubectl -n "${K8S_NAMESPACE}" set env deployment/tiering-worker \
    TIERING_POLICY_VARIANT="${TIERING_POLICY_VARIANT:-A}" \
    TIERING_TRIGGER_MODE="${TIERING_TRIGGER_MODE:-periodic}" \
    TIERING_POLICY_PERIOD_SEC="${TIERING_POLICY_PERIOD_SEC:-5}" \
    TIERING_THRESHOLD_CHECK_SEC="${TIERING_THRESHOLD_CHECK_SEC:-10}" \
    TIERING_THRESHOLD_COOLDOWN_SEC="${TIERING_THRESHOLD_COOLDOWN_SEC:-60}" \
    AGE_THRESHOLD_SEC="${AGE_THRESHOLD_SEC:-0}" \
    MAX_OBJECTS_PER_ROUND="${MAX_OBJECTS_PER_ROUND:-200}" \
    MAX_BYTES_PER_ROUND="${MAX_BYTES_PER_ROUND:-1073741824}" \
    WORKER_BW_LIMIT_MBPS="${WORKER_BW_LIMIT_MBPS:-0}" \
    WORKER_EC_SHARD_WRITE_PARALLELISM="${WORKER_EC_SHARD_WRITE_PARALLELISM:-2}" \
    TIERING_IDLE_STABLE_ROUNDS="${TIERING_IDLE_STABLE_ROUNDS:-3}" \
    TIERING_IDLE_CPU_PCT="${TIERING_IDLE_CPU_PCT:-70}" \
    TIERING_IDLE_MEMORY_PCT="${TIERING_IDLE_MEMORY_PCT:-90}" \
    TIERING_IDLE_IOWAIT_PCT="${TIERING_IDLE_IOWAIT_PCT:-20}" \
    TIERING_IDLE_QUEUE_DEPTH="${TIERING_IDLE_QUEUE_DEPTH:-16}" \
    TIERING_IDLE_MIN_NODE_RATIO="${TIERING_IDLE_MIN_NODE_RATIO:-0.8}" \
    TIERING_IDLE_MIN_NODE_COUNT="${TIERING_IDLE_MIN_NODE_COUNT:-4}" >/dev/null
  kubectl -n "${K8S_NAMESPACE}" scale deployment/tiering-worker --replicas="${TIERING_WORKER_REPLICAS}" >/dev/null
  kubectl -n "${K8S_NAMESPACE}" rollout status deployment/tiering-worker --timeout=180s
}

pressure_job_name() {
  local suffix
  suffix="$(printf '%s' "${RUN_ID}" | tr '[:upper:]' '[:lower:]' | tr '_' '-' | tr -c 'a-z0-9-' '-' | cut -c1-40)"
  suffix="${suffix%-}"
  if [[ -z "${suffix}" ]]; then
    suffix="run"
  fi
  printf 'pressure-%s-%s\n' "$1" "${suffix}"
}

start_k8s_pressure() {
  local profile="${PRESSURE_PROFILE}"
  if [[ "${profile}" == "none" || -z "${profile}" ]]; then
    return 0
  fi
  if (( PRESSURE_DURATION_SEC <= 0 )); then
    exp_log "ERROR: PRESSURE_DURATION_SEC must be > 0 when PRESSURE_PROFILE=${profile}"
    return 1
  fi
  case "${profile}" in
    cpu|io)
      ;;
    *)
      exp_log "ERROR: unknown PRESSURE_PROFILE=${profile}"
      return 1
      ;;
  esac

  (
    set +e
    if (( PRESSURE_DELAY_SEC > 0 )); then
      sleep "${PRESSURE_DELAY_SEC}"
    fi

    job=""
    job="$(pressure_job_name "${profile}")"
    kubectl -n "${K8S_NAMESPACE}" delete job "${job}" --ignore-not-found >/dev/null 2>&1
    case "${profile}" in
      cpu)
        exp_log "Start k3s CPU pressure job: cpus=${PRESSURE_CPUS} duration=${PRESSURE_DURATION_SEC}s"
        kubectl -n "${K8S_NAMESPACE}" create job "${job}" --image=alpine:3.18 -- \
          /bin/sh -c "apk add --no-cache stress-ng >/dev/null && stress-ng --cpu ${PRESSURE_CPUS} --timeout ${PRESSURE_DURATION_SEC}s"
        ;;
      io)
        exp_log "Start k3s I/O pressure job: hdd-workers=${HDD_WORKERS} bytes=${HDD_BYTES} duration=${PRESSURE_DURATION_SEC}s"
        kubectl -n "${K8S_NAMESPACE}" create job "${job}" --image=alpine:3.18 -- \
          /bin/sh -c "apk add --no-cache stress-ng >/dev/null && stress-ng --hdd ${HDD_WORKERS} --hdd-bytes ${HDD_BYTES} --timeout ${PRESSURE_DURATION_SEC}s"
        ;;
    esac
    kubectl -n "${K8S_NAMESPACE}" wait --for=condition=complete "job/${job}" --timeout="$((PRESSURE_DURATION_SEC + 180))s" >/dev/null
  ) &
  PRESSURE_PID="$!"
}

deploy_k8s_stack
configure_k8s_experiment_env
discover_k8s_api_base
write_k8s_run_env
wait_api_health
wait_node_discovery_ready "${MIN_HEALTHY_NODES}"

if [[ "${PRELOAD_OBJECTS}" == "true" ]]; then
  OBJECT_COUNT="${OBJECT_COUNT}" \
  OBJECT_SIZE_BYTES="${OBJECT_SIZE_BYTES}" \
  KEY_PREFIX="${KEY_PREFIX}" \
  "${SCRIPT_DIR}/../workloads/prepare_objects.sh"

  if (( PRELOAD_AGE_WAIT_SEC > 0 )); then
    exp_log "Wait preload aging: ${PRELOAD_AGE_WAIT_SEC}s"
    sleep "${PRELOAD_AGE_WAIT_SEC}"
  fi
fi

DURATION_SEC="${COLLECT_DURATION_SEC}" \
INTERVAL_SEC="${METRICS_INTERVAL_SEC}" \
OUT_FILE="${RESULT_DIR}/metrics.csv" \
RAW_FILE="${RESULT_DIR}/admin_samples.jsonl" \
"${SCRIPT_DIR}/../collect/collect_admin_metrics.sh" &
metrics_pid="$!"

PRESSURE_PID=""
start_k8s_pressure

if (( PRESSURE_WARMUP_SEC > 0 )); then
  exp_log "Wait pressure warmup: ${PRESSURE_WARMUP_SEC}s"
  sleep "${PRESSURE_WARMUP_SEC}"
fi

if [[ "${START_TIERING_WORKER}" == "true" ]]; then
  start_k8s_tiering_worker
fi

DURATION_SEC="${WORKLOAD_DURATION_SEC}" \
CONCURRENCY="${WORKLOAD_CONCURRENCY}" \
GET_PERCENT="${GET_PERCENT}" \
PRELOAD_COUNT="${OBJECT_COUNT}" \
PUT_SIZE_BYTES="${OBJECT_SIZE_BYTES}" \
KEY_PREFIX="${KEY_PREFIX}" \
RESULT_FILE="${RESULT_DIR}/latency.csv" \
"${SCRIPT_DIR}/../workloads/mixed_put_get.sh"

if [[ -n "${PRESSURE_PID}" ]]; then
  wait "${PRESSURE_PID}" || exp_log "WARN: k3s pressure job did not complete cleanly"
fi
wait "${metrics_pid}" || true

collect_k8s_logs "${K8S_NAMESPACE}" || true
analyze_phase_latency_dir || true

"${SCRIPT_DIR}/../collect/summarize_latency.py" "${RESULT_DIR}/latency.csv" --out "${SUMMARY_FILE}" | tee "${RESULT_DIR}/summary.stdout.csv"

exp_log "K3s scenario complete: ${SCENARIO}"
exp_log "Result dir: ${RESULT_DIR}"
