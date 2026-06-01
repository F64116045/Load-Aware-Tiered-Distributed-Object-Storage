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
K8S_PRESSURE_IMAGE="${K8S_PRESSURE_IMAGE:-alpine:3.18}"
K8S_PRESSURE_INSTALL_CMD="${K8S_PRESSURE_INSTALL_CMD:-apk add --no-cache stress-ng >/dev/null}"
K8S_PRESSURE_AFFINITY_MODE="${K8S_PRESSURE_AFFINITY_MODE:-preferred}"
K8S_PRESSURE_AVOID_APPS="${K8S_PRESSURE_AVOID_APPS:-pd,tikv,meta-service,api}"
K8S_PRESSURE_TOPOLOGY_KEY="${K8S_PRESSURE_TOPOLOGY_KEY:-kubernetes.io/hostname}"
K8S_PRESSURE_TARGET_NODE="${K8S_PRESSURE_TARGET_NODE:-}"
METRICS_INTERVAL_SEC="${METRICS_INTERVAL_SEC:-5}"
COLLECT_DURATION_SEC="${COLLECT_DURATION_SEC:-$((WORKLOAD_DURATION_SEC + PRESSURE_DURATION_SEC + PRESSURE_DELAY_SEC + 30))}"
SUMMARY_FILE="${SUMMARY_FILE:-${RESULT_DIR}/summary.csv}"
TIERING_WORKER_REPLICAS="${TIERING_WORKER_REPLICAS:-1}"
K8S_DISCOVER_API_BASE="${K8S_DISCOVER_API_BASE:-false}"
K8S_API_SERVICE_NAME="${K8S_API_SERVICE_NAME:-api}"
K8S_API_SERVICE_PORT="${K8S_API_SERVICE_PORT:-8000}"
K8S_IN_CLUSTER_WORKLOAD="${K8S_IN_CLUSTER_WORKLOAD:-false}"
K8S_WORKLOAD_API_BASE="${K8S_WORKLOAD_API_BASE:-http://${K8S_API_SERVICE_NAME}:${K8S_API_SERVICE_PORT}}"
K8S_WORKLOAD_IMAGE="${K8S_WORKLOAD_IMAGE:-alpine:3.18}"
K8S_WORKLOAD_INSTALL_CMD="${K8S_WORKLOAD_INSTALL_CMD:-apk add --no-cache bash curl coreutils gawk >/dev/null}"

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
K8S_IN_CLUSTER_WORKLOAD=${K8S_IN_CLUSTER_WORKLOAD}
K8S_WORKLOAD_API_BASE=${K8S_WORKLOAD_API_BASE}
K8S_WORKLOAD_IMAGE=${K8S_WORKLOAD_IMAGE}
K8S_WORKLOAD_INSTALL_CMD=${K8S_WORKLOAD_INSTALL_CMD}
K8S_PRESSURE_IMAGE=${K8S_PRESSURE_IMAGE}
K8S_PRESSURE_INSTALL_CMD=${K8S_PRESSURE_INSTALL_CMD}
K8S_PRESSURE_AFFINITY_MODE=${K8S_PRESSURE_AFFINITY_MODE}
K8S_PRESSURE_AVOID_APPS=${K8S_PRESSURE_AVOID_APPS}
K8S_PRESSURE_TOPOLOGY_KEY=${K8S_PRESSURE_TOPOLOGY_KEY}
K8S_PRESSURE_TARGET_NODE=${K8S_PRESSURE_TARGET_NODE}
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

  if [[ -n "${STORAGE_GROUP_SYNC_INTERVAL_MS:-}" || -n "${STORAGE_GROUP_SYNC_MAX_BATCH:-}" ]]; then
    exp_log "Configure storage group sync: interval_ms=${STORAGE_GROUP_SYNC_INTERVAL_MS:-default} max_batch=${STORAGE_GROUP_SYNC_MAX_BATCH:-default}"
    kubectl -n "${K8S_NAMESPACE}" set env statefulset/storage-node \
      ${STORAGE_GROUP_SYNC_INTERVAL_MS:+STORAGE_GROUP_SYNC_INTERVAL_MS="${STORAGE_GROUP_SYNC_INTERVAL_MS}"} \
      ${STORAGE_GROUP_SYNC_MAX_BATCH:+STORAGE_GROUP_SYNC_MAX_BATCH="${STORAGE_GROUP_SYNC_MAX_BATCH}"} >/dev/null
    changed_storage=true
  fi

  if [[ -n "${STORAGE_BACKGROUND_MAX_QUEUED_WRITE_BYTES:-}" ]]; then
    exp_log "Configure storage background queue cap: STORAGE_BACKGROUND_MAX_QUEUED_WRITE_BYTES=${STORAGE_BACKGROUND_MAX_QUEUED_WRITE_BYTES}"
    kubectl -n "${K8S_NAMESPACE}" set env statefulset/storage-node \
      STORAGE_BACKGROUND_MAX_QUEUED_WRITE_BYTES="${STORAGE_BACKGROUND_MAX_QUEUED_WRITE_BYTES}" >/dev/null
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
  suffix="$(k8s_safe_suffix "${RUN_ID}")"
  printf 'pressure-%s-%s\n' "$1" "${suffix}"
}

render_k8s_pressure_app_values() {
  local apps="${1:-}"
  local indent="${2:-                }"
  local app
  IFS=',' read -r -a app_array <<<"${apps}"
  for app in "${app_array[@]}"; do
    app="${app// /}"
    [[ -n "${app}" ]] || continue
    printf '%s- %s\n' "${indent}" "${app}"
  done
}

render_k8s_pressure_affinity() {
  if [[ -z "${K8S_PRESSURE_AVOID_APPS}" || "${K8S_PRESSURE_AFFINITY_MODE}" == "none" ]]; then
    return 0
  fi

  case "${K8S_PRESSURE_AFFINITY_MODE}" in
    required)
      cat <<EOF
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchExpressions:
              - key: app
                operator: In
                values:
$(render_k8s_pressure_app_values "${K8S_PRESSURE_AVOID_APPS}" "                ")
            topologyKey: ${K8S_PRESSURE_TOPOLOGY_KEY}
EOF
      ;;
    preferred)
      cat <<EOF
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                - key: app
                  operator: In
                  values:
$(render_k8s_pressure_app_values "${K8S_PRESSURE_AVOID_APPS}" "                  ")
              topologyKey: ${K8S_PRESSURE_TOPOLOGY_KEY}
EOF
      ;;
    *)
      exp_log "ERROR: unknown K8S_PRESSURE_AFFINITY_MODE=${K8S_PRESSURE_AFFINITY_MODE}"
      return 1
      ;;
  esac
}

render_k8s_pressure_scheduling() {
  if [[ -n "${K8S_PRESSURE_TARGET_NODE}" ]]; then
    cat <<EOF
      nodeName: ${K8S_PRESSURE_TARGET_NODE}
EOF
    return 0
  fi

  render_k8s_pressure_affinity
}

record_k8s_pressure_placement() {
  local job="$1"
  local profile="$2"
  local log_dir="${RESULT_DIR}/logs/k8s-pressure"
  local out_file="${log_dir}/${profile}-placement.txt"
  local pod node
  mkdir -p "${log_dir}"

  for _ in $(seq 1 60); do
    pod="$(kubectl -n "${K8S_NAMESPACE}" get pods -l "job-name=${job}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    node="$(kubectl -n "${K8S_NAMESPACE}" get pods -l "job-name=${job}" -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null || true)"
    if [[ -n "${pod}" && -n "${node}" ]]; then
      {
        printf 'profile=%s\n' "${profile}"
        printf 'job=%s\n' "${job}"
        printf 'pod=%s\n' "${pod}"
        printf 'node=%s\n' "${node}"
        printf 'target_node=%s\n' "${K8S_PRESSURE_TARGET_NODE}"
        printf 'affinity_mode=%s\n' "${K8S_PRESSURE_AFFINITY_MODE}"
        printf 'avoid_apps=%s\n' "${K8S_PRESSURE_AVOID_APPS}"
      } >"${out_file}"
      exp_log "Pressure job placement: profile=${profile} pod=${pod} node=${node}"
      return 0
    fi
    sleep 1
  done

  exp_log "WARN: could not observe pressure job placement for job=${job}"
  return 0
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
    local job pressure_cmd log_dir
    if (( PRESSURE_DELAY_SEC > 0 )); then
      sleep "${PRESSURE_DELAY_SEC}"
    fi

    job="$(pressure_job_name "${profile}")"
    log_dir="${RESULT_DIR}/logs/k8s-pressure"
    mkdir -p "${log_dir}"
    kubectl -n "${K8S_NAMESPACE}" delete job "${job}" --ignore-not-found >/dev/null 2>&1
    case "${profile}" in
      cpu)
        exp_log "Start k3s CPU pressure job: cpus=${PRESSURE_CPUS} duration=${PRESSURE_DURATION_SEC}s"
        pressure_cmd="stress-ng --cpu ${PRESSURE_CPUS} --timeout ${PRESSURE_DURATION_SEC}s"
        ;;
      io)
        exp_log "Start k3s I/O pressure job: hdd-workers=${HDD_WORKERS} bytes=${HDD_BYTES} duration=${PRESSURE_DURATION_SEC}s"
        pressure_cmd="stress-ng --hdd ${HDD_WORKERS} --hdd-bytes ${HDD_BYTES} --timeout ${PRESSURE_DURATION_SEC}s"
        ;;
    esac

    cat <<EOF | kubectl -n "${K8S_NAMESPACE}" apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job}
  labels:
    app: rec-store-pressure
    pressure-profile: ${profile}
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: rec-store-pressure
        pressure-profile: ${profile}
    spec:
      restartPolicy: Never
$(render_k8s_pressure_scheduling)
      containers:
      - name: pressure
        image: ${K8S_PRESSURE_IMAGE}
        imagePullPolicy: IfNotPresent
        command:
        - /bin/sh
        - -c
        - |
          set -eu
          ${K8S_PRESSURE_INSTALL_CMD}
          echo "pressure_profile=${profile}"
          echo "node_name=\${NODE_NAME:-unknown}"
          ${pressure_cmd}
        env:
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
EOF
    record_k8s_pressure_placement "${job}" "${profile}"
    if ! kubectl -n "${K8S_NAMESPACE}" wait --for=condition=complete "job/${job}" --timeout="$((PRESSURE_DURATION_SEC + 180))s" >/dev/null; then
      kubectl -n "${K8S_NAMESPACE}" describe "job/${job}" >"${log_dir}/${profile}-job.describe.txt" 2>&1 || true
      kubectl -n "${K8S_NAMESPACE}" get pods -l "job-name=${job}" -o wide >"${log_dir}/${profile}-pods.txt" 2>&1 || true
      kubectl -n "${K8S_NAMESPACE}" logs "job/${job}" --all-containers=true >"${log_dir}/${profile}.log" 2>&1 || true
      exp_log "WARN: k3s pressure job did not complete cleanly; see ${log_dir}"
      exit 1
    fi
    kubectl -n "${K8S_NAMESPACE}" get pods -l "job-name=${job}" -o wide >"${log_dir}/${profile}-pods.txt" 2>&1 || true
    kubectl -n "${K8S_NAMESPACE}" logs "job/${job}" --all-containers=true >"${log_dir}/${profile}.log" 2>&1 || true
  ) &
  PRESSURE_PID="$!"
}

k8s_safe_suffix() {
  local raw="${1:-${RUN_ID}}"
  local suffix
  suffix="$(printf '%s' "${raw}" | tr '[:upper:]' '[:lower:]' | tr '_' '-' | tr -c 'a-z0-9-' '-' | cut -c1-42 | sed 's/^-*//; s/-*$//')"
  if [[ -z "${suffix}" ]]; then
    suffix="run"
  fi
  printf '%s\n' "${suffix}"
}

k8s_yaml_string() {
  local value="${1:-}"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"\n' "${value}"
}

create_k8s_workload_configmap() {
  local configmap="$1"
  kubectl -n "${K8S_NAMESPACE}" delete configmap "${configmap}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${K8S_NAMESPACE}" create configmap "${configmap}" \
    --from-file=common.sh="${SCRIPT_DIR}/../lib/common.sh" \
    --from-file=prepare_objects.sh="${SCRIPT_DIR}/../workloads/prepare_objects.sh" \
    --from-file=mixed_put_get.sh="${SCRIPT_DIR}/../workloads/mixed_put_get.sh" >/dev/null
}

run_k8s_workload_script() {
  local kind="$1"
  local script_name="$2"
  local output_name="$3"
  local timeout_sec="$4"
  local suffix configmap job log_dir log_file

  suffix="$(k8s_safe_suffix "${RUN_ID}-${kind}")"
  configmap="$(printf 'workload-scripts-%s' "${suffix}" | cut -c1-63 | sed 's/-*$//')"
  job="$(printf 'workload-%s-%s' "${kind}" "${suffix}" | cut -c1-63 | sed 's/-*$//')"
  log_dir="${RESULT_DIR}/logs/k8s-workload"
  log_file="${log_dir}/${kind}.log"
  mkdir -p "${log_dir}"

  exp_log "Run ${kind} workload inside k8s: job=${job} api_base=${K8S_WORKLOAD_API_BASE}"
  create_k8s_workload_configmap "${configmap}"
  kubectl -n "${K8S_NAMESPACE}" delete job "${job}" --ignore-not-found >/dev/null 2>&1 || true

  cat <<EOF | kubectl -n "${K8S_NAMESPACE}" apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job}
  labels:
    app: rec-store-workload
    run-id: ${suffix}
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: rec-store-workload
        run-id: ${suffix}
    spec:
      restartPolicy: Never
      containers:
      - name: workload
        image: ${K8S_WORKLOAD_IMAGE}
        imagePullPolicy: IfNotPresent
        command:
        - /bin/sh
        - -c
        - |
          set -eu
          ${K8S_WORKLOAD_INSTALL_CMD}
          mkdir -p /work/experiments/lib /work/experiments/workloads /work/results
          cp /work/scripts/common.sh /work/experiments/lib/common.sh
          cp /work/scripts/prepare_objects.sh /work/experiments/workloads/prepare_objects.sh
          cp /work/scripts/mixed_put_get.sh /work/experiments/workloads/mixed_put_get.sh
          chmod +x /work/experiments/workloads/*.sh
          cd /work
          /work/experiments/workloads/${script_name}
          printf '__REC_STORE_RESULT_BEGIN__:${output_name}\n'
          base64 /work/results/${output_name}
          printf '\n__REC_STORE_RESULT_END__:${output_name}\n'
        env:
        - name: API_BASE
          value: $(k8s_yaml_string "${K8S_WORKLOAD_API_BASE}")
        - name: SCENARIO
          value: $(k8s_yaml_string "${SCENARIO}")
        - name: RUN_ID
          value: $(k8s_yaml_string "${RUN_ID}")
        - name: RESULT_ROOT
          value: "/work/results-root"
        - name: RESULT_DIR
          value: "/work/results"
        - name: OBJECT_COUNT
          value: $(k8s_yaml_string "${OBJECT_COUNT}")
        - name: OBJECT_SIZE_BYTES
          value: $(k8s_yaml_string "${OBJECT_SIZE_BYTES}")
        - name: KEY_PREFIX
          value: $(k8s_yaml_string "${KEY_PREFIX}")
        - name: MANIFEST_FILE
          value: "/work/results/objects.csv"
        - name: DURATION_SEC
          value: $(k8s_yaml_string "${WORKLOAD_DURATION_SEC}")
        - name: CONCURRENCY
          value: $(k8s_yaml_string "${WORKLOAD_CONCURRENCY}")
        - name: GET_PERCENT
          value: $(k8s_yaml_string "${GET_PERCENT}")
        - name: PRELOAD_COUNT
          value: $(k8s_yaml_string "${OBJECT_COUNT}")
        - name: PUT_SIZE_BYTES
          value: $(k8s_yaml_string "${OBJECT_SIZE_BYTES}")
        - name: RESULT_FILE
          value: "/work/results/latency.csv"
        volumeMounts:
        - name: workload-scripts
          mountPath: /work/scripts
          readOnly: true
        - name: workload-results
          mountPath: /work/results
      volumes:
      - name: workload-scripts
        configMap:
          name: ${configmap}
          defaultMode: 0755
      - name: workload-results
        emptyDir: {}
EOF

  if ! kubectl -n "${K8S_NAMESPACE}" wait --for=condition=complete "job/${job}" --timeout="${timeout_sec}s"; then
    kubectl -n "${K8S_NAMESPACE}" describe "job/${job}" >"${log_dir}/${kind}-job.describe.txt" 2>&1 || true
    kubectl -n "${K8S_NAMESPACE}" get pods -l "job-name=${job}" -o wide >"${log_dir}/${kind}-pods.txt" 2>&1 || true
    kubectl -n "${K8S_NAMESPACE}" logs "job/${job}" --all-containers=true >"${log_file}" 2>&1 || true
    exp_log "ERROR: in-cluster ${kind} workload did not complete; see ${log_dir}"
    return 1
  fi

  kubectl -n "${K8S_NAMESPACE}" logs "job/${job}" --all-containers=true >"${log_file}" 2>&1 || true
  if ! awk -v begin="__REC_STORE_RESULT_BEGIN__:${output_name}" -v end="__REC_STORE_RESULT_END__:${output_name}" '
      $0 == begin { capture = 1; next }
      $0 == end { found = 1; capture = 0; next }
      capture { print }
      END { if (!found) exit 1 }
    ' "${log_file}" | base64 -d >"${RESULT_DIR}/${output_name}"; then
    exp_log "ERROR: could not extract ${output_name} from in-cluster ${kind} log: ${log_file}"
    return 1
  fi
  kubectl -n "${K8S_NAMESPACE}" delete job "${job}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${K8S_NAMESPACE}" delete configmap "${configmap}" --ignore-not-found >/dev/null 2>&1 || true
}

run_prepare_objects() {
  if [[ "${K8S_IN_CLUSTER_WORKLOAD}" == "true" ]]; then
    local preload_timeout
    preload_timeout="${K8S_PRELOAD_TIMEOUT_SEC:-$((OBJECT_COUNT * 5 + 300))}"
    run_k8s_workload_script "preload" "prepare_objects.sh" "objects.csv" "${preload_timeout}"
  else
    OBJECT_COUNT="${OBJECT_COUNT}" \
    OBJECT_SIZE_BYTES="${OBJECT_SIZE_BYTES}" \
    KEY_PREFIX="${KEY_PREFIX}" \
    "${SCRIPT_DIR}/../workloads/prepare_objects.sh"
  fi
}

run_mixed_workload() {
  if [[ "${K8S_IN_CLUSTER_WORKLOAD}" == "true" ]]; then
    local workload_timeout
    workload_timeout="${K8S_WORKLOAD_TIMEOUT_SEC:-$((WORKLOAD_DURATION_SEC + 300))}"
    run_k8s_workload_script "mixed" "mixed_put_get.sh" "latency.csv" "${workload_timeout}"
  else
    DURATION_SEC="${WORKLOAD_DURATION_SEC}" \
    CONCURRENCY="${WORKLOAD_CONCURRENCY}" \
    GET_PERCENT="${GET_PERCENT}" \
    PRELOAD_COUNT="${OBJECT_COUNT}" \
    PUT_SIZE_BYTES="${OBJECT_SIZE_BYTES}" \
    KEY_PREFIX="${KEY_PREFIX}" \
    RESULT_FILE="${RESULT_DIR}/latency.csv" \
    "${SCRIPT_DIR}/../workloads/mixed_put_get.sh"
  fi
}

deploy_k8s_stack
configure_k8s_experiment_env
discover_k8s_api_base
write_k8s_run_env
wait_api_health
wait_node_discovery_ready "${MIN_HEALTHY_NODES}"

if [[ "${PRELOAD_OBJECTS}" == "true" ]]; then
  run_prepare_objects

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

run_mixed_workload

if [[ -n "${PRESSURE_PID}" ]]; then
  wait "${PRESSURE_PID}" || exp_log "WARN: k3s pressure job did not complete cleanly"
fi
wait "${metrics_pid}" || true

collect_k8s_logs "${K8S_NAMESPACE}" || true
analyze_phase_latency_dir || true

"${SCRIPT_DIR}/../collect/summarize_latency.py" "${RESULT_DIR}/latency.csv" --out "${SUMMARY_FILE}" | tee "${RESULT_DIR}/summary.stdout.csv"

exp_log "K3s scenario complete: ${SCENARIO}"
exp_log "Result dir: ${RESULT_DIR}"
