# Local Experiment Harness

This directory contains repeatable local experiments for the tiering scheduler.
The intended flow is:

1. start a clean Docker Compose stack,
2. preload HOT replicated objects,
3. start the selected tiering policy,
4. run foreground PUT/GET traffic,
5. collect admin metrics and latency CSV files,
6. summarize P50/P95/P99 latency.

## Quick Start

Run one short local scenario:

```bash
OBJECT_COUNT=50 WORKLOAD_DURATION_SEC=30 WORKLOAD_CONCURRENCY=4 \
  ./experiments/scenarios/strategy_a_age_based.sh
```

After the first successful build, use `BUILD_STACK=false` to skip rebuilding
images while you are only changing experiment parameters:

```bash
BUILD_STACK=false OBJECT_COUNT=50 WORKLOAD_DURATION_SEC=30 \
  ./experiments/scenarios/strategy_a_age_based.sh
```

Run the default local matrix:

```bash
OBJECT_COUNT=200 WORKLOAD_DURATION_SEC=120 WORKLOAD_CONCURRENCY=8 \
  ./experiments/scenarios/run_matrix_local.sh
```

Results are written under:

```text
experiments/results/<scenario>/<run_id>/
```

Important files:

```text
objects.csv          preload PUT result
latency.csv          foreground request latency
metrics.csv          sampled admin/node/task metrics
admin_samples.jsonl  raw admin API samples
summary.csv          p50/p95/p99 summary
run.env              scenario configuration
```

## Scenarios

| Script | Purpose |
| --- | --- |
| `baseline_no_migration.sh` | foreground-only baseline; tiering worker is not started |
| `strategy_a_age_based.sh` | age-based migration, no byte budget |
| `strategy_b_throttled.sh` | age-based migration with object/byte budget and worker bandwidth throttle |
| `strategy_c_pressure_aware.sh` | idle-window admission gate with optional CPU pressure |

The scripts default to `K=4, M=2` and wait for six storage nodes, matching the
current code configuration.

## Useful Overrides

```bash
OBJECT_COUNT=300
OBJECT_SIZE_BYTES=1048576
WORKLOAD_DURATION_SEC=180
WORKLOAD_CONCURRENCY=12
GET_PERCENT=70
MAX_OBJECTS_PER_ROUND=25
MAX_BYTES_PER_ROUND=33554432
WORKER_BW_LIMIT_MBPS=8
PRESSURE_PROFILE=cpu
PRESSURE_DURATION_SEC=90
```

Strategy C uses these idle-window settings:

```bash
TIERING_IDLE_STABLE_ROUNDS=3
TIERING_IDLE_CPU_PCT=70
TIERING_IDLE_MEMORY_PCT=90
TIERING_IDLE_IOWAIT_PCT=20
TIERING_IDLE_QUEUE_DEPTH=16
```

## Notes

The pressure scripts run a temporary `alpine:3.18` container and install
`stress-ng` inside it. This is convenient for local experiments, but it means the
first pressure run may take longer while the image/package is fetched.

The pressure load is host-level for the local Docker environment. That is enough
to validate the scheduler and graphing pipeline locally; AWS/k3s should be used
later for node-separated validation.

If Docker Desktop is running but WSL does not see the `docker` command, try:

```bash
DOCKER_BIN=docker.exe ./experiments/scenarios/baseline_no_migration.sh
```
