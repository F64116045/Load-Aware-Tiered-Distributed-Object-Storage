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

Run a fair local matrix with no injected pressure:

```bash
MATRIX_PRESSURE_PROFILE=none \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 \
  ./experiments/scenarios/run_matrix_local.sh
```

Run the same fair matrix with CPU pressure applied to every scenario:

```bash
MATRIX_PRESSURE_PROFILE=cpu MATRIX_PRESSURE_CPUS=2 \
MATRIX_PRESSURE_DURATION_SEC=60 MATRIX_PRESSURE_WARMUP_SEC=10 \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 \
  ./experiments/scenarios/run_matrix_local.sh
```

Run the same fair matrix with I/O pressure applied to every scenario:

```bash
MATRIX_PRESSURE_PROFILE=io MATRIX_HDD_WORKERS=2 MATRIX_HDD_BYTES=512M \
MATRIX_PRESSURE_DURATION_SEC=60 MATRIX_PRESSURE_WARMUP_SEC=10 \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 \
  ./experiments/scenarios/run_matrix_local.sh
```

The matrix runner passes the same workload and pressure parameters to every
scenario. It also writes a fairness report and fails if non-policy parameters
diverge:

```text
experiments/results/matrix-<run_id_root>-fairness.txt
experiments/results/matrix-<run_id_root>-comparison.csv
experiments/results/matrix-<run_id_root>-migration.csv
```

To compare the newest available run of each scenario without rerunning:

```bash
./experiments/collect/compare_summaries.py --latest-per-scenario
./experiments/collect/summarize_migration.py --latest-per-scenario
```

For AWS/k3s runs, build and push an image, deploy `deploy/k3s/base`, then use
the k3s matrix runner:

```bash
IMAGE=<registry>/<repo>:aws-exp-001 \
API_BASE=http://<control-plane-public-ip>:30080 \
MATRIX_PRESSURE_PROFILE=none \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
  ./experiments/scenarios/run_matrix_k3s.sh
```

See `docs/how-to/run-aws-k3s-experiments.md` for the full cloud workflow.

For GCP/GKE runs, build and push an Artifact Registry image, deploy the GKE
overlay, then use the GKE matrix runner. The runner discovers the `api`
LoadBalancer IP after each namespace reset.

```bash
IMAGE=asia-east1-docker.pkg.dev/<project-id>/rec-store/rec-store:gke-exp-001 \
MATRIX_PRESSURE_PROFILE=none \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
  ./experiments/scenarios/run_matrix_gke.sh
```

See `docs/how-to/run-gcp-gke-experiments.md` for the full GCP workflow.

To run the full GKE experiment suite (`none`, `cpu`, and `io` pressure
profiles; each profile runs baseline/A/B/C), use:

```bash
IMAGE=asia-east1-docker.pkg.dev/<project-id>/rec-store/rec-store:gke-exp-001 \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=90 \
OBJECT_COUNT=50 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=60 WORKLOAD_CONCURRENCY=2 GET_PERCENT=70 \
  ./experiments/scenarios/run_gke_experiment_suite.sh
```

Results are written under:

```text
experiments/results/<scenario>/<run_id>/
experiments/results/suite-<suite_run_id>-index.csv
experiments/results/suite-<suite_run_id>-latency.csv
experiments/results/suite-<suite_run_id>-migration.csv
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
| `strategy_c_pressure_aware.sh` | Strategy B's budget/throttle plus idle-window admission gate |

The scripts default to `K=4, M=2` and wait for six storage nodes, matching the
current code configuration.

For strategy comparison, use `run_matrix_local.sh` rather than comparing ad-hoc
single scenario runs. The intended controlled variables are object count, object
size, preload aging, age threshold, foreground workload duration, foreground
concurrency, GET ratio, pressure profile, pressure duration, and metrics
interval. The intended policy variables are:

| Policy | Intentional difference |
| --- | --- |
| baseline | no background tiering worker |
| A | age-based enqueue, no migration budget/throttle |
| B | age-based enqueue with object/byte budget and worker bandwidth limit |
| C | same budget/throttle as B, plus pressure-aware idle-window admission |

For the research question "does pressure awareness reduce tail latency under
load?", compare B and C under the same `MATRIX_PRESSURE_PROFILE`. A is useful as
an unthrottled stress baseline, but B is the closest control group for C.

## Useful Overrides

```bash
OBJECT_COUNT=300
OBJECT_SIZE_BYTES=1048576
AGE_THRESHOLD_SEC=60
PRELOAD_AGE_WAIT_SEC=70
WORKLOAD_DURATION_SEC=180
WORKLOAD_CONCURRENCY=12
GET_PERCENT=70
MAX_OBJECTS_PER_ROUND=25
MAX_BYTES_PER_ROUND=33554432
WORKER_BW_LIMIT_MBPS=8
PRESSURE_PROFILE=cpu
PRESSURE_DURATION_SEC=90
PRESSURE_DELAY_SEC=0
PRESSURE_WARMUP_SEC=10
```

Strategy C uses these idle-window settings:

```bash
TIERING_IDLE_STABLE_ROUNDS=3
TIERING_IDLE_CPU_PCT=70
TIERING_IDLE_MEMORY_PCT=90
TIERING_IDLE_IOWAIT_PCT=20
TIERING_IDLE_QUEUE_DEPTH=16
TIERING_IDLE_MIN_NODE_RATIO=0.8
TIERING_IDLE_MIN_NODE_COUNT=4
```

## Notes

For local matrix runs, keep `WORKLOAD_DURATION_SEC < AGE_THRESHOLD_SEC` unless
you intentionally want live foreground PUTs to become migration candidates in the
same run. The default matrix ages only the preload set before the workload
starts, which avoids the feedback loop where foreground traffic creates new
migration work immediately.

Do not compare runs with different `run.env` workload, age, or pressure fields.
A single run can contain local outliers, especially on WSL/Docker Desktop, so
use at least three repeated matrices for report-quality numbers and report
median P99 or mean P99 with the raw CSV files retained.

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
