# Run Fair Local Experiments

This project has two different experiment goals, and they should not be mixed in
the same comparison table.

1. **Fair strategy comparison:** baseline, A, B, and C run with the same object
   size, object count, foreground workload, and pressure profile. Only the
   tiering policy changes.
2. **Pressure-awareness mechanism check:** inspect a Strategy C run to confirm
   that the idle-window gate delays task enqueue while pressure is high and
   resumes after the cluster becomes idle.

## Controlled Variables

For a fair matrix, these fields in `run.env` must match across scenarios:

```text
OBJECT_COUNT
OBJECT_SIZE_BYTES
PRELOAD_OBJECTS
PRELOAD_AGE_WAIT_SEC
AGE_THRESHOLD_SEC
WORKLOAD_DURATION_SEC
WORKLOAD_CONCURRENCY
GET_PERCENT
PRESSURE_PROFILE
PRESSURE_CPUS
PRESSURE_DURATION_SEC
PRESSURE_DELAY_SEC
PRESSURE_WARMUP_SEC
HDD_WORKERS
HDD_BYTES
METRICS_INTERVAL_SEC
```

`run_matrix_local.sh` now passes these fields to every scenario and writes a
fairness report:

```text
experiments/results/matrix-<run_id_root>-fairness.txt
```

If the report does not end with `PASS`, do not use that matrix as a formal
comparison.

## Policy Groups

The intentional differences are:

```text
baseline: no tiering worker
A:        age-based enqueue, no object/byte budget, no worker bandwidth limit
B:        age-based enqueue, object/byte budget, worker bandwidth limit
C:        same budget/throttle as B, plus pressure-aware idle-window admission
```

For the key research claim, compare **B vs C** under the same pressure profile.
A is still useful because it shows what happens when migration is injected
aggressively.

## Recommended Local Runs

No-pressure matrix, measuring migration overhead without external stress. Keep
`WORKLOAD_DURATION_SEC < AGE_THRESHOLD_SEC` so live foreground PUTs do not become
migration candidates during the same run:

```bash
MATRIX_PRESSURE_PROFILE=none \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
./experiments/scenarios/run_matrix_local.sh
```

CPU-pressure matrix, measuring behavior under compute contention:

```bash
MATRIX_PRESSURE_PROFILE=cpu MATRIX_PRESSURE_CPUS=2 \
MATRIX_PRESSURE_DURATION_SEC=60 MATRIX_PRESSURE_WARMUP_SEC=10 \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
./experiments/scenarios/run_matrix_local.sh
```

I/O-pressure matrix, measuring behavior under disk contention:

```bash
MATRIX_PRESSURE_PROFILE=io MATRIX_HDD_WORKERS=2 MATRIX_HDD_BYTES=512M \
MATRIX_PRESSURE_DURATION_SEC=60 MATRIX_PRESSURE_WARMUP_SEC=10 \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
./experiments/scenarios/run_matrix_local.sh
```

For report-quality numbers, run each matrix at least three times after a clean
machine restart or a quiet period. Do not average together runs with different
`run.env` values.

## Why Preload Aging Exists

The fair matrix should migrate a controlled set of older objects. It should not
immediately migrate objects created by the foreground workload. If
`AGE_THRESHOLD_SEC=0`, every live PUT becomes eligible right away, causing this
feedback loop:

```text
foreground PUT -> due index -> migration task -> more background work
```

`PRELOAD_AGE_WAIT_SEC` waits after preload and before the foreground workload.
With `AGE_THRESHOLD_SEC=60` and `PRELOAD_AGE_WAIT_SEC=70`, only preload objects
are old enough when the worker starts. New live PUTs remain hot during a
45-second local workload.

## Reading Results

Latency comparison:

```text
experiments/results/matrix-<run_id_root>-comparison.csv
```

Use `ALL p99_ms` for the headline, then check `GET p99_ms` and `PUT p99_ms`
separately. Large `max_ms` spikes on local WSL should be discussed as local
environment noise unless they repeat across runs.

Migration summary:

```text
experiments/results/matrix-<run_id_root>-migration.csv
```

Key fields:

```text
final_repl_done       completed REPL_TO_EC tasks
final_repl_pending    queued migration tasks not yet completed
final_due_ready       eligible objects not yet enqueued as tasks
first_repl_activity   first observed migration activity offset
repl_done_per_min     completed migration rate
```

For Strategy C, a useful pressure-aware pattern is:

```text
pressure high -> due_ready grows, pending grows slowly or pauses
pressure low  -> pending/done increase, due_ready decreases
```

That proves the admission gate is working. It does not mean already-running
tasks are cancelled; current C controls enqueue admission, not task preemption.
