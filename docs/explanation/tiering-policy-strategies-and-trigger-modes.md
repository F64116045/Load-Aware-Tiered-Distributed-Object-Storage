# Explanation: Tiering Policy Strategies and Trigger Modes

This document explains the full tiering chain and the exact runtime differences between:

1. Strategy variants `A` / `B` / `C` (candidate selection and admission behavior)
2. Trigger modes `periodic` / `threshold` / `hybrid` (when scanner runs)

It is code-aligned to current implementation.

## 1. One-Screen Mental Model

Two axes are configured independently:

1. `TIERING_POLICY_VARIANT` controls how candidates are selected and admitted.
2. `TIERING_TRIGGER_MODE` controls when scanner ticks happen.

This means `A/B/C` can each run under `periodic`, `threshold`, or `hybrid`.

Code:

1. `cmd/tiering_worker/main.go`
2. `internal/tiering/policy_scanner.go`
3. `internal/meta/tikv_store_policy.go`
4. `internal/config/config.go`

## 2. Shared End-to-End Chain (PUT -> EC)

The following chain is shared by all strategies. Strategy differences happen at scanner enqueue stage.

### 2.1 PUT writes HOT data and metadata

1. API receives `PUT /v2/objects/{object_id}`.
2. Write service writes HOT replicas with quorum.
3. Metadata upsert writes:
   1. `obj/*` object head
   2. `objv/*` version row
   3. `repl/*` active HOT locations
   4. due-index:
      1. `tdue/<eligible_at>/<object>/<version>`
      2. `tdue_ref/<object>/<version>`

Code:

1. `cmd/api/main.go`
2. `internal/writeservice/writeservice.go`
3. `internal/meta/tikv_store_objects.go`
4. `internal/meta/tikv_store_due_index.go`

Important:

1. Current write path does not directly enqueue `REPL_TO_EC` task.
2. Tiering enqueue is scanner-driven from due-index.

### 2.2 Scanner reads due-index and enqueues tasks

1. Leader scanner calls strategy enqueue method.
2. Metadata store scans ready due candidates from `tdue/*` (bounded by `TIERING_DUE_INDEX_MAX_SCAN`).
3. For each due candidate, it validates:
   1. object exists
   2. candidate version is still current version
   3. object state is HOT-side state (`HOT_ACTIVE` or `MIGRATION_PENDING`)
   4. version tier is still `HOT`
4. It sorts candidates and selects a subset by strategy logic.
5. For each selected candidate:
   1. create deterministic task id `repl2ec:<object>:<version>` if not exists
   2. set task state to `PENDING`
   3. delete `tdue/*` and `tdue_ref/*` for that version
   4. move object state from `HOT_ACTIVE` to `MIGRATION_PENDING` when applicable

Code:

1. `internal/tiering/policy_scanner.go`
2. `internal/meta/tikv_store_policy.go`
3. `internal/meta/tikv_store_due_index.go`

### 2.3 Worker claims task and promotes to EC

1. Worker claim loop selects runnable tasks (`PENDING` / `RETRY_WAIT`, `scheduled_at <= now`).
2. Claimed task becomes `RUNNING`.
3. `REPL_TO_EC` processor:
   1. loads snapshot
   2. rejects stale version task
   3. marks object `MIGRATING`
   4. reads HOT bytes
   5. encodes shards
   6. writes shards
   7. promotes version tier to `EC` and object state to `EC_ACTIVE`
   8. enqueues replication GC task
4. Success path marks task `DONE`.
5. Failure path marks `RETRY_WAIT` with backoff, or `FAILED` at retry cap.

Code:

1. `internal/tiering/worker.go`
2. `internal/meta/tikv_store_tasks.go`
3. `internal/tiering/repl_to_ec_processor.go`
4. `internal/meta/tikv_store_migration.go`

## 3. Strategy A: Age Baseline

Entry point:

1. `EnqueueTieringCandidatesStrategyA` -> `enqueueTieringCandidates(..., applyByteBudget=false)`
2. Code: `internal/meta/tikv_store_policy.go`

Behavior:

1. Uses age-eligible due-index candidates.
2. Sorts by older `UpdatedAt` first.
3. Tie-break uses larger object first.
4. Caps by `MAX_OBJECTS_PER_ROUND`.
5. Does not apply `MAX_BYTES_PER_ROUND`.

## 4. Strategy B: Static Throttling

Entry point:

1. `EnqueueTieringCandidatesStrategyB` -> `enqueueTieringCandidates(..., applyByteBudget=true)`
2. Code: `internal/meta/tikv_store_policy.go`

Behavior:

1. Same candidate source and validation as strategy A.
2. Same sorting as strategy A.
3. Applies both:
   1. object-count cap (`MAX_OBJECTS_PER_ROUND`)
   2. byte budget cap (`MAX_BYTES_PER_ROUND`)
4. If adding one candidate exceeds byte budget, that candidate is skipped.

## 5. Strategy C: Idle-Window Admission + Static Throttling

Entry points:

1. Gate: `strategyCGatePass` in scanner
2. Enqueue: `EnqueueTieringCandidatesStrategyC` -> same byte-budgeted selector as B
3. Code:
   1. `internal/tiering/policy_scanner.go`
   2. `internal/meta/tikv_store_policy.go`

Behavior:

1. Before enqueue, scanner requires idle-window gate pass.
2. Gate pass requires:
   1. at least one live node heartbeat
   2. no live node exceeds any idle threshold:
      1. `TIERING_IDLE_CPU_PCT`
      2. `TIERING_IDLE_QUEUE_DEPTH`
      3. `TIERING_IDLE_IOWAIT_PCT`
      4. `TIERING_IDLE_MEMORY_PCT`
   3. idle condition must hold for `TIERING_IDLE_STABLE_ROUNDS` consecutive samples
   4. cooldown must pass: `TIERING_THRESHOLD_COOLDOWN_SEC`
3. Once gate passes, candidate selection uses same B logic (`maxObjects` + `maxBytes`).

### 5.1 What happens when gate is false

1. No tiering candidates are enqueued in that pass.
2. `tdue/*` entries remain for future passes.
3. idle stable counter resets when busy sample appears.
4. scanner retries on next trigger tick.
5. in current implementation, repair enqueue and old-version-gc enqueue are also skipped in that pass because strategy-C gate returns before those stages.

## 6. Trigger Modes: periodic vs threshold vs hybrid

Entry:

1. `PolicyScanner.Run` in `internal/tiering/policy_scanner.go`

Modes:

1. `periodic`
   1. runs policy loop every `TIERING_PERIOD_SEC`
2. `threshold`
   1. runs threshold tick every `TIERING_THRESHOLD_CHECK_SEC`
   2. for non-C variants, threshold loop uses idle-window checks + cooldown before running policy pass
   3. for C variant, threshold tick is wake-up source and C gate still executes inside policy pass
3. `hybrid`
   1. runs both periodic and threshold ticks

## 7. Shared vs Different (A/B/C)

Shared:

1. same PUT and due-index insertion path
2. same due-index candidate source
3. same metadata validity checks
4. same deterministic task id and task state machine
5. same worker execution (`REPL_TO_EC`, retry/backoff/fail)

Different:

1. A vs B:
   1. A: object cap only
   2. B: object cap + byte cap
2. C vs B:
   1. C adds idle-window gate before enqueue
   2. enqueue logic after gate is same as B

## 8. Current Implementation Notes

1. `HOT_PRESSURE_DISK_PCT` and `HOT_PRESSURE_QUEUE_DEPTH` are configuration inputs but not active trigger conditions in current scanner code path.
2. Worker claim currently iterates task records under `task/*` and then filters/sorts in memory.
3. due-index scanning avoids full object-table scan for tiering candidate discovery.

## 9. Exact Files to Read in Order

1. `cmd/tiering_worker/main.go`
2. `internal/tiering/policy_scanner.go`
3. `internal/meta/tikv_store_policy.go`
4. `internal/meta/tikv_store_due_index.go`
5. `internal/meta/tikv_store_tasks.go`
6. `internal/tiering/repl_to_ec_processor.go`
7. `internal/meta/tikv_store_migration.go`
