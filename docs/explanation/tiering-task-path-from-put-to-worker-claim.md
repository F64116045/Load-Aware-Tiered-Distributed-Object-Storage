# Explanation: Tiering Task Path from PUT to Worker Claim

This document explains one end-to-end path in detail:

1. object `PUT` succeeds
2. metadata and due-index are written
3. tiering task is inserted by scanner from due-index
4. worker claims and executes the task

## 1. Components in This Path

1. API route and write service
2. Metadata repository (`meta_service` + TiKV backend)
3. Policy scanner (leader-only)
4. Tiering worker claim loop
5. REPL_TO_EC processor

## 2. Data Structures and Their Roles

1. Object metadata (`obj/*`, `objv/*`, `repl/*`, `ec/*`)
2. Due-index:
   1. `tdue/*`: main time-ordered candidate records
   2. `tdue_ref/*`: reverse pointers from `(object_id, version)` to `tdue` key
3. Task records (`task/*`): execution state machine (`PENDING/RUNNING/RETRY_WAIT/DONE/FAILED`)

## 3. Step-by-Step Runtime Path

### 3.1 API write and metadata commit

1. `PUT /v2/objects/:id` enters write service (`WriteReplicationWithMetadata`).
2. API writes bytes to selected storage nodes in parallel.
3. API requires hot-write quorum success before metadata commit.
4. API calls `UpsertNormalizedMetadata(objectID, metadata)`.
5. Metadata layer writes:
   1. object head (`current_version`, `state`)
   2. version record
   3. HOT replica placement records
   4. due-index records (`tdue/*` and `tdue_ref/*`)

Code:

1. [`internal/writeservice/writeservice.go`](../../internal/writeservice/writeservice.go)
2. [`internal/meta/tikv_store_objects.go`](../../internal/meta/tikv_store_objects.go)
3. [`internal/meta/tikv_store_due_index.go`](../../internal/meta/tikv_store_due_index.go)

### 3.2 Scanner enqueue path (policy-driven)

Scanner is the task insertion path:

1. scanner runs in policy loop (periodic / threshold / hybrid)
2. scanner loads due candidates from `tdue/*`
3. scanner validates each candidate:
   1. object exists
   2. `candidate.version == object.current_version`
   3. object state is HOT-side state
   4. version tier is `HOT`
4. scanner checks task existence by deterministic task id (`repl2ec:<object>:<version>`)
5. if task not found, scanner inserts task as `PENDING`
6. scanner removes due-index entries for that `(object,version)`
7. scanner updates object state to `MIGRATION_PENDING` when applicable

Code:

1. [`internal/tiering/policy_scanner.go`](../../internal/tiering/policy_scanner.go)
2. [`internal/meta/tikv_store_policy.go`](../../internal/meta/tikv_store_policy.go)
3. [`internal/meta/tikv_store_due_index.go`](../../internal/meta/tikv_store_due_index.go)

### 3.3 Worker claim path

Worker loop:

1. worker calls `ClaimNextTieringTask`
2. claim path first promotes due waiting tasks:
   1. scans `task_wait/*` by `scheduled_at`
   2. moves due tasks (`scheduled_at <= now`) back into runnable index
3. claim then scans `task_ready/*` in key order:
   1. higher priority first (`priority_desc`)
   2. then earlier `scheduled_at`
4. for each candidate:
   1. open a CAS-style metadata transaction
   2. read `task/<task_id>` row inside the transaction
   3. verify row still matches the scanned task type
   4. verify task state is `PENDING` or `RETRY_WAIT`
   5. verify `scheduled_at <= now`
   6. set task state to `RUNNING`
   7. clear terminal/error fields for the new attempt
   8. delete stale `task_ready/*` and `task_wait/*` index entries
   9. commit the transaction
5. if commit conflicts, another metadata service likely claimed the row first; worker skips this candidate and keeps scanning
6. worker dispatches claimed task by type after a successful claim

Code:

1. [`internal/meta/tikv_store_tasks.go`](../../internal/meta/tikv_store_tasks.go) (`ClaimNextTieringTask`)
2. [`internal/meta/tikv_store_task_index.go`](../../internal/meta/tikv_store_task_index.go)
3. [`internal/meta/kvstore/client.go`](../../internal/meta/kvstore/client.go) (`RunInTxn`)
4. [`internal/tiering/worker.go`](../../internal/tiering/worker.go)

### 3.4 Scheduling time semantics (`eligible_at` vs `scheduled_at`)

These are different gates:

1. due-index gate (`tdue.eligible_at`): controls when scanner may enqueue migration work
2. task gate (`task.scheduled_at`): controls when worker may claim queued work

Current behavior:

1. scanner-created migration/repair/gc candidates use `scheduled_at=now`
2. `task_wait/*` is mostly from retry backoff (`RETRY_WAIT`)
3. future `scheduled_at` can also come from direct `EnqueueTieringTask` callers

Budget behavior:

1. If a B/C candidate would exceed `MAX_BYTES_PER_ROUND`, scanner does not create a task for it in that pass.
2. The candidate stays in due-index, so the next scanner policy pass can consider it again.
3. Workers only see tasks after scanner creates `task/*` and `task_ready/*`; byte-budget skips do not enter the worker claim loop.

### 3.5 Delivery guarantee boundary

Current processing model remains `at-least-once`:

1. claim transition uses a TiKV transaction as a CAS fence on `task/<task_id>`
2. two metadata services racing for the same candidate should not both commit `RUNNING`
3. this is still not strict end-to-end exactly-once processing
4. worker crash after successful claim can leave the task stuck in `RUNNING`
5. processors are designed to be idempotent and stale-safe for retries, stale versions, and manual recovery

### 3.6 RUNNING stuck behavior (current)

Current implementation has no automatic `RUNNING` timeout reclaim.

Implication:

1. worker crash after claim can leave task in `RUNNING`
2. operator uses admin `retry-now` / `cancel` for manual recovery

### 3.7 REPL_TO_EC execution and state transitions

Processor steps:

1. load version snapshot for `(object, task.version)`
2. reject stale task if task version is not current version
3. mark object as `MIGRATING`
4. fetch source bytes from active HOT replicas
5. encode EC shards (`k`,`m`)
6. write shards to target nodes
7. promote version tier to `EC`, object state to `EC_ACTIVE`
8. mark task `DONE`
9. enqueue replication GC task

Failure path:

1. transient errors -> `RETRY_WAIT` with backoff
2. retry cap exceeded -> `FAILED`

Code:

1. [`internal/tiering/repl_to_ec_processor.go`](../../internal/tiering/repl_to_ec_processor.go)
2. [`internal/meta/tikv_store_migration.go`](../../internal/meta/tikv_store_migration.go)
3. [`internal/meta/tikv_store_tasks.go`](../../internal/meta/tikv_store_tasks.go)

## 4. Why `tdue_ref/*` Exists

`tdue_ref/*` is a reverse index for deletion and idempotent updates.

Without reverse index, deleting due-index by `(object,version)` would require scanning `tdue/*`.

With reverse index:

1. lookup `tdue_ref/<object>/<version>`
2. get exact `DueKey`
3. delete `DueKey` and `ref` in one batch

Code:

1. [`internal/meta/tikv_store_due_index.go`](../../internal/meta/tikv_store_due_index.go) (`removeTieringDueIndexByVersionInBatch`)

## 5. Current Performance Characteristics

1. Due-index scan is prefix-ordered and stops at first future eligibility or scan cap.
2. Worker claim first uses runnable indexes (`task_ready/*`, `task_wait/*`) instead of broad `task/*` scan.
3. Current optimization already avoids object-table full scan for tiering candidate selection.
4. Waiting tasks are promoted from `task_wait/*` to `task_ready/*` when `scheduled_at <= now`.

## 6. Operational Implications

1. If scanner is down, HOT objects can accumulate because enqueue path is paused.
2. If worker is down, tasks accumulate in `PENDING/RETRY_WAIT`.
3. Admin endpoints should be checked together:
   1. due-index stats
   2. task state counts
   3. leader status

## 7. Minimal Troubleshooting Procedure

1. Confirm object metadata exists and state/version are sane.
2. Check due-index stats for ready candidates.
3. Check leader status for scanner ownership.
4. Check task state counts (`PENDING/RUNNING/RETRY_WAIT/FAILED`).
5. Inspect worker logs for claim and processor errors.
