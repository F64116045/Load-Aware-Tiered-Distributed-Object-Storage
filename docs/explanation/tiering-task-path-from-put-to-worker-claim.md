# Explanation: Tiering Task Path from PUT to Worker Claim

This document explains one end-to-end path in detail:

1. object `PUT` succeeds
2. metadata and due-index are written
3. tiering task is inserted (directly or by scanner)
4. worker claims and executes the task

## 1. Components in This Path

1. API route and write service
2. Metadata repository (`meta_service` + TiKV backend)
3. Policy scanner (leader-only)
4. Tiering worker claim loop
5. REPL_TO_EC processor

## 2. Data Structures and Their Roles

1. Object metadata (`obj/*`, `objv/*`, `repl/*`, `ecs/*`)
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

1. `internal/writeservice/writeservice.go`
2. `internal/meta/tikv_store_objects.go`
3. `internal/meta/tikv_store_due_index.go`

### 3.2 Direct enqueue on write (best effort)

1. After metadata commit, write service tries to enqueue `REPL_TO_EC` task.
2. If partial replica write happened, it also tries enqueue `REPAIR`.
3. If enqueue fails, API still returns success for foreground write.

This is intentional:

1. foreground write availability is prioritized
2. scanner + due-index provides later compensation

Code:

1. `internal/writeservice/writeservice.go` (`finalizeMetadata`, `enqueueTieringTaskIfEligible`)

### 3.3 Scanner compensation path (policy-driven enqueue)

If direct enqueue failed or was disabled, scanner can still enqueue:

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

1. `internal/tiering/policy_scanner.go`
2. `internal/meta/tikv_store_policy.go`
3. `internal/meta/tikv_store_due_index.go`

### 3.4 Worker claim path

Worker loop:

1. reads task records
2. filters runnable tasks:
   1. state in `PENDING` or `RETRY_WAIT`
   2. `scheduled_at <= now`
3. sorts by:
   1. higher priority first
   2. earlier schedule first
4. marks selected task as `RUNNING`
5. dispatches by task type

Code:

1. `internal/meta/tikv_store_tasks.go` (`ClaimNextTieringTask`)
2. `internal/tiering/worker.go`

### 3.5 REPL_TO_EC execution and state transitions

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

1. `internal/tiering/repl_to_ec_processor.go`
2. `internal/meta/tikv_store_migration.go`
3. `internal/meta/tikv_store_tasks.go`

## 4. Why `tdue_ref/*` Exists

`tdue_ref/*` is a reverse index for deletion and idempotent updates.

Without reverse index, deleting due-index by `(object,version)` would require scanning `tdue/*`.

With reverse index:

1. lookup `tdue_ref/<object>/<version>`
2. get exact `DueKey`
3. delete `DueKey` and `ref` in one batch

Code:

1. `internal/meta/tikv_store_due_index.go` (`removeTieringDueIndexByVersionInBatch`)

## 5. Current Performance Characteristics

1. Due-index scan is prefix-ordered and stops at first future eligibility or scan cap.
2. Worker claim currently builds candidate set from task records by prefix iteration.
3. Current optimization already avoids object-table full scan for tiering candidate selection.
4. Next optimization target is a dedicated runnable-task index to avoid broad task scans.

## 6. Operational Implications

1. If direct enqueue fails but metadata commit succeeds, task can still appear later through scanner.
2. If scanner is down, HOT objects can accumulate because compensation path is paused.
3. If worker is down, tasks accumulate in `PENDING/RETRY_WAIT`.
4. Admin endpoints should be checked together:
   1. due-index stats
   2. task state counts
   3. leader status

## 7. Minimal Troubleshooting Checklist

1. Confirm object metadata exists and state/version are sane.
2. Check due-index stats for ready candidates.
3. Check leader status for scanner ownership.
4. Check task state counts (`PENDING/RUNNING/RETRY_WAIT/FAILED`).
5. Inspect worker logs for claim and processor errors.
