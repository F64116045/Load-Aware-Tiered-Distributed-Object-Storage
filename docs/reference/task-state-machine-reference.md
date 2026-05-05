# Task State Machine Reference

Applies to all async task types:

1. `REPL_TO_EC`
2. `REPAIR`
3. `GC`
4. `GC_OLD_VERSION`

## 1. States

1. `PENDING`: waiting for schedule time and claim.
2. `RUNNING`: claimed by a worker.
3. `DONE`: completed successfully.
4. `RETRY_WAIT`: failed transiently, waiting next retry time.
5. `FAILED`: terminal failure (retry cap or explicit cancel path).

## 2. Transitions

1. enqueue -> `PENDING`
2. claim -> `RUNNING`
3. success -> `DONE`
4. failure under retry cap -> `RETRY_WAIT`
5. retry claim -> `RUNNING`
6. retry cap reached -> `FAILED`
7. admin cancel -> `FAILED`
8. admin retry-now -> `PENDING`

## 3. Retry/Backoff Logic

Implemented in worker loop:

1. exponential backoff from 2 seconds
2. doubles each retry
3. capped at 5 minutes
4. `TIERING_TASK_MAX_RETRY_COUNT` decides terminal failure threshold

When task enters `RETRY_WAIT`:

1. `scheduled_at` is set to `now + backoff`
2. runnable index moves from `task_ready/*` to `task_wait/*`
3. claim path later promotes due entries back to `task_ready/*` when `scheduled_at <= now`

## 4. Scheduling Gates

There are two different time gates:

1. due-index gate: `tdue.eligible_at` controls when scanner may enqueue a migration task
2. task gate: `task.scheduled_at` controls when worker may claim a queued task

Operationally:

1. scanner-created `REPL_TO_EC` / `REPAIR` / `GC_OLD_VERSION` tasks currently use `scheduled_at=now`
2. most `task_wait/*` entries come from retry backoff (`RETRY_WAIT`)
3. external callers can still create future-scheduled tasks via `EnqueueTieringTask(..., scheduled_at)`

## 5. Claim Semantics and Guarantees

Current claim path:

1. promote due waiting tasks (`task_wait/*`) into ready index
2. scan `task_ready/*` in key order (priority-desc, then schedule)
3. for each candidate, run a CAS-style transaction:
   1. read authoritative row `task/<task_id>`
   2. verify row still matches the scanned index task type
   3. verify state is `PENDING` or `RETRY_WAIT`
   4. verify `scheduled_at <= now`
   5. write task row as `RUNNING`
   6. delete stale runnable/waiting index rows for that task
   7. commit the transaction
4. if the transaction hits a write conflict, treat that candidate as already claimed and continue scanning

Guarantee boundary:

1. concurrent `meta_service` replicas should not both successfully claim the same task, because both transactions read/write the same `task/<task_id>` row
2. this still is not strict end-to-end `exactly-once` processing
3. if a worker crashes after a successful claim, the task can stay `RUNNING`
4. current recovery for stuck `RUNNING` tasks is admin-driven (`retry-now` or `cancel`)
5. processors still remain idempotent and stale-safe because retries, crashes, and stale task versions are possible

## 6. RUNNING Liveness (Current Behavior)

Current implementation does not have automatic timeout reclaim for long-running/stuck `RUNNING` tasks.

Implication:

1. if worker crashes after claim, task may remain `RUNNING`
2. task is not auto-moved back to `PENDING`/`RETRY_WAIT` by background timeout logic
3. recovery is currently admin-driven: `retry-now` or `cancel`

## 7. Stale Version Protection

Processors compare:

1. `task.version`
2. `object.current_version`

If mismatch, task is treated stale and safely skipped to avoid mutating wrong version.

## 8. Admin Actions

1. `POST /v2/admin/tasks/:id/retry-now`
2. `POST /v2/admin/tasks/:id/cancel`

Use cases:

1. manual recovery after transient infrastructure fault
2. aborting known-bad task sequence

## 9. Related Documents

1. [Tiering Task Path from PUT to Worker Claim](../explanation/tiering-task-path-from-put-to-worker-claim.md)
2. [Consistency and Failure Model](../explanation/consistency-and-failure-model.md)
3. [TiKV Keyspace and Key Encoding Reference](tikv-keyspace-and-key-encoding-reference.md)
