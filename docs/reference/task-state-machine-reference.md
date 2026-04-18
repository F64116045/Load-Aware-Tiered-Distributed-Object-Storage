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

## 4. Stale Version Protection

Processors compare:

1. `task.version`
2. `object.current_version`

If mismatch, task is treated stale and safely skipped to avoid mutating wrong version.

## 5. Admin Actions

1. `POST /v2/admin/tasks/:id/retry-now`
2. `POST /v2/admin/tasks/:id/cancel`

Use cases:

1. manual recovery after transient infrastructure fault
2. aborting known-bad task sequence
