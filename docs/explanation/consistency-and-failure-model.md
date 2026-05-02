# Explanation: Consistency and Failure Model

Core references:

1. [Request and Task Lifecycles](put-get-delete-and-task-lifecycles.md)
2. [Tiering Task Path from PUT to Worker Claim](tiering-task-path-from-put-to-worker-claim.md)
3. [Task State Machine Reference](../reference/task-state-machine-reference.md)

## 1. Metadata is the Source of Truth

1. object visibility is controlled by metadata head/version records
2. placement tables are authoritative for physical locations
3. worker actions are validated against current object version

## 2. Foreground Write Consistency

For replication writes:

1. storage writes are attempted in parallel
2. request is ACKed only when write quorum is met
3. metadata commit happens after quorum success
4. partial replica success is marked (`is_dirty=true`) and repair is scanner-enqueued from metadata state

## 3. Version Semantics

1. each successful write generates a new version id (`hot_version`)
2. task ids are version-specific (example: `repl2ec:<object>:<version>`)
3. worker rejects stale tasks where `task.version != current_version`

## 4. Task State Safety

Task states:

1. `PENDING`
2. `RUNNING`
3. `DONE`
4. `RETRY_WAIT`
5. `FAILED`

Safety properties:

1. each transition is explicit and observable
2. transient errors do not drop work silently
3. retry cap prevents infinite retry storms
4. claim transition uses a CAS-style transaction on `task/<task_id>`
5. concurrent `meta_service` replicas should not both successfully claim the same task
6. processing model is still not strict end-to-end `exactly-once`

## 5. Failure Cases

### 5.1 Worker crash during migration

1. task is left unfinished
2. if already marked `RUNNING`, task is not auto-reclaimed by timeout
3. recovery is currently admin-driven (`retry-now` or `cancel`)

### 5.2 Node loss during HOT writes

1. quorum may still succeed
2. object may be marked dirty
3. repair pipeline restores desired replica count

### 5.3 Metadata transient failures

1. API path fails fast if metadata commit cannot complete
2. background path retries via task model

### 5.4 Leader lock loss

1. scanner stops immediately
2. worker retries lock acquisition
3. another worker can resume scanner ownership

## 6. Concurrent Access Semantics

### 6.1 Concurrent write/write on same object id

1. each successful write creates a new version id.
2. object head `current_version` points to the latest committed version.
3. background tasks are version-bound; stale tasks are rejected when `task.version != current_version`.

### 6.2 Concurrent read/write on same object id

1. reads are resolved by metadata head at read time.
2. before new metadata commit, reads return previous current version.
3. after metadata commit, reads switch to the new current version.
4. no partial new-version visibility is exposed through metadata head.

### 6.3 Read/write during REPL->EC migration

1. migration is executed in background and validated against version snapshot.
2. object state transitions (`MIGRATION_PENDING`/`MIGRATING`/`EC_ACTIVE`) are metadata-driven.
3. stale or superseded migration tasks are skipped safely.
4. task claim is transaction-fenced, but correctness still depends on processor idempotency and stale-version checks after crashes/retries.

## 7. Current Boundary Conditions

1. task claim is CAS-style: claim transaction reads `task/<task_id>`, verifies it is still runnable, writes `RUNNING`, deletes runnable indexes, and commits.
2. if another metadata service already claimed the same row, the later transaction sees a conflict or no-longer-runnable row and skips that candidate.
3. this fences duplicate successful claims under normal concurrent `meta_service` races.
4. no automatic timeout reclaim exists for stuck `RUNNING` tasks in current implementation.
5. if a worker crashes after claim, recovery is admin `retry-now` / `cancel`.
6. processors remain idempotent and version-safe because retry/crash recovery is still not strict end-to-end `exactly-once`.
