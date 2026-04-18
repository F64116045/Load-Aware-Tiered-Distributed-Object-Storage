# Explanation: Consistency and Failure Model

## 1. Metadata is the Source of Truth

1. object visibility is controlled by metadata head/version records
2. placement tables are authoritative for physical locations
3. worker actions are validated against current object version

## 2. Foreground Write Consistency

For replication writes:

1. storage writes are attempted in parallel
2. request is ACKed only when write quorum is met
3. metadata commit happens after quorum success
4. partial replica success is marked (`is_dirty=true`) and repair task is queued

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

## 5. Failure Cases

### 5.1 Worker crash during migration

1. task is left unfinished
2. task becomes claimable again
3. processing resumes from state machine

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
