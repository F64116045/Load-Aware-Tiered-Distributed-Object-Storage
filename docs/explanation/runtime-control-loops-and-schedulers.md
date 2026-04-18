# Explanation: Runtime Control Loops

The system is mostly asynchronous. These loops are what keep it converging.

## 1. Storage Node Heartbeat Loop

Source:

1. `cmd/storage_node/heartbeat.go`

Behavior:

1. every `NODE_HEARTBEAT_INTERVAL_SEC`, each node pushes heartbeat metadata
2. includes free space, total space, queue depth, cpu/memory/iowait, status

Purpose:

1. API can avoid stale/dead nodes
2. scanner can evaluate pressure and idle windows

## 2. API Node Watch Loop

Source:

1. `cmd/api/main.go` (`watchNodesFromMetadata`)

Behavior:

1. periodically queries healthy node list from metadata
2. refreshes in-memory node pool for write placement

Purpose:

1. dynamic placement without process restart

## 3. Scanner Leader Loop

Source:

1. `cmd/tiering_worker/main.go` (`runScannerAsLeader`)

Behavior:

1. worker instances compete for leader lock on `TIERING_POLICY_LEADER_LOCK_KEY`
2. lock owner starts scanner
3. owner periodically pings lock and updates leader state
4. lock loss stops scanner and retries election

Purpose:

1. enforce single scanner even if many workers run

## 4. Policy Scanner Loop

Source:

1. `internal/tiering/policy_scanner.go`

Behavior:

1. trigger by periodic and/or threshold/hybrid mode
2. enqueue tiering candidates via A/B/C policy
3. optionally enqueue repair candidates
4. optionally enqueue old-version GC candidates

## 5. Worker Poll Loop

Source:

1. `internal/tiering/worker.go`

Behavior:

1. claim next task (`ClaimNextTieringTask`)
2. dispatch by type (`REPL_TO_EC`, `REPAIR`, `GC`, `GC_OLD_VERSION`)
3. update task state to `DONE`, `RETRY_WAIT`, or `FAILED`
4. enforce retry cap with backoff

## 6. Why Loops Instead of Synchronous Pipeline

1. foreground latency stays predictable
2. failures are retriable/idempotent in task state model
3. policy experiments become configurable without rewriting request path
