# Metadata RPC Reference

This document maps RPC methods to repository operations and runtime callers.

Source of truth:

1. protocol constants: `internal/meta/rpc_protocol.go`
2. server dispatch: `internal/meta/rpc_server.go`
3. repository contract: `internal/meta/repository.go`

## 1. Transport Contract

Endpoint:

1. `POST /meta/rpc`

Auth:

1. `X-Meta-Token` is required when `META_RPC_AUTH_TOKEN` is configured on `meta_service`.
2. The header is a single transport-level shared secret, reused by all RPC methods.
3. When `META_RPC_AUTH_TOKEN` is empty, RPC transport auth is disabled.

Envelope:

1. request: `{ "method": "...", "params": { ... } }`
2. response: `{ "ok": true|false, "result": ..., "error": "..." }`

### 1.1 Token Semantics (`X-Meta-Token` vs Leader Lock Token)

1. `X-Meta-Token`:
   1. HTTP header checked before RPC dispatch.
   2. Same value for all callers/components in one environment.
   3. Used for caller authentication only.
2. Leader lock token:
   1. Returned by `try_acquire_leader_lock`.
   2. Passed in RPC `params.token` for `leader_lock_ping` and `leader_lock_release`.
   3. Encodes lock ownership payload and is HMAC-signed when auth token is configured.
3. These two tokens are different layers and are both valid at the same time.

## 2. Method Groups

## 2.1 Health and Node Heartbeat

| RPC method | Repository call | Main callers |
| --- | --- | --- |
| `ping` | `Ping` | all services during startup/health |
| `upsert_node_heartbeat` | `UpsertNodeHeartbeat` | storage node heartbeat loop |
| `list_healthy_node_ids` | `ListHealthyNodeIDs` | API node discovery, processors |
| `list_node_heartbeats` | `ListNodeHeartbeats` | scanner idle-window checks, admin API |

## 2.2 Leader State and Lock

| RPC method | Repository/lease call | Main callers |
| --- | --- | --- |
| `upsert_tiering_leader_state` | `UpsertTieringLeaderState` | tiering worker leader loop |
| `mark_tiering_leader_stopped` | `MarkTieringLeaderStopped` | tiering worker lock-loss/shutdown |
| `get_tiering_leader_state` | `GetTieringLeaderState` | admin endpoints |
| `try_acquire_leader_lock` | `TryAcquireLeaderLease` + token issuance | tiering worker leader election |
| `leader_lock_ping` | `RefreshLeaderLease` | lock owner heartbeat |
| `leader_lock_release` | `ReleaseLeaderLease` | clean leadership release |

## 2.3 Object Metadata

| RPC method | Repository call | Main callers |
| --- | --- | --- |
| `upsert_normalized_metadata` | `UpsertNormalizedMetadata` | API write finalize |
| `get_normalized_metadata` | `GetNormalizedMetadata` | API GET/DELETE |
| `delete_normalized_metadata` | `DeleteNormalizedMetadata` | API DELETE |
| `get_object_admin_view` | `GetObjectAdminView` | admin object endpoint, EC repair |
| `get_tiering_index_stats` | `GetTieringIndexStats` | admin metrics endpoint |

## 2.4 Task Queue and Candidate Enqueue

| RPC method | Repository call | Main callers |
| --- | --- | --- |
| `enqueue_tiering_task` | `EnqueueTieringTask` | processors (for follow-up tasks such as GC) |
| `list_tiering_tasks` | `ListTieringTasks` | admin tasks endpoint |
| `list_tiering_task_state_counts` | `ListTieringTaskStateCounts` | admin metrics/tasks |
| `requeue_tiering_task_now` | `RequeueTieringTaskNow` | admin retry-now |
| `cancel_tiering_task` | `CancelTieringTask` | admin cancel |
| `claim_next_tiering_task` | `ClaimNextTieringTask` | worker loop |
| `mark_tiering_task_done` | `MarkTieringTaskDone` | worker success path |
| `mark_tiering_task_retry` | `MarkTieringTaskRetry` | worker retry path |
| `mark_tiering_task_failed` | `MarkTieringTaskFailed` | worker terminal failure |
| `purge_terminal_tiering_tasks` | `PurgeTerminalTieringTasks` | scanner task-history reaper |
| `enqueue_tiering_candidates_strategy_a` | `EnqueueTieringCandidatesStrategyA` | scanner |
| `enqueue_tiering_candidates_strategy_b` | `EnqueueTieringCandidatesStrategyB` | scanner |
| `enqueue_tiering_candidates_strategy_c` | `EnqueueTieringCandidatesStrategyC` | scanner |
| `enqueue_repair_candidates` | `EnqueueRepairCandidates` | scanner |
| `enqueue_old_version_gc_candidates` | `EnqueueOldVersionGCCandidates` | scanner |

## 2.5 Migration/Repair Metadata Operations

| RPC method | Repository call | Main callers |
| --- | --- | --- |
| `get_object_version_snapshot` | `GetObjectVersionSnapshot` | REPL_TO_EC, REPAIR processor |
| `get_object_version_gc_view` | `GetObjectVersionGCView` | old-version GC processor |
| `purge_object_version_metadata` | `PurgeObjectVersionMetadata` | old-version GC processor |
| `mark_object_migrating` | `MarkObjectMigrating` | REPL_TO_EC processor |
| `promote_object_version_to_ec` | `PromoteObjectVersionToEC` | REPL_TO_EC, EC repair processor |
| `list_active_replica_locations` | `ListActiveReplicaLocations` | REPL_TO_EC, REPAIR processor |
| `upsert_replica_locations` | `UpsertReplicaLocations` | HOT repair processor |
| `mark_replica_locations_deleted` | `MarkReplicaLocationsDeleted` | replication GC processor |

## 3. Error Behavior

1. server returns `ok=false` with message in `error`.
2. caller decides retry/fail policy.
3. worker path converts processor errors into task retry/fail transitions.

## 4. Compatibility Notes

1. leader lock token is statelessly encoded and verified by HMAC in RPC server.
2. lock ownership state is persisted in metadata backend, not process memory.
