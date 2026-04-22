# Configuration Reference

Source of truth:

1. `internal/config/config.go`
2. service-specific startup files in `cmd/*`

## 1. Metadata Connectivity

| Variable | Default | Meaning |
| --- | --- | --- |
| `META_ENABLED` | `false` | Enable metadata integration |
| `META_ENDPOINT` | empty | RPC endpoint to `meta_service` |
| `META_REQUIRE_ENDPOINT` | `false` | Fail when endpoint is required but missing |
| `META_RPC_AUTH_TOKEN` | empty | shared token for metadata RPC |
| `META_DSN` | empty | TiKV PD address list for direct backend mode |

## 2. Node Discovery and Heartbeat

| Variable | Default | Meaning |
| --- | --- | --- |
| `NODE_HEARTBEAT_INTERVAL_SEC` | `3` | storage node heartbeat publish interval |
| `NODE_HEARTBEAT_STALE_SEC` | `15` | staleness threshold for healthy node filtering |
| `NODE_NAMES_CSV` | predefined 6 nodes | expected node ids |

## 3. Write Path

| Variable | Default | Meaning |
| --- | --- | --- |
| `HOT_REPLICA_COUNT` | `3` | number of target HOT replicas |
| `HOT_WRITE_QUORUM` | `2` | minimum successful writes before ACK |

## 4. Policy Variant and Trigger Mode

| Variable | Default | Allowed |
| --- | --- | --- |
| `TIERING_POLICY_VARIANT` | `A` | `A`, `B`, `C` |
| `TIERING_TRIGGER_MODE` | `periodic` | `periodic`, `threshold`, `hybrid` |
| `TIERING_PERIOD_SEC` | `300` | periodic scan interval |
| `TIERING_POLICY_PERIOD_SEC` | fallback to `TIERING_PERIOD_SEC` | worker runtime override for scanner periodic interval |
| `TIERING_THRESHOLD_CHECK_SEC` | `10` | threshold sampling interval |
| `TIERING_THRESHOLD_COOLDOWN_SEC` | `60` | cooldown after threshold trigger |
| `THRESHOLD_COOLDOWN_SEC` | fallback alias | compatibility alias used when `TIERING_THRESHOLD_COOLDOWN_SEC` is unset |

## 5. Idle Window and Pressure Thresholds

| Variable | Default | Meaning |
| --- | --- | --- |
| `TIERING_IDLE_STABLE_ROUNDS` | `3` | N consecutive rounds required for idle window |
| `TIERING_IDLE_CPU_PCT` | `70` | max cpu load for idle |
| `TIERING_IDLE_MEMORY_PCT` | `80` | max memory used for idle |
| `TIERING_IDLE_IOWAIT_PCT` | `20` | max disk iowait for idle |
| `TIERING_IDLE_QUEUE_DEPTH` | `16` | max io queue depth for idle |
| `HOT_PRESSURE_DISK_PCT` | `80` | disk pressure trigger |
| `HOT_PRESSURE_QUEUE_DEPTH` | `1000` | queue-depth pressure trigger |

## 6. Candidate Budgets

| Variable | Default | Meaning |
| --- | --- | --- |
| `AGE_THRESHOLD_SEC` | `3600` | minimum HOT age before migration candidate is eligible |
| `MAX_OBJECTS_PER_ROUND` | `200` | max selected objects per round |
| `MAX_BYTES_PER_ROUND` | `1073741824` | per-round byte cap used by strategy B/C |
| `TIERING_DUE_INDEX_MAX_SCAN` | `2000` | due-index scan cap |
| `TIERING_DUE_INDEX_BURST_ROUNDS` | `4` | max due-index scan bursts per policy pass |
| `TIERING_DUE_INDEX_ADAPTIVE_MAX_SCAN` | `20000` | adaptive upper bound for due-index scan window |

## 7. Worker and Retry Controls

| Variable | Default | Meaning |
| --- | --- | --- |
| `TIERING_WORKER_POLL_SEC` | `2` | task polling interval |
| `TIERING_WORKER_ID` | hostname | worker identity used in leader-state records |
| `TIERING_WORKER_TASK_TYPE` | empty/ALL | optional task type filter |
| `TIERING_TASK_MAX_RETRY_COUNT` | `8` | retry cap before FAILED |
| `TIERING_TASK_WAIT_PROMOTE_BASE` | `256` | wait-index promote batch size per claim |
| `TIERING_TASK_WAIT_PROMOTE_BURST_ROUNDS` | `4` | max promote bursts per claim call |
| `TIERING_TASK_WAIT_PROMOTE_ADAPTIVE_MAX` | `4096` | adaptive upper bound for promote batch size |
| `WORKER_BW_LIMIT_MBPS` | `0` | optional migration bandwidth cap |

## 8. Leader Lock / Scanner Controls

| Variable | Default | Meaning |
| --- | --- | --- |
| `TIERING_POLICY_LEADER_LOCK_KEY` | `42042` | lock id for scanner election |
| `TIERING_POLICY_LEADER_RETRY_SEC` | `2` | retry interval for lock acquisition |
| `TIERING_LEADER_STALE_SEC` | `10` | stale threshold for leader heartbeat |

## 9. Repair and Old-Version Reaper

| Variable | Default | Meaning |
| --- | --- | --- |
| `REPAIR_SCAN_ENABLED` | `true` | enable repair candidate scan |
| `REPAIR_MAX_OBJECTS_PER_ROUND` | `200` | repair enqueue cap |
| `OLD_VERSION_REAPER_ENABLED` | `true` | enable old-version GC candidate scan |
| `OLD_VERSION_RETENTION_COUNT` | `2` | keep latest N versions |
| `OLD_VERSION_RETENTION_AGE_SEC` | `86400` | keep versions newer than this age |
| `OLD_VERSION_REAPER_MAX_TASKS_PER_ROUND` | `200` | old-version GC enqueue cap |
| `TIERING_TASK_HISTORY_REAPER_ENABLED` | `true` | enable terminal task history cleanup |
| `TIERING_TASK_HISTORY_RETENTION_SEC` | `604800` | retain terminal tasks newer than this age |
| `TIERING_TASK_HISTORY_REAPER_MAX_TASKS_PER_ROUND` | `200` | max terminal tasks purged per reaper run |
| `TIERING_TASK_HISTORY_REAPER_INTERVAL_SEC` | `900` | minimum interval between task-history reaper runs |

## 10. meta_service Startup Hardening

| Variable | Default | Meaning |
| --- | --- | --- |
| `META_SERVICE_PORT` | `8091` | bind port for `meta_service` process |
| `META_STARTUP_PING_TIMEOUT_SEC` | `5` | single ping timeout |
| `META_STARTUP_MAX_WAIT_SEC` | `300` | total wait budget |
| `META_STARTUP_RETRY_INTERVAL_SEC` | `2` | initial retry interval |
| `META_STARTUP_MAX_RETRY_INTERVAL_SEC` | `15` | retry interval cap |
| `META_HEALTH_PING_TIMEOUT_SEC` | `5` | readiness probe ping timeout |

## 11. Storage Node Process Variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `NODE_PORT` | none (required) | storage node listen port |
| `NODE_NAME` | none (required) | storage node id and internal address host |
| `STORAGE_DIR` | none (required) | local blob root directory |
