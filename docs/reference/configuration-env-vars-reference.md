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
| `TIERING_ENQUEUE_ON_WRITE` | `true` | enqueue tiering/repair tasks during write finalize |

## 4. Policy Variant and Trigger Mode

| Variable | Default | Allowed |
| --- | --- | --- |
| `TIERING_POLICY_VARIANT` | `A1` | `A1`, `A2`, `A3` |
| `TIERING_TRIGGER_MODE` | `periodic` | `periodic`, `threshold`, `hybrid` |
| `TIERING_PERIOD_SEC` | `300` | periodic scan interval |
| `TIERING_THRESHOLD_CHECK_SEC` | `10` | threshold sampling interval |
| `TIERING_THRESHOLD_COOLDOWN_SEC` | `60` | cooldown after threshold trigger |

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
| `AGE_THRESHOLD_SEC` | `3600` | minimum age for A1/A2/A3 |
| `SIZE_THRESHOLD_BYTES` | `1048576` | A2 size threshold |
| `MAX_OBJECTS_PER_ROUND` | `200` | max selected objects per round |
| `MAX_BYTES_PER_ROUND` | `1073741824` | A3 bytes cap |
| `TIERING_DUE_INDEX_MAX_SCAN` | `2000` | due-index scan cap |

## 7. Worker and Retry Controls

| Variable | Default | Meaning |
| --- | --- | --- |
| `TIERING_WORKER_POLL_SEC` | `2` | task polling interval |
| `TIERING_WORKER_TASK_TYPE` | empty/ALL | optional task type filter |
| `TIERING_TASK_MAX_RETRY_COUNT` | `8` | retry cap before FAILED |
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

## 10. meta_service Startup Hardening

| Variable | Default | Meaning |
| --- | --- | --- |
| `META_STARTUP_PING_TIMEOUT_SEC` | `5` | single ping timeout |
| `META_STARTUP_MAX_WAIT_SEC` | `300` | total wait budget |
| `META_STARTUP_RETRY_INTERVAL_SEC` | `2` | initial retry interval |
| `META_STARTUP_MAX_RETRY_INTERVAL_SEC` | `15` | retry interval cap |
| `META_HEALTH_PING_TIMEOUT_SEC` | `5` | readiness probe ping timeout |
