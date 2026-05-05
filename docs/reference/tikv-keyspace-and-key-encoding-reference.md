# Metadata Keyspace Reference (TiKV)

Source:

1. [`internal/meta/tikv_store_schema.go`](../../internal/meta/tikv_store_schema.go)
2. [`internal/meta/tikv_store_keys.go`](../../internal/meta/tikv_store_keys.go)
3. [`internal/meta/tikv_store_task_index.go`](../../internal/meta/tikv_store_task_index.go)
4. [`internal/meta/kvstore/client.go`](../../internal/meta/kvstore/client.go)

## 1. Prefix Catalog

| Prefix | Purpose |
| --- | --- |
| `obj/` | object head record |
| `objv/` | object version record |
| `repl/` | replica placements |
| `ec/` | EC shard placements |
| `task/` | task queue records |
| `task_ready/` | runnable task index (priority/schedule ordered) |
| `task_wait/` | not-yet-runnable task index (scheduled-time ordered) |
| `task_terminal/` | terminal task index (finished-at ordered) |
| `hb/` | node heartbeat records |
| `leader/` | leader observability record |
| `leader_lock/` | leader lock lease value |
| `tdue/` | due-index main records |
| `tdue_ref/` | due-index reverse references |

## 2. Key Shapes

1. `obj/<object_id>`
2. `objv/<object_id>/<version_20d>`
3. `repl/<object_id>/<version_20d>/<node_id>`
4. `ec/<object_id>/<version_20d>/<shard_10d>`
5. `task/<task_id>`
6. `task_ready/<priority_desc_10d>/<scheduled_at_unixnano_20d>/<task_type>/<task_id>`
7. `task_wait/<scheduled_at_unixnano_20d>/<task_type>/<task_id>`
8. `task_terminal/<finished_at_unixnano_20d>/<task_id>`
9. `hb/<node_id>`
10. `leader/<lock_key>`
11. `leader_lock/<lock_key>`
12. `tdue/<eligible_at_unixnano_20d>/<object_id>/<version_20d>`
13. `tdue_ref/<object_id>/<version_20d>`

## 3. Numeric Encoding Rules

1. int64 key component format: `%020d`
2. int key component format: `%010d`
3. fixed-width decimal preserves lexical ordering for range scans

## 4. Due Index Ordering

`tdue` key starts with `eligible_at` encoded integer, so scanning by prefix yields due candidates in time order.

## 5. Leader Lock Value

`leader_lock/<key>` value stores lease owner and expiration timestamp.

Logical fields:

1. owner id
2. expires-at unix nano

Used by:

1. `TryAcquireLockWithTTL`
2. `RefreshLock`
3. `ReleaseLock`
4. `IsLockOwner`

## 6. Value Data Model (By Prefix)

| Prefix | Value model |
| --- | --- |
| `obj/` | `tiKVObjectRecord` |
| `objv/` | `tiKVObjectVersionRecord` |
| `repl/` | `tiKVReplicaRecord` |
| `ec/` | `tiKVECShardRecord` |
| `task/` | `tiKVTaskRecord` |
| `task_ready/` | index marker byte (`0x01`) |
| `task_wait/` | index marker byte (`0x01`) |
| `task_terminal/` | index marker byte (`0x01`) |
| `hb/` | `NodeHeartbeatSnapshot` |
| `leader/` | `TieringLeaderState` |
| `leader_lock/` | lease payload JSON (`owner`, `expires_at_unix_nano`) |
| `tdue/` | `tiKVTierDueRecord` |
| `tdue_ref/` | `tiKVTierDueRefRecord` |

Field-level definitions:

1. [Metadata Keyspace Data Model Walkthrough](metadata-keyspace-data-model-walkthrough.md)
2. [Metadata Record Schema Reference](metadata-record-schema-reference.md)
3. [Logical Data Schema Reference](logical-data-schema-reference.md)
