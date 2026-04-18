# Metadata Keyspace Reference (TiKV)

Source:

1. `internal/meta/tikv_store_schema.go`
2. `internal/meta/tikv_store_keys.go`

## 1. Prefix Catalog

| Prefix | Purpose |
| --- | --- |
| `obj/` | object head record |
| `objv/` | object version record |
| `repl/` | replica placements |
| `ec/` | EC shard placements |
| `task/` | task queue records |
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
6. `hb/<node_id>`
7. `leader/<lock_key>`
8. `leader_lock/<lock_key>`
9. `tdue/<eligible_at_unixnano_20d>/<object_id>/<version_20d>`
10. `tdue_ref/<object_id>/<version_20d>`

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
