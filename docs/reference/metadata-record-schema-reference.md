# Metadata Schema (TiKV Value Models)

This is the concrete metadata record schema persisted in TiKV values.

Source of truth:

1. [`internal/meta/tikv_store_schema.go`](../../internal/meta/tikv_store_schema.go)

For a worked explanation of how these records combine into read/write/delete
flows, start with [Metadata Keyspace Data Model Walkthrough](metadata-keyspace-data-model-walkthrough.md).

## 1. `tiKVObjectRecord`

```text
object_id: string
tenant_id: string
current_version: int64
state: string
created_at: time
updated_at: time
```

## 2. `tiKVObjectVersionRecord`

```text
object_id: string
version: int64
size_bytes: int64
checksum_sha256: string
tier: string
content_type: *string
encoding_k: *int
encoding_m: *int
created_at: time
```

## 3. `tiKVReplicaRecord`

```text
object_id: string
version: int64
node_id: string
path: string
status: string
```

## 4. `tiKVECShardRecord`

```text
object_id: string
version: int64
shard_index: int
node_id: string
path: string
status: string
```

## 5. `tiKVTaskRecord`

```text
task_id: string
object_id: string
version: int64
task_type: string
task_state: string
priority: int
retry_count: int
last_error: *string
scheduled_at: time
started_at: *time
finished_at: *time
```

## 6. `tiKVTierDueRecord`

```text
object_id: string
version: int64
eligible_at: time
size_bytes: int64
created_at: time
```

## 7. `tiKVTierDueRefRecord`

```text
object_id: string
version: int64
due_key: string
eligible_at: time
updated_at: time
```

## 8. Derived Runtime Views

The repository maps concrete records to admin/runtime structs in [`internal/meta/types.go`](../../internal/meta/types.go):

1. `ObjectAdminView`
2. `TieringTask`
3. `NodeHeartbeatSnapshot`
4. `TieringLeaderState`

## 9. Enum Sources

Current enum-like strings are code-convention based (not centralized in one enum file):

1. object state: `HOT_ACTIVE`, `MIGRATION_PENDING`, `MIGRATING`, `EC_ACTIVE`
2. object tier: `HOT`, `EC`
3. strategy: `replication`, `ec`
4. task types: `REPL_TO_EC`, `REPAIR`, `GC`, `GC_OLD_VERSION`
5. task states: `PENDING`, `RUNNING`, `DONE`, `RETRY_WAIT`, `FAILED`
6. node status: `UP`, `DOWN`
7. leader status: `LEADING`, `STOPPED`, `LOCK_LOST`

## 10. Related Documents

1. [Metadata Keyspace Data Model Walkthrough](metadata-keyspace-data-model-walkthrough.md)
2. [Logical Data Schema Reference](logical-data-schema-reference.md)
3. [TiKV Keyspace and Key Encoding Reference](tikv-keyspace-and-key-encoding-reference.md)
4. [Task State Machine Reference](task-state-machine-reference.md)
