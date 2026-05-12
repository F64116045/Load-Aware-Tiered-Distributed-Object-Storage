# Metadata Keyspace Data Model Walkthrough

This document explains how metadata records in TiKV fit together during
foreground reads/writes and background tiering.

Source code:

1. [`internal/meta/tikv_store_schema.go`](../../internal/meta/tikv_store_schema.go)
2. [`internal/meta/tikv_store_keys.go`](../../internal/meta/tikv_store_keys.go)
3. [`internal/meta/tikv_store_objects.go`](../../internal/meta/tikv_store_objects.go)
4. [`internal/meta/tikv_store_migration.go`](../../internal/meta/tikv_store_migration.go)
5. [`internal/meta/tikv_store_task_index.go`](../../internal/meta/tikv_store_task_index.go)
6. [`internal/meta/kvstore/client.go`](../../internal/meta/kvstore/client.go)
7. [`internal/readservice/readservice.go`](../../internal/readservice/readservice.go)
8. [`internal/storageops/storageops.go`](../../internal/storageops/storageops.go)
9. [`internal/tiering/repl_to_ec_processor.go`](../../internal/tiering/repl_to_ec_processor.go)

## 1. Mental Model

Metadata is split into three kinds of records:

1. durable facts about objects and versions
2. durable facts about physical placement
3. indexes that make scanner and worker lookups cheap

The high-level relationship is:

```text
obj/<object_id>
  points to current_version

objv/<object_id>/<version>
  describes one version's size, tier, checksum, and EC parameters

repl/<object_id>/<version>/<node_id>
  describes where HOT replica bytes live

ec/<object_id>/<version>/<shard_index>
  describes where EC shards live
```

`obj/` is the current pointer. `objv/` is the version descriptor.
`repl/` and `ec/` are physical placement tables.

## 2. Prefix Catalog

| Prefix | Role | Value |
| --- | --- | --- |
| `obj/` | object head | JSON object head record |
| `objv/` | object version descriptor | JSON object version record |
| `repl/` | HOT replica placement | JSON replica placement record |
| `ec/` | EC shard placement | JSON EC shard placement record |
| `task/` | durable background task | JSON task record |
| `task_ready/` | runnable task index | marker byte |
| `task_wait/` | future scheduled task index | marker byte |
| `task_terminal/` | terminal task history index | marker byte |
| `tdue/` | tiering due-index | JSON due candidate record |
| `tdue_ref/` | due-index reverse reference | JSON due reference record |
| `hb/` | storage node heartbeat | JSON heartbeat snapshot |
| `leader/` | scanner leader observability | JSON leader state |
| `leader_lock/` | scanner leader lock | JSON lease payload |

Indexes such as `task_ready/`, `task_wait/`, `task_terminal/`, and `tdue/`
store only enough data to find candidates efficiently. The authoritative record
usually lives elsewhere.

## 3. Object Head: `obj/`

Key:

```text
obj/<object_id>
```

Example:

```json
{
  "object_id": "demo-001",
  "tenant_id": "default",
  "current_version": 1714300000000000000,
  "state": "HOT_ACTIVE",
  "created_at": "2026-05-05T10:00:00Z",
  "updated_at": "2026-05-05T10:00:00Z"
}
```

Fields:

| Field | Meaning |
| --- | --- |
| `object_id` | user-facing object id |
| `tenant_id` | namespace, currently `default` |
| `current_version` | active version id |
| `state` | lifecycle state of the current object |
| `created_at` | first creation time |
| `updated_at` | latest head mutation time |

Current state values:

1. `HOT_ACTIVE`
2. `MIGRATION_PENDING`
3. `MIGRATING`
4. `EC_ACTIVE`

`obj/` answers:

1. does this object exist?
2. which version is current?
3. what lifecycle state is current work in?

`obj/` does not fully describe how to read bytes. It does not store object
size, checksum, EC parameters, replica paths, or shard paths.

## 4. Object Version: `objv/`

Key:

```text
objv/<object_id>/<version_20d>
```

Example HOT version:

```json
{
  "object_id": "demo-001",
  "version": 1714300000000000000,
  "size_bytes": 1024,
  "checksum_sha256": "",
  "tier": "HOT",
  "content_type": "application/octet-stream",
  "created_at": "2026-05-05T10:00:00Z"
}
```

Example EC version:

```json
{
  "object_id": "demo-001",
  "version": 1714300000000000000,
  "size_bytes": 1024,
  "checksum_sha256": "a5d4...",
  "tier": "EC",
  "content_type": "application/octet-stream",
  "encoding_k": 4,
  "encoding_m": 2,
  "created_at": "2026-05-05T10:00:00Z"
}
```

Fields:

| Field | Meaning |
| --- | --- |
| `object_id` | parent object id |
| `version` | version id |
| `size_bytes` | original object length |
| `checksum_sha256` | checksum of original bytes when available |
| `tier` | storage layout: `HOT` or `EC` |
| `content_type` | response MIME type |
| `encoding_k` | EC data shard count |
| `encoding_m` | EC parity shard count |
| `created_at` | version creation time |

Why `objv/` exists:

1. `obj/` can point to a current version without storing all version facts.
2. background tasks can target a specific immutable version.
3. old-version GC can delete stale versions without touching current head.
4. EC read needs `size_bytes`, `encoding_k`, and `encoding_m`.
5. repair and migration can compare `task.version` with `obj.current_version`.

Important distinction:

```text
obj.state = lifecycle state of current object work
objv.tier = storage layout of a specific version
```

For example, `obj.state=MIGRATING` can still mean foreground reads should use
HOT replicas until promotion commits `obj.state=EC_ACTIVE` and `objv.tier=EC`.

## 5. HOT Replica Placement: `repl/`

Key:

```text
repl/<object_id>/<version_20d>/<node_id>
```

Example:

```json
{
  "object_id": "demo-001",
  "version": 1714300000000000000,
  "node_id": "http://storage_node_1:9001",
  "path": "demo-001_hot_1714300000000000000",
  "status": "ACTIVE"
}
```

Fields:

| Field | Meaning |
| --- | --- |
| `object_id` | object id |
| `version` | version id |
| `node_id` | storage node URL/id |
| `path` | blob key on the storage node |
| `status` | `ACTIVE` or `DELETED` |

Used by:

1. HOT read path
2. REPL_TO_EC source fetch
3. HOT repair
4. replica GC after EC promotion

Normalized metadata exposes active HOT placements as:

```json
{
  "hot_key": "hot/demo-001/01714300000000000000",
  "replica_nodes": [
    "http://storage_node_2:9002",
    "http://storage_node_5:9005"
  ],
  "hot_replicas": [
    {
      "node_id": "http://storage_node_2:9002",
      "path": "hot/demo-001/01714300000000000000",
      "status": "ACTIVE"
    }
  ]
}
```

Foreground HOT reads and deletes prefer these recorded placements. Dynamic node
selection is a fallback for missing legacy placement metadata.

## 6. EC Shard Placement: `ec/`

Key:

```text
ec/<object_id>/<version_20d>/<shard_index_10d>
```

Example:

```json
{
  "object_id": "demo-001",
  "version": 1714300000000000000,
  "shard_index": 2,
  "node_id": "http://storage_node_4:9004",
  "path": "demo-001_cold_chunk_2",
  "status": "ACTIVE"
}
```

Fields:

| Field | Meaning |
| --- | --- |
| `object_id` | object id |
| `version` | version id |
| `shard_index` | Reed-Solomon shard index |
| `node_id` | storage node URL/id |
| `path` | shard blob key on that node |
| `status` | shard status, usually `ACTIVE` |

Used by:

1. EC read path
2. EC repair
3. EC delete
4. admin object view

Current `GET /v2/objects/:id` behavior for EC objects:

1. load `obj/` to find current version
2. load `objv/` to get `k`, `m`, and `size_bytes`
3. load active `ec/` placement rows
4. fetch each shard by its recorded `node_id` and `path`
5. reconstruct when at least `k` shards are available
6. concatenate data shards and truncate to `size_bytes`

This avoids assuming that `ecNodes[i]` always stores shard `i`.

## 7. Task Records: `task/`

Key:

```text
task/<task_id>
```

Example:

```json
{
  "task_id": "repl2ec:demo-001:1714300000000000000",
  "object_id": "demo-001",
  "version": 1714300000000000000,
  "task_type": "REPL_TO_EC",
  "task_state": "PENDING",
  "priority": 100,
  "retry_count": 0,
  "scheduled_at": "2026-05-05T10:10:00Z"
}
```

Task types:

1. `REPL_TO_EC`
2. `REPAIR`
3. `GC`
4. `GC_OLD_VERSION`

Task states:

1. `PENDING`
2. `RUNNING`
3. `RETRY_WAIT`
4. `DONE`
5. `FAILED`

The task row is authoritative. Task indexes are only lookup aids.

## 8. Task Indexes

### 8.1 Runnable index: `task_ready/`

Key:

```text
task_ready/<priority_desc_10d>/<scheduled_at_unixnano_20d>/<task_type>/<task_id>
```

Value:

```text
0x01
```

Purpose:

1. find runnable tasks without scanning `task/`
2. claim higher priority tasks first
3. break ties by earlier schedule time

### 8.2 Waiting index: `task_wait/`

Key:

```text
task_wait/<scheduled_at_unixnano_20d>/<task_type>/<task_id>
```

Purpose:

1. hold retry/future tasks that are not runnable yet
2. promote due entries into `task_ready/` when `scheduled_at <= now`

### 8.3 Terminal index: `task_terminal/`

Key:

```text
task_terminal/<finished_at_unixnano_20d>/<task_id>
```

Purpose:

1. support terminal task history cleanup
2. avoid broad scans when purging old `DONE` / `FAILED` tasks

### 8.4 CAS Claim

Current claim path:

1. scan `task_ready/` for a candidate
2. open one TiKV transaction
3. transaction reads `task/<task_id>`
4. transaction checks state is `PENDING` or `RETRY_WAIT`
5. transaction checks `scheduled_at <= now`
6. transaction writes `task_state=RUNNING`
7. transaction deletes runnable index entries
8. commit

If another `meta_service` claimed the same task first, the transaction can
return a write conflict. That conflict is treated as a claim miss, and the
worker scans for the next candidate.

This protects against duplicate successful claims across metadata service
replicas. The processor logic still remains idempotent because worker crashes
after claim can leave a task in `RUNNING` until admin recovery.

## 9. Tiering Due Index: `tdue/` and `tdue_ref/`

### 9.1 Due main record: `tdue/`

Key:

```text
tdue/<eligible_at_unixnano_20d>/<object_id>/<version_20d>
```

Example:

```json
{
  "object_id": "demo-001",
  "version": 1714300000000000000,
  "eligible_at": "2026-05-05T11:00:00Z",
  "size_bytes": 1024,
  "created_at": "2026-05-05T10:00:00Z"
}
```

Purpose:

1. scanner can read age-eligible objects in time order
2. scanner avoids full `obj/` table scans for tiering candidates
3. policy B/C can apply object and byte budgets from candidate records

### 9.2 Due reverse reference: `tdue_ref/`

Key:

```text
tdue_ref/<object_id>/<version_20d>
```

Example:

```json
{
  "object_id": "demo-001",
  "version": 1714300000000000000,
  "due_key": "tdue/01714300000000000000/demo-001/01714300000000000000",
  "eligible_at": "2026-05-05T11:00:00Z",
  "updated_at": "2026-05-05T10:00:00Z"
}
```

Purpose:

1. delete or update a due entry by `(object_id, version)`
2. avoid scanning `tdue/` when metadata changes or object is deleted

## 10. Heartbeat and Leader Records

### 10.1 Node heartbeat: `hb/`

Key:

```text
hb/<node_id>
```

Example:

```json
{
  "node_id": "http://storage_node_1:9001",
  "last_seen_at": "2026-05-05T10:00:00Z",
  "free_bytes": 1000000000,
  "total_bytes": 2000000000,
  "io_queue_depth": 0,
  "cpu_load": 0.10,
  "memory_used_pct": 42.5,
  "disk_iowait_pct": 1.2,
  "status": "UP"
}
```

Used by:

1. API node discovery
2. worker placement decisions
3. policy-C idle-window admission
4. admin node observability

### 10.2 Leader state: `leader/`

Key:

```text
leader/<lock_key>
```

Example:

```json
{
  "lock_key": 42042,
  "leader_id": "tiering-worker-1",
  "scanner_status": "LEADING",
  "acquired_at": "2026-05-05T10:00:00Z",
  "last_heartbeat_at": "2026-05-05T10:00:03Z"
}
```

Used by admin views to show which worker currently owns scanner duty.

### 10.3 Leader lock: `leader_lock/`

Key:

```text
leader_lock/<lock_key>
```

Example:

```json
{
  "owner": "tiering-worker-1",
  "expires_at_unix_nano": 1714300003000000000
}
```

Used by scanner leader election. Only the lock owner should run the scanner.

## 11. PUT Path: HOT Write

`PUT /v2/objects/:id` writes HOT replicas first.

Metadata mutation:

```text
healthy node set
  filtered by status/staleness from node heartbeats
  ranked per object with rendezvous hashing

obj/<object_id>
  current_version = new version
  state = HOT_ACTIVE

objv/<object_id>/<version>
  tier = HOT
  size_bytes = original payload size
  content_type = request content type

repl/<object_id>/<version>/<node_id>
  one row per successful HOT replica

tdue/<eligible_at>/<object_id>/<version>
tdue_ref/<object_id>/<version>
  scanner candidate indexes
```

No `REPL_TO_EC` task is inserted directly by the foreground write path. The
scanner is the producer of migration tasks.

## 12. GET Path: HOT Object

Read plan:

```text
1. load obj/<object_id>
2. load objv/<object_id>/<current_version>
3. read active repl/<object_id>/<current_version>/* placement rows
4. GET /retrieve/<hot_path> from replica nodes
5. first successful replica response is returned
```

`obj.state` indicates lifecycle state. `objv.tier` and `repl/` rows provide
the actual read plan.

## 13. REPL_TO_EC Migration Path

Scanner enqueue:

```text
1. scan ready tdue/* entries
2. validate object still points to candidate version
3. validate object state is HOT-side state
4. validate objv.tier is HOT
5. insert task/<task_id>
6. insert task_ready/*
7. remove tdue/* and tdue_ref/*
8. set obj.state = MIGRATION_PENDING
```

Worker execution:

```text
1. CAS-claim task into RUNNING
2. verify task.version == obj.current_version
3. mark obj.state = MIGRATING
4. fetch source bytes from repl/* placements
5. split and encode Reed-Solomon shards
6. rank healthy target nodes with rendezvous hashing using object/version
7. write shards to selected storage nodes
8. write ec/* shard placement rows
9. set objv.tier = EC and objv.encoding_k/m
10. set obj.state = EC_ACTIVE
11. enqueue GC task for old HOT replicas
```

## 14. GET Path: EC Object

Read plan:

```text
1. load obj/<object_id>
2. load objv/<object_id>/<current_version>
3. read active ec/<object_id>/<current_version>/* placement rows
4. fetch each shard by its recorded node_id/path
5. reconstruct when at least k shards are available
6. concatenate data shards
7. truncate to objv.size_bytes
8. return bytes
```

Normalized metadata returned to the read service includes:

```json
{
  "strategy": "ec",
  "original_length": 1024,
  "k": 4,
  "m": 2,
  "chunk_prefix": "demo-001_cold_chunk_",
  "ec_shards": [
    {
      "shard_index": 0,
      "node_id": "http://storage_node_1:9001",
      "path": "demo-001_cold_chunk_0",
      "status": "ACTIVE"
    }
  ]
}
```

If `ec_shards` is absent, the read service falls back to the legacy
`ecNodes[i] + chunk_prefix+i` behavior.

## 15. DELETE Path

Foreground delete first removes physical blobs, then deletes metadata.

HOT delete:

```text
1. load metadata
2. delete HOT blob from replica nodes
3. delete obj/, objv/, repl/, ec/, and due-index records for the object
```

EC delete:

```text
1. load metadata
2. if ec_shards exists, delete each shard by recorded node_id/path
3. if ec_shards is absent, fall back to legacy node-order deletion
4. delete obj/, objv/, repl/, ec/, and due-index records for the object
```

The placement-aware EC delete prevents orphan shards after repair or placement
changes.

## 16. Common Questions

### 16.1 Why does `objv/` exist if `obj/` has `state`?

`obj.state` is lifecycle state. It says whether the current object is active,
pending migration, migrating, or EC-active.

`objv.tier` is version storage layout. It says whether one specific version is
read from HOT replicas or reconstructed from EC shards.

`obj/` also does not store:

1. original byte length
2. checksum
3. content type
4. EC `k/m`
5. per-replica paths
6. per-shard paths

Those facts are version-specific and placement-specific, so they belong in
`objv/`, `repl/`, and `ec/`.

### 16.2 Why are task and due records duplicated into indexes?

The indexes are not duplicate authoritative state. They are lookup structures:

1. `tdue/` lets scanner find eligible objects by time.
2. `tdue_ref/` lets updates delete a due record without scanning.
3. `task_ready/` lets workers find runnable tasks by priority/schedule.
4. `task_wait/` keeps future tasks out of runnable scans.
5. `task_terminal/` lets history cleanup purge by finish time.

### 16.3 What is the current consistency boundary?

Task claim uses CAS-style transaction logic to avoid duplicate successful
claims under concurrent metadata service replicas.

Processing is still not strict exactly-once because:

1. a worker can crash after a successful claim
2. `RUNNING` tasks are not automatically timeout-reclaimed
3. admin retry/cancel is the current recovery path
4. processors must remain idempotent and version-safe

## 17. Code-Level Flow Cheat Sheet

| Runtime question | Primary code path | Metadata keys touched |
| --- | --- | --- |
| PUT creates a HOT object | `SelectByRendezvous` -> `WriteReplicationWithMetadata` -> `UpsertNormalizedMetadata` -> `upsertTieringDueIndex` | `obj/`, `objv/`, `repl/`, `tdue/`, `tdue_ref/` |
| GET reads a HOT object | API `loadMetadata` -> `GetNormalizedMetadata` -> `ReadReplication` | `obj/`, `objv/`, `repl/` |
| GET reads an EC object | API `loadMetadata` -> `GetNormalizedMetadata` -> `ReadEC` | `obj/`, `objv/`, `ec/` |
| DELETE removes HOT data | API delete route -> `DeleteReplication` -> `DeleteNormalizedMetadata` | physical HOT blobs, then `obj/`, `objv/`, `repl/`, `ec/`, `tdue/`, `tdue_ref/` |
| DELETE removes EC data | API delete route -> `DeleteEC` -> `DeleteNormalizedMetadata` | physical EC shards by `ec_shards`, then all object metadata |
| Scanner creates migration task | `PolicyScanner.ScanOnce` -> `EnqueueTieringCandidatesStrategy*` -> `EnqueueTieringTask` | `tdue/`, `tdue_ref/`, `task/`, `task_ready/`, `obj/` |
| Worker claims a task | `ClaimNextTieringTask` -> `claimNextTieringTaskFromReadyIndexLocked` -> `tryClaimTaskCAS` -> `RunInTxn` | `task_wait/`, `task_ready/`, `task/` |
| Worker promotes HOT to EC | `ProcessReplicationToEC` -> `MarkObjectMigrating` -> `PromoteObjectVersionToEC` | `obj/`, `objv/`, `repl/`, `ec/`, `task/` |
| Worker retries/fails/completes task | `MarkTieringTaskRetry`, `MarkTieringTaskFailed`, `MarkTieringTaskDone` | `task/`, `task_wait/`, `task_ready/`, `task_terminal/` |

The key point is that read services do not infer placement from object state
alone. They ask metadata for a normalized read plan, and that plan is assembled
from the head row, version row, and placement rows.

## 18. Related Documents

1. [Logical Data Schema Reference](logical-data-schema-reference.md)
2. [Metadata Record Schema Reference](metadata-record-schema-reference.md)
3. [TiKV Keyspace and Key Encoding Reference](tikv-keyspace-and-key-encoding-reference.md)
4. [Task State Machine Reference](task-state-machine-reference.md)
5. [Tiering Task Path from PUT to Worker Claim](../explanation/tiering-task-path-from-put-to-worker-claim.md)
