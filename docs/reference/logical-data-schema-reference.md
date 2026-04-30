# Data Schema (Field-Level)

This document explains logical data entities, field semantics, enum meanings, and mutation points.

## 1. Object Head Entity

Logical role:

1. stable pointer to current version
2. lifecycle state for current version operations

Fields:

| Field | Type | Meaning | Updated by |
| --- | --- | --- | --- |
| `object_id` | string | object primary identifier | create once |
| `tenant_id` | string | tenant namespace (currently `default`) | create once |
| `current_version` | int64 | active version id | write path |
| `state` | string enum | current lifecycle state | write path / processors |
| `created_at` | timestamp | first creation time | create once |
| `updated_at` | timestamp | last head mutation time | many paths |

State enum (observed in code):

1. `HOT_ACTIVE`
2. `MIGRATION_PENDING`
3. `MIGRATING`
4. `EC_ACTIVE`

## 2. Object Version Entity

Logical role:

1. immutable metadata for one version
2. source for read strategy and integrity fields

Fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `object_id` | string | parent object |
| `version` | int64 | unique version id |
| `size_bytes` | int64 | object payload size |
| `checksum_sha256` | string | checksum (empty if unavailable) |
| `tier` | enum string | `HOT` or `EC` |
| `content_type` | nullable string | MIME type |
| `encoding_k` | nullable int | EC data shards |
| `encoding_m` | nullable int | EC parity shards |
| `created_at` | timestamp | version create time |

Tier enum:

1. `HOT`: read from replica path
2. `EC`: reconstruct from shards

## 3. Replica Placement Entity

Logical role:

1. records where HOT bytes of a version are stored
2. source of truth for HOT reads, deletes, repair, and REPL->EC source fetches

Fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `object_id` | string | object id |
| `version` | int64 | version id |
| `node_id` | string | storage node URL/id |
| `path` | string | blob key used by storage node (`hot/<id>/<version>`) |
| `status` | enum string | `ACTIVE`, `DELETED` |

## 4. EC Shard Placement Entity

Logical role:

1. records where each EC shard is stored

Fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `object_id` | string | object id |
| `version` | int64 | version id |
| `shard_index` | int | shard number |
| `node_id` | string | storage node URL/id |
| `path` | string | shard key, usually `<id>_cold_chunk_<i>` |
| `status` | enum string | shard status, usually `ACTIVE` |

## 5. Task Entity

Logical role:

1. durable async work queue for background control plane

Fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `task_id` | string | deterministic task id |
| `object_id` | string | target object |
| `version` | int64 | target version |
| `task_type` | enum string | task class |
| `task_state` | enum string | current state |
| `priority` | int | higher first |
| `retry_count` | int | retries consumed |
| `last_error` | nullable string | last failure reason |
| `scheduled_at` | timestamp | earliest runnable time |
| `started_at` | nullable timestamp | claim time |
| `finished_at` | nullable timestamp | terminal time |

Task type enum:

1. `REPL_TO_EC`
2. `REPAIR`
3. `GC`
4. `GC_OLD_VERSION`

Task state enum:

1. `PENDING`
2. `RUNNING`
3. `DONE`
4. `RETRY_WAIT`
5. `FAILED`

Task index keys:

1. `task_ready/<priority_desc>/<scheduled_at>/<task_type>/<task_id>` for runnable claim order
2. `task_wait/<scheduled_at>/<task_type>/<task_id>` for future-scheduled runnable tasks
3. `task_terminal/<finished_at>/<task_id>` for terminal-history retention and purge

## 6. Node Heartbeat Entity

Logical role:

1. node liveness and load input for placement/scanner decisions
2. healthy node pool for rendezvous-ranked HOT replica and EC shard target selection

Fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `node_id` | string | storage node id/url |
| `last_seen_at` | timestamp | heartbeat wall-clock |
| `free_bytes` | int64 | free disk |
| `total_bytes` | int64 | total disk |
| `io_queue_depth` | int | storage queue depth |
| `cpu_load` | float64 | cpu load ratio |
| `memory_used_pct` | float64 | memory used % |
| `disk_iowait_pct` | float64 | iowait % |
| `status` | enum string | `UP` or `DOWN` |

Current placement boundary:

1. status and heartbeat staleness decide whether a node is eligible.
2. rendezvous hashing balances eligible nodes per object/version.
3. load fields are used by scanner idle-window admission and observability, not yet as weighted placement penalties.

## 7. Leader State Entity

Logical role:

1. scanner leadership observability record

Fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `lock_key` | int64 | lock identity |
| `leader_id` | string | worker id owning scanner |
| `scanner_status` | string | `LEADING`, `STOPPED`, `LOCK_LOST` |
| `acquired_at` | timestamp | leadership start |
| `last_heartbeat_at` | timestamp | latest leader heartbeat |

## 8. Due Index Entities

### 8.1 Due main record

Fields:

1. `object_id`
2. `version`
3. `eligible_at`
4. `size_bytes`
5. `created_at`

### 8.2 Due reference record

Fields:

1. `object_id`
2. `version`
3. `due_key`
4. `eligible_at`
5. `updated_at`

Purpose:

1. event-driven candidate lookup without full object scan
2. reverse cleanup when object/version changes

## 9. Mutation Matrix (Who updates what)

| Flow | Head | Version | Replica | EC shard | Task | Due index |
| --- | --- | --- | --- | --- | --- | --- |
| PUT replication | create/update | insert | upsert ACTIVE | - | scanner later enqueues REPL/REPAIR | upsert |
| REPL->EC promote | update state | set tier=EC | keep (later GC) | insert ACTIVE | enqueue GC | remove/refreshed |
| REPAIR HOT | - | - | upsert missing ACTIVE | - | update REPAIR | - |
| REPAIR EC | - | maybe k/m/checksum refresh | - | upsert missing ACTIVE | update REPAIR | - |
| GC (replication) | maybe state | - | mark deleted | - | update GC | - |
| GC_OLD_VERSION | no current change | delete old version | delete old | delete old | update GC_OLD_VERSION | remove |

## 10. Related Documents

1. [Metadata Keyspace Data Model Walkthrough](metadata-keyspace-data-model-walkthrough.md)
2. [Metadata Record Schema Reference](metadata-record-schema-reference.md)
3. [TiKV Keyspace and Key Encoding Reference](tikv-keyspace-and-key-encoding-reference.md)
4. [Task State Machine Reference](task-state-machine-reference.md)
