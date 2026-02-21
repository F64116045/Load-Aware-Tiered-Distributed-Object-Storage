# Architecture v2 Freeze (Stability-First)

Status: Frozen for current implementation phase  
Owner: Project Team  
Last Updated: 2026-02-20

## 1. Purpose

This document freezes the v2 system architecture so implementation, testing, and benchmarking all target the same design baseline.

## 2. Core Differences vs v1

1. Metadata source of truth moves from Etcd to PostgreSQL.
2. Foreground write path is simplified (no synchronous EC in foreground).
3. Replication to EC conversion is executed by background workers.
4. Node discovery can run on PostgreSQL heartbeats (`NODE_DISCOVERY_SOURCE=postgres`).
5. Etcd is treated as optional/compatibility control path in current phase.

## 3. Plane Decomposition

### 3.1 Control Plane

1. API Gateway (`cmd/api`)
2. Metadata DB (PostgreSQL via `internal/meta`)
3. Tiering / Recovery workers (current and planned modules)
4. Optional Etcd control compatibility path

### 3.2 Data Plane

1. Storage Nodes (`cmd/storage_node`)
2. Local Linux filesystem persistence
3. Replication objects + EC shards

### 3.3 Event Plane

1. WAL/Event stream (Redpanda)
2. Used for durability/event workflows in write path

## 4. Component Responsibilities (Frozen)

1. API Gateway:
   - Accepts PUT/GET/DELETE
   - Resolves metadata from PostgreSQL normalized tables first
   - Orchestrates storage strategy path
2. Metadata Store (PostgreSQL):
   - `objects`, `object_versions`, `tiering_tasks`, `node_heartbeats`
   - Transactional state transitions and task consistency
3. Storage Node:
   - Raw blob/chunk persistence
   - Heartbeat reporting to `node_heartbeats`
4. Write Service:
   - Hot path commit + metadata commit
   - Foreground commit path avoids synchronous EC conversion
5. Read Service:
   - HOT: replica read
   - EC: shard fetch + reconstruct

## 5. Metadata and Discovery Sources

### 5.1 Metadata Lookup

1. Primary: PostgreSQL normalized metadata (`objects + object_versions`)
2. Fallback: Etcd compatibility path (current migration phase only)
3. Runtime switch: `META_SOURCE=postgres|etcd|auto`

### 5.2 Node Discovery

1. Primary: PostgreSQL `node_heartbeats`
2. Optional fallback: Etcd `nodes/health/*`
3. Runtime switch: `NODE_DISCOVERY_SOURCE=postgres|etcd|auto`

## 6. Frozen Write/Read/Delete Flows

### 6.1 PUT (Foreground)

1. Validate request + serialize payload
2. Write to hot replicas (quorum/profile rule)
3. Commit metadata transaction in PostgreSQL normalized tables
4. Optional compatibility write path depending on source mode
5. Return ACK

### 6.2 GET

1. Resolve latest metadata version/state
2. `HOT_ACTIVE` / `MIGRATING`: read HOT replica first
3. `EC_ACTIVE`: read shards and reconstruct

### 6.3 DELETE

1. Resolve metadata
2. Remove physical data (replica/chunks based on strategy)
3. Remove normalized metadata rows
4. Remove compatibility metadata where enabled

## 7. State Machine (Frozen)

1. `HOT_ACTIVE`
2. `MIGRATION_PENDING`
3. `MIGRATING`
4. `EC_ACTIVE`
5. `DELETE_PENDING`
6. `DELETED`
7. `ERROR_REPAIR_PENDING`

Transition/consistency rules:

1. Metadata state transition must be transactional.
2. Migration execution must be idempotent by object/version identity.
3. Reads remain available during migration.

## 8. Configuration Surface (Frozen Subset)

1. `META_ENABLED`
2. `META_SOURCE=postgres|etcd|auto`
3. `NODE_DISCOVERY_SOURCE=postgres|etcd|auto`
4. `NODE_HEARTBEAT_INTERVAL_SEC`
5. `NODE_HEARTBEAT_STALE_SEC`
6. `HOT_REPLICA_COUNT`
7. `HOT_WRITE_QUORUM`
8. `EC_K`
9. `EC_M`

## 9. Acceptance Mapping

1. Foreground writes avoid synchronous EC: write path implementation in `internal/writeservice`.
2. Replication to EC conversion is background-driven: worker design in v2 spec.
3. PostgreSQL-first metadata path active: `internal/meta` + API resolution flow.
4. Reproducible benchmarks/observability path preserved: compose + metrics snapshot endpoints.

## 10. Deferred / Out of Freeze Scope

1. Full Etcd removal from all control paths.
2. Dynamic CPU/load-aware node placement policies.
3. Multi-region consistency and routing.
4. Full production hardening of WAL/event semantics.
