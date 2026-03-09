# Architecture Index

This repository now tracks architecture in two versions:

1. `docs/ARCHITECTURE_V2_FREEZE.md` (current, authoritative)
   - PostgreSQL-first metadata architecture
   - Tiered hot-write + background EC migration model
   - Node discovery source switch (`postgres|etcd|auto`)
2. `docs/ARCHITECTURE_V1_LEGACY.md` (historical reference)
   - Original log-centric/etcd-heavy architecture used in early prototype stage

For implementation and milestone details, see `docs/DAILY_PROGRESS.md` and `docs/SPEC_V2_LOAD_AWARE_TIERED_OBJECT_STORAGE.md`.

## Current Implementation Snapshot (2026-02-21)

Legend:
- `DONE`: implemented in code and wired into runtime path.
- `PARTIAL`: implemented but still transitional/simplified.
- `TODO`: not implemented yet.

| Area | Status | Notes |
|---|---|---|
| API data plane (`/write`, `/read/:key`, `/delete/:key`) | `DONE` | Routed in `cmd/api/main.go`; write/read/delete paths are active. |
| Metadata primary store (PostgreSQL normalized tables) | `DONE` | `objects` + `object_versions` as main source; runtime no longer depends on `metadata_kv`. |
| Metadata fallback source switch (`META_SOURCE`) | `DONE` | `postgres|etcd|auto` supported. |
| Node discovery source switch (`NODE_DISCOVERY_SOURCE`) | `DONE` | `postgres|etcd|auto` supported; postgres-first default path in compose. |
| Storage node heartbeat to PostgreSQL | `DONE` | `node_heartbeats` upsert from each storage node. |
| Foreground replication metadata placement (`replica_locations`) | `DONE` | replication write now persists successful node placements. |
| Tiering task queue (`tiering_tasks`) | `DONE` | enqueue/claim/retry/done lifecycle with SKIP LOCKED claim. |
| Write-path enqueue for REPL->EC | `DONE` | replication writes enqueue deterministic `repl2ec:{object}:{version}` task IDs. |
| Periodic A1 policy scanner | `DONE` | age-based scan marks `MIGRATION_PENDING` and enqueues tasks. |
| Tiering worker loop | `DONE` | worker + processor process `REPL_TO_EC` tasks. |
| REPL->EC processor (data + metadata promotion) | `PARTIAL` | core flow works; robustness/edge handling still needs hardening. |
| Admin API `/v2/admin/tasks` | `DONE` | filters, state summary, and action hints included. |
| Admin API task actions (`retry-now`, `cancel`) | `DONE` | manual unstick and cancel endpoints live. |
| Admin API `/v2/admin/nodes` | `DONE` | heartbeat-based node visibility with staleness flag. |
| Admin API `/v2/admin/objects/:id` | `DONE` | object state + current version placement view. |
| Full spec policy variants A2/A3 + threshold trigger | `TODO` | only A1 periodic path is implemented now. |
| Repair/reconciliation worker for missing shards/replicas | `TODO` | legacy healer path removed; v2 repair model still to be implemented. |
| Benchmark one-command reproducible v2 matrix | `PARTIAL` | benchmark assets exist, v2 matrix integration still pending cleanup. |

## Runtime Components (Current)

1. `api` (`cmd/api`)
   - data plane APIs and admin v2 APIs
   - node discovery watcher (postgres-first)
2. `storage_node_*` (`cmd/storage_node`)
   - blob persistence endpoints (`/store`, `/retrieve/:key`, `/delete/:key`)
   - heartbeat reporting to PostgreSQL
3. `tiering_worker` (`cmd/tiering_worker`)
   - periodic A1 scanner + task worker in one process
   - executes REPL->EC migration flow
4. `postgres`
   - authoritative metadata and task store
5. `redpanda`
   - WAL/event path still present for existing write flow compatibility
6. `etcd`
   - behind `legacy-etcd` compose profile (non-default path)
