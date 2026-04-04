# Architecture Index

This repository now tracks architecture in two versions:

1. `docs/ARCHITECTURE_V2_FREEZE.md` (current, authoritative)
   - PostgreSQL-first metadata architecture
   - Tiered hot-write + background EC migration model
   - PostgreSQL heartbeat-based node discovery
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
| Metadata source | `DONE` | PostgreSQL normalized tables only (no etcd fallback in mainline). |
| Node discovery source | `DONE` | PostgreSQL heartbeats only in mainline. |
| Storage node heartbeat to PostgreSQL | `DONE` | `node_heartbeats` upsert from each storage node. |
| Foreground replication metadata placement (`replica_locations`) | `DONE` | replication write now persists successful node placements. |
| Tiering task queue (`tiering_tasks`) | `DONE` | enqueue/claim/retry/done lifecycle with SKIP LOCKED claim. |
| Write-path enqueue for REPL->EC | `DONE` | replication writes enqueue deterministic `repl2ec:{object}:{version}` task IDs. |
| Periodic A1 policy scanner | `DONE` | age-based scan marks `MIGRATION_PENDING` and enqueues tasks. |
| Scanner leader election | `DONE` | PostgreSQL advisory lock gates scanner so only one worker instance enqueues periodic policy tasks. |
| Scanner leader observability | `DONE` | `/v2/admin/leader` + metrics snapshot expose lock owner and heartbeat freshness. |
| Tiering worker loop | `DONE` | worker + processor process `REPL_TO_EC` tasks. |
| Post-promotion HOT GC task flow | `DONE` | `REPL_TO_EC` success enqueues `GC`; worker deletes HOT replicas and marks `replica_locations` as `DELETED`. |
| Task retry cap + terminal failure | `DONE` | worker enforces `TIERING_TASK_MAX_RETRY_COUNT`; over-cap tasks are marked `FAILED` with actionable `last_error`. |
| REPL->EC processor (data + metadata promotion) | `PARTIAL` | core flow works; robustness/edge handling still needs hardening. |
| Admin API `/v2/admin/tasks` | `DONE` | filters, state summary, and action hints included. |
| Admin API task actions (`retry-now`, `cancel`) | `DONE` | manual unstick and cancel endpoints live. |
| Admin API `/v2/admin/nodes` | `DONE` | heartbeat-based node visibility with staleness flag. |
| Admin API `/v2/admin/objects/:id` | `DONE` | object state + current version placement view. |
| Full spec policy variants A2/A3 + threshold trigger | `TODO` | only A1 periodic path is implemented now. |
| Repair/reconciliation worker for missing shards/replicas | `TODO` | legacy healer path removed; v2 repair model still to be implemented. |
| Benchmark one-command reproducible v2 matrix | `PARTIAL` | benchmark assets exist, v2 matrix integration still pending cleanup. |

## Must-Do Backlog (Required)

1. Implement v2 repair/reconciliation worker:
   - replace removed legacy healer capabilities
   - repair missing replicas/shards based on PostgreSQL metadata state
2. Complete policy variants beyond A1:
   - implement A2/A3 and threshold trigger path from spec
   - keep benchmark configs reproducible across variants

## Runtime Components (Current)

1. `api` (`cmd/api`)
   - data plane APIs and admin v2 APIs
   - node discovery watcher (postgres-first)
2. `storage_node_*` (`cmd/storage_node`)
   - blob persistence endpoints (`/store`, `/retrieve/:key`, `/delete/:key`)
   - heartbeat reporting to PostgreSQL
3. `tiering_worker` (`cmd/tiering_worker`)
   - periodic A1 scanner + task worker in one process
   - executes REPL->EC migration and HOT replica GC flow
4. `postgres`
   - authoritative metadata and task store
