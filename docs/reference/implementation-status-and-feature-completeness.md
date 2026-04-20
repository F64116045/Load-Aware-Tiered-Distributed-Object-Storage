# System Implementation Status and Feature Completeness

Status Snapshot Date: 2026-04-22

This document summarizes what is implemented now, how complete each capability is, and where current scope ends.

Status labels used here:

1. `DONE`: integrated in runtime path and wired end-to-end.
2. `PARTIAL`: implemented with explicit scope limits.
3. `NOT_IMPLEMENTED`: intentionally out of current scope.

## 1. Runtime Component Status

| Component | Status | Notes |
| --- | --- | --- |
| API service (`cmd/api`) | `DONE` | v2 object API + admin routes are active; legacy routes still available. |
| Storage node (`cmd/storage_node`) | `DONE` | durable blob write/read/delete and metadata heartbeat loop implemented. |
| Tiering worker (`cmd/tiering_worker`) | `DONE` | leader lock loop, policy scanner, and task processors integrated. |
| Metadata RPC service (`cmd/meta_service`) | `DONE` | repository-backed `/meta/rpc`, startup readiness wait, health/ready probes. |
| TiKV metadata backend (`internal/meta/tikv_store_*`) | `DONE` | object/version/task/due-index/lock keyspaces implemented. |

## 2. Core Feature Matrix

| Capability | Status | Current behavior |
| --- | --- | --- |
| `PUT /v2/objects/:id` HOT replication write | `DONE` | writes replicas with quorum, commits metadata, writes due-index for scanner. |
| `GET /v2/objects/:id` | `DONE` | dispatches by metadata strategy to replication or EC read path. |
| `DELETE /v2/objects/:id` | `DONE` | deletes physical blobs and normalized metadata records. |
| Foreground-to-background boundary | `DONE` | write path does not directly enqueue `REPL_TO_EC`; scanner enqueues from due-index. |
| REPL -> EC migration task | `DONE` | claim, stale check, encode/write shards, promote metadata, enqueue follow-up GC. |
| REPAIR tasks (HOT/EC) | `DONE` | scanner enqueues from metadata state (`is_dirty`, missing placements); worker repairs placements. |
| Replication GC task | `DONE` | runs after EC promotion to delete obsolete HOT replicas. |
| Old-version GC task | `DONE` | scanner enqueues by retention policy; processor purges older version metadata/blobs. |
| Policy variants A/B/C | `DONE` | A age-only, B budgeted, C idle-window gate + budget. |
| Trigger modes (`periodic`, `threshold`, `hybrid`) | `DONE` | scanner supports all three through policy scanner config. |
| Leader election and lock keepalive | `DONE` | single active scanner via lease + ping/release token flow. |
| Metadata RPC transport auth (`X-Meta-Token`) | `DONE` | shared-secret header check at RPC server when configured. |
| Admin observability (`/health`, `/v2/admin/*`) | `DONE` | nodes, tasks, leader state, object admin view, metrics snapshot exposed. |
| S3-compatible API | `NOT_IMPLEMENTED` | current public API is repository-native `v2` endpoints only. |
| Bucket and ACL semantics | `NOT_IMPLEMENTED` | object model is key-based without bucket/ACL domain model. |

## 3. Completeness by Domain

| Domain | Completeness | Why |
| --- | --- | --- |
| Foreground object lifecycle | `High` | PUT/GET/DELETE paths are complete for current v2 model. |
| Background control plane | `High` | scanner + worker + retry/state machine are end-to-end wired. |
| Metadata durability model | `High` | TiKV keyspace schema, transitions, and admin views are in place. |
| Compatibility surface | `Medium` | legacy routes exist, but modern target is v2 only. |
| Product-facing API breadth | `Low` | no S3, no bucket model, no ACL model yet. |

## 4. How to Run the Current Feature Set

1. start stack: `./scripts/up_stack.sh`
2. basic health: `curl -sS http://127.0.0.1:8000/health`
3. v2 smoke path: `START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh`
4. optional HA smoke path: `START_STACK=false ./scripts/smoke_e2e_v2_tikv_ha.sh`

## 5. Current Design Guarantees

1. PUT ACK requires hot-write quorum before metadata commit.
2. metadata transitions are version-aware; stale tasks are skipped by version mismatch checks.
3. scanner ownership is single-leader by lease lock.
4. task execution follows persisted state machine (`PENDING/RUNNING/RETRY_WAIT/DONE/FAILED`).
5. due-index (`tdue/*` + `tdue_ref/*`) is the candidate source for migration enqueue.

## 6. Key Scope Boundaries

1. metadata backend in main runtime profile is TiKV behind `meta_service` RPC.
2. auth for metadata RPC is shared transport token (`X-Meta-Token`), not per-method credentials.
3. public object API contract is `/v2/objects/:id`, not S3-compatible endpoints.
4. system currently uses a single logical tenant (`tenant_id` defaults to `default` in record creation path).
