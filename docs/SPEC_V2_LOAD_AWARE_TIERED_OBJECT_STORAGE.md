# Spec v1.1: Load-Aware Tiered Object Storage (Stability-First)

Status: Draft v1.1  
Owner: Project Team  
Last Updated: 2026-02-18

## Implementation Progress

Daily implementation updates are tracked in `docs/DAILY_PROGRESS.md`.

## 1. Executive Summary

This project evolves the current prototype into a practical and research-valid storage system:

- Practical target: a general object storage system for JSON, images, audio, and video.
- Research target: adaptive Replication <-> Erasure Coding (EC) tiering under resource constraints.

Core design change:

- Remove heavy synchronous control-plane work from the foreground write path.
- Use append-first hot writes (Replication) for low latency.
- Move EC conversion to background workers with load-aware policy.

This keeps the strongest parts of the current codebase (Go services, storage nodes, EC library, healing mindset) while fixing current bottlenecks and weak points (synchronous WAL pressure, etcd write-path dependency, fragile benchmark adapter).

## 2. Problem Statement

### 2.1 Current Pain Points

1. Foreground write path is too heavy.
2. Strong coordination logic is mixed into high-frequency data path.
3. Metadata and recovery path are coupled to components not optimized for sustained write-heavy workloads.
4. Current architecture is excellent for concept demonstration but not robust enough for high-load repeatable experiments.

### 2.2 Research Question

How should a storage system decide when and how to migrate objects from Replication to EC in resource-constrained clusters, while minimizing foreground latency impact and preserving recoverability?

## 3. Scope and Non-Goals

### 3.1 In Scope

1. Generic object storage (binary blobs + optional metadata).
2. Hot tier (Replication) + cold tier (EC).
3. Background conversion with periodic trigger (must-have) and threshold trigger (phase-2).
4. Crash recovery and metadata consistency.
5. End-to-end benchmark reproducibility.

### 3.2 Out of Scope

1. Full document database features (secondary indexes, ad-hoc query language, transactions across object sets).
2. Multi-region consistency protocol.
3. Public-cloud scale auto-sharding.

## 4. System Goals and SLOs

### 4.1 Functional Goals

1. PUT/GET/DELETE for generic objects.
2. Deterministic object state machine (HOT, MIGRATING, EC_ACTIVE, etc.).
3. Idempotent background conversion jobs.
4. Recoverable operations after gateway/worker crashes.

### 4.2 Non-Functional Goals

1. Keep write path simple and predictable.
2. Ensure recovery can converge without manual intervention.
3. Expose metrics for policy analysis.

### 4.3 Target SLOs (single cluster, lab environment)

1. Foreground PUT p95 (hot path) <= 50 ms for 1 MB objects.
2. HOT GET p95 <= 40 ms for 1 MB objects.
3. EC GET p95 <= 150 ms for 1 MB objects.
4. No acknowledged write loss for committed hot writes.
5. Recovery convergence for interrupted migration within configurable retry window (e.g., <= 5 min median).

## 5. Workload Model

### 5.1 Object Types

1. Immutable large media blobs (image/video/audio).
2. Mutable objects (metadata-rich payloads, JSON-like content).
3. Mixed workloads with skewed access distributions.

### 5.2 Access Patterns

1. Hot-write bursts for recent objects.
2. Read-skew (Zipf-like).
3. Temporal cooling (objects become cold over time).

## 6. Technology Selection (v2)

## 6.1 Keep

1. Go as implementation language.
2. Existing storage node process model (HTTP + local disk persistence).
3. Reed-Solomon implementation (`klauspost/reedsolomon`).
4. Docker Compose based reproducible deployment.

### 6.2 Replace / Refactor

1. Metadata primary store: PostgreSQL (instead of etcd as data-path metadata store).
   - Reason: better write throughput, richer indexing, transactional updates.
2. Coordination service:
   - Option A (recommended for course project simplicity): PostgreSQL advisory locks for leader election + heartbeat table.
   - Option B (if needed): keep etcd only for membership/election, not per-object metadata writes.
3. Event/WAL path:
   - Remove mandatory synchronous external WAL in hot write path.
   - Use transactional metadata + outbox table as durable recovery source.
   - Optional async bus (NATS JetStream/Kafka) can be added later for scale-out dispatch.

### 6.3 Stability-First Profile (Default for this semester)

1. Metadata/task queue: PostgreSQL only.
2. Foreground ACK does not depend on external WAL broker.
3. etcd is not required in data-path; if retained, keep it control-only.
4. Tiering trigger default is periodic; threshold mode is phase-2.

### 6.4 Why this is better

1. Reduced moving parts in write-critical path.
2. Better control over consistency with fewer distributed commit boundaries.
3. Easier to debug and benchmark reproducibly in a semester project.

## 7. High-Level Architecture

1. API Gateway
   - Handles PUT/GET/DELETE.
   - Writes to hot tier synchronously.
   - Commits metadata and job records.
2. Metadata Service (PostgreSQL-backed)
   - Object catalog, state machine, node health, task queue/outbox.
3. Storage Nodes
   - Store replicated objects and EC shards.
4. Tiering Worker Pool
   - Consumes migration tasks.
   - Performs Replication -> EC conversion under rate limits.
5. Repair Worker
   - Detects and repairs missing replica/shard artifacts.
6. Policy Engine
   - Periodic trigger logic (must-have).
   - Threshold trigger logic (phase-2).
7. Observability Stack
   - Prometheus metrics + optional Grafana.

## 8. Object State Machine

States:

1. `HOT_ACTIVE`
2. `MIGRATION_PENDING`
3. `MIGRATING`
4. `EC_ACTIVE`
5. `DELETE_PENDING`
6. `DELETED`
7. `ERROR_REPAIR_PENDING`

Transitions:

1. PUT success -> `HOT_ACTIVE`
2. Policy match -> `MIGRATION_PENDING`
3. Worker lock acquired -> `MIGRATING`
4. EC write + metadata commit success -> `EC_ACTIVE`
5. Delete request -> `DELETE_PENDING` -> `DELETED`
6. Any failed migration with partial side effects -> `ERROR_REPAIR_PENDING` -> repaired state

Rules:

1. State transition must be transactional in metadata store.
2. Migration is idempotent by `(object_id, version, attempt_id)`.
3. Reads must remain available during `MIGRATING` (prefer HOT source).

## 9. Data Model (PostgreSQL)

### 9.1 Tables

1. `objects`
   - `object_id` (PK)
   - `tenant_id`
   - `current_version`
   - `state`
   - `created_at`, `updated_at`

2. `object_versions`
   - `object_id`
   - `version`
   - `size_bytes`
   - `checksum_sha256`
   - `tier` (`HOT` or `EC`)
   - `encoding_k`, `encoding_m`
   - `created_at`
   - PK: `(object_id, version)`

3. `replica_locations`
   - `object_id`, `version`, `node_id`, `path`, `status`
   - PK: `(object_id, version, node_id)`

4. `ec_shard_locations`
   - `object_id`, `version`, `shard_index`, `node_id`, `path`, `status`
   - PK: `(object_id, version, shard_index)`

5. `tiering_tasks`
   - `task_id` (PK)
   - `object_id`, `version`
   - `task_type` (`REPL_TO_EC`, `REPAIR`, `GC`)
   - `task_state` (`PENDING`, `RUNNING`, `DONE`, `FAILED`, `RETRY_WAIT`)
   - `priority`
   - `retry_count`
   - `last_error`
   - `scheduled_at`, `started_at`, `finished_at`

6. `node_heartbeats`
   - `node_id` (PK)
   - `last_seen_at`
   - `free_bytes`
   - `io_queue_depth`
   - `cpu_load`
   - `status`

7. `write_journal` (optional, if stronger forensic trace is needed)
   - append-only operation log for audit/recovery diagnostics

### 9.2 Indexes

1. `objects(state, updated_at)`
2. `tiering_tasks(task_state, scheduled_at, priority)`
3. `object_versions(tier, created_at)`
4. `node_heartbeats(status, last_seen_at)`

## 10. API Specification (v2)

### 10.1 PUT `/v2/objects/{object_id}`

Request:

1. binary body
2. optional metadata headers (content type, tags, custom fields)

Response:

1. `200/201`
2. object version
3. committed tier (`HOT`)
4. write latency summary

Semantics:

1. Synchronous: write to hot replicas and commit metadata.
2. Asynchronous: enqueue migration task if policy says eligible.

### 10.2 GET `/v2/objects/{object_id}`

Semantics:

1. Resolve latest committed version.
2. If `HOT_ACTIVE` or `MIGRATING`, read from hot replica first.
3. If `EC_ACTIVE`, reconstruct from shards.

### 10.3 DELETE `/v2/objects/{object_id}`

Semantics:

1. Mark `DELETE_PENDING` in metadata transaction.
2. Enqueue physical cleanup task.
3. Return success when delete intent is committed (or synchronous hard-delete mode via query flag).

### 10.4 Admin APIs

1. `GET /v2/admin/nodes`
2. `GET /v2/admin/tasks`
3. `POST /v2/admin/policy/reload`
4. `GET /v2/admin/metrics-snapshot`

## 11. Write Path Design

Foreground PUT flow:

1. Validate request + compute checksum.
2. Choose hot replica set (e.g., 3 nodes, placement by weighted policy).
3. Parallel write object to replicas.
4. Require write quorum (recommended: 2/3) or strict 3/3 based on profile.
5. Commit metadata transaction:
   - upsert `objects`
   - insert `object_versions` with `tier=HOT`
   - insert `replica_locations`
   - optional enqueue `tiering_tasks` candidate
6. ACK client.

Notes:

1. No synchronous EC encode in foreground.
2. No synchronous external WAL dependency required for ACK.

## 12. Tiering Worker Design

Worker loop:

1. Pull task (`SELECT ... FOR UPDATE SKIP LOCKED`).
2. Transition task `PENDING` -> `RUNNING`.
3. Re-check object state and version.
4. Read source data from healthy hot replica.
5. EC split/encode (`k=4,m=2` default).
6. Write shards to selected nodes.
7. In one metadata transaction:
   - write `ec_shard_locations`
   - update `object_versions.tier = EC`
   - update `objects.state = EC_ACTIVE`
   - mark task `DONE`
8. Optional delayed hot replica GC task.

Failure handling:

1. If shard writes < `k`, mark task `RETRY_WAIT`.
2. If metadata commit fails after shard writes, keep idempotent retry by `(object_id,version)`.
3. Exponential backoff with max retry count and dead-letter state.

## 13. Policy Engine Design

### 13.1 Trigger Strategy A: Periodic

Every `T` minutes:

1. scan HOT objects older than threshold
2. rank by size descending and coldness score
3. enqueue migration tasks within budget

### 13.2 Trigger Strategy B: Pressure/Threshold (Phase-2)

Trigger when any condition is true:

1. hot-tier disk usage > `X%`
2. node write queue depth > `Y`
3. CPU load below migration-safe bound and disk pressure above threshold
4. background window open (time-based)

### 13.3 Policy Score (example)

`score = w1*object_age + w2*object_size + w3*(1-read_frequency) + w4*hot_tier_pressure`

Higher score means higher migration priority.

### 13.4 Periodic Strategy Variants (Ablation in Benchmark)

All variants run on fixed periodic scheduler; only selection policy differs.

1. `A1: Age-based`
   - Migrate objects with `age >= AGE_THRESHOLD`.
2. `A2: Size+Age`
   - Migrate objects with both `age >= AGE_THRESHOLD` and `size >= SIZE_THRESHOLD`.
3. `A3: Budget-limited`
   - For each period, migrate by score but cap by budget (`MAX_OBJECTS_PER_ROUND` or `MAX_BYTES_PER_ROUND`).

## 14. Recovery and Consistency Model

### 14.1 Consistency

1. Metadata is source of truth.
2. Blob data is eventually consistent with metadata via workers.
3. Reads only serve committed versions.

### 14.2 Crash Recovery

1. On API crash during PUT:
   - if metadata transaction not committed, object is not visible.
   - periodic orphan cleaner removes unreferenced blobs.
2. On worker crash during migration:
   - task remains `RUNNING` past lease timeout -> reset to `RETRY_WAIT`.
3. On node crash:
   - heartbeat timeout marks node unavailable.
   - repair tasks rebuild missing replicas/shards.

### 14.3 Idempotency Rules

1. PUT supports idempotency key header (recommended).
2. Task execution keyed by deterministic object/version tuple.
3. Duplicate shard writes must be safe.

## 15. Observability and Benchmarking

### 15.1 Metrics

1. API latency: p50/p95/p99 by endpoint.
2. write quorum success/failure rates.
3. tiering queue depth, task duration, retry counts.
4. EC encode/decode CPU time.
5. storage overhead per tier.
6. repair convergence time.

### 15.2 Benchmark Matrix

Baselines:

1. Replication-only
2. EC-only (synchronous)
3. Tiered-periodic A1 (Age-based)
4. Tiered-periodic A2 (Size+Age)
5. Tiered-periodic A3 (Budget-limited)
6. Tiered-threshold (phase-2, optional in final report)

Workloads:

1. Large media write/read mix
2. Mixed object sizes (64 KB - 16 MB)
3. Hot-to-cold lifecycle
4. Node failure during migration

Outputs:

1. CSV + plotted figures
2. reproducible scripts
3. one-command run entry point

### 15.3 Benchmark Execution Protocol (Must Follow)

1. Fix cluster resources and node counts for all runs.
2. Reset data and metadata before each run.
3. Warmup phase first, then measurement phase.
4. Run each scenario at least 3 times; report mean and variance.
5. Record exact config snapshot with each result file.
6. Keep client concurrency and object size distribution identical across compared scenarios.

## 16. Security and Multi-Tenant Notes (Minimal)

1. tenant-id namespace in object key space.
2. basic auth token in API gateway (optional for course scope).
3. signed checksum verification on read/write.

## 17. Backward Compatibility and Migration from Current Repo

### 17.1 Reuse Existing Modules

1. `cmd/api` remains entrypoint, but write path simplified.
2. `cmd/storage_node` remains data plane.
3. `internal/ec` reused directly.
4. existing benchmark folder reused with updated adapter.

### 17.2 Planned Refactor

1. De-emphasize/remove `internal/mq` from critical write path.
2. Introduce `internal/meta` (PostgreSQL repository + schema migration).
3. Introduce `internal/tiering` worker package.
4. Introduce `internal/policy` scoring and triggers.
5. Add `internal/recovery` reconciliation jobs.

### 17.3 Compatibility Mode

Optional phase where old endpoints remain:

1. `/write`, `/read/:key`, `/delete/:key` mapped to v2 internals.
2. New `/v2/*` added progressively.

## 18. Delivery Plan (16 Weeks)

1. Week 1-2: metadata schema + repository layer + local migration scripts.
2. Week 3-4: new hot write path + quorum + versioning.
3. Week 5-6: basic tiering worker (periodic only).
4. Week 7-8: threshold trigger + policy scoring.
5. Week 9-10: recovery/reconciliation + idempotency hardening.
6. Week 11-12: benchmark harness, reproducibility scripts, regression tests.
7. Week 13-14: tuning and ablation experiments.
8. Week 15-16: report, figures, demo, final freeze.

## 19. Acceptance Criteria

Must-have:

1. Foreground writes no longer perform synchronous EC.
2. Background Replication -> EC conversion works and is recoverable.
3. Tiered-periodic policy is implemented with at least 3 variants (A1/A2/A3).
4. One-command reproducible benchmark run exists.
5. Report includes baseline comparisons and failure experiments.

Nice-to-have:

1. Threshold trigger mode implemented and benchmarked.
2. adaptive dynamic `k,m` by object class.
3. bi-directional tiering (EC -> Replication reheating).
4. cost-aware placement by heterogeneous node capabilities.
5. Heterogeneous media evaluation (HDD vs SSD sensitivity benchmark).

## 20. Risks and Mitigations

1. Risk: Worker-induced latency spikes.
   - Mitigation: strict rate limit + admission control + migration windows.
2. Risk: Metadata hotspot under high load.
   - Mitigation: batching, indexes, connection pooling, partitioning plan.
3. Risk: Complex failure edges in migration.
   - Mitigation: idempotent task protocol + explicit state machine + chaos tests.
4. Risk: Time overrun.
   - Mitigation: freeze must-have scope at week 8 and defer extras.

## 21. Open Questions for Review

1. Quorum policy for HOT writes: 2/3 or 3/3 default?
2. Should hot replicas be kept after EC promotion for read acceleration?
3. Which periodic variant should be default in production profile (A1/A2/A3)?
4. Which pressure metrics should dominate threshold trigger in phase-2?
5. Is etcd kept for election in v2, or fully replaced by DB lock for simplicity?

---

## Appendix A: Suggested Config Surface

1. `HOT_REPLICA_COUNT=3`
2. `HOT_WRITE_QUORUM=2`
3. `EC_K=4`
4. `EC_M=2`
5. `TIERING_MODE=periodic|threshold|hybrid`
6. `TIERING_PERIOD_SEC=300`
7. `HOT_PRESSURE_DISK_PCT=80`
8. `HOT_PRESSURE_QUEUE_DEPTH=1000`
9. `WORKER_MAX_CONCURRENCY=4`
10. `WORKER_BW_LIMIT_MBPS=200`
11. `AGE_THRESHOLD_SEC=3600`
12. `SIZE_THRESHOLD_BYTES=1048576`
13. `MAX_OBJECTS_PER_ROUND=200`
14. `MAX_BYTES_PER_ROUND=1073741824`
15. `META_SOURCE=auto|postgres|etcd`

## Appendix B: Minimal Experiment Figures (Final Report)

1. Foreground write latency vs offered load.
2. Storage overhead vs object age distribution.
3. Throughput impact under worker concurrency sweep.
4. Recovery convergence time after injected failures.

Optional:

1. HDD vs SSD sensitivity figure under identical workload and policy config.
