# Daily Progress Log

This file tracks day-by-day implementation progress.
Architecture and requirements stay in `docs/SPEC_V2_LOAD_AWARE_TIERED_OBJECT_STORAGE.md`.

## 2026-02-18 (Milestone 1 bootstrap)

1. Added `internal/meta` package skeleton:
   - DB store wrapper (`NewStore`, `Ping`, `Close`)
   - embedded SQL migrator (`Up`, `Down`)
2. Added initial schema migration:
   - `objects`, `object_versions`, `replica_locations`, `ec_shard_locations`
   - `tiering_tasks`, `node_heartbeats`, `write_journal`
   - baseline indexes from the spec
3. Added `cmd/meta_migrate` utility for migration `up|down` execution.
4. Added metadata DB env configs in `internal/config/config.go`.
5. Ran formatting and compile validation for new modules:
   - `go test ./internal/meta ./cmd/meta_migrate`

## 2026-02-18 (Milestone 2 metadata wiring)

1. Added PostgreSQL service to `docker-compose.yaml`:
   - `postgres:16-alpine`
   - persistent volume `postgres_data`
   - healthcheck via `pg_isready`
2. Wired API service env vars for metadata DB bootstrap:
   - `META_ENABLED`, `META_DRIVER`, `META_DSN`, pool settings
3. Connected `cmd/api/main.go` to metadata store initialization:
   - bootstrap `meta.NewStore(...)`
   - startup ping check
   - non-fatal failure handling with degraded health
4. Extended `/health` response in API:
   - includes metadata component status (`enabled`, `status`, `driver`, `error`)
5. Registered PostgreSQL driver in `internal/meta/store.go`:
   - `_ \"github.com/lib/pq\"`
6. Added optional metadata auto-migrate startup flow:
   - new config flag `META_AUTO_MIGRATE` (default `false`)
   - API can run `meta.NewMigrator(...).Up()` on boot when enabled
   - `/health` now reports `metadata.auto_migrate`

## 2026-02-19 (Milestone 3 metadata dual-write start)

1. Added migration `000002_metadata_kv`:
   - new table `metadata_kv(meta_key, payload, updated_at)`
2. Added metadata repository helper:
   - `Store.UpsertMetadataKV(...)`
3. Wired write finalization to dual-write metadata:
   - `writeservice.finalizeWALEntry` now writes to PostgreSQL (`metadata_kv`) when `META_ENABLED=true`
   - existing etcd metadata commit path remains active for read-path compatibility

## 2026-02-19 (Milestone 3 metadata read-path transition, step 1)

1. Added PostgreSQL metadata lookup/delete methods:
   - `Store.GetMetadataKV(...)`
   - `Store.DeleteMetadataKV(...)`
2. Updated API metadata resolution flow:
   - new helper `loadMetadata(...)` in `cmd/api/main.go`
   - metadata read now uses PostgreSQL first, then etcd fallback
3. Updated delete flow:
   - when metadata exists, delete metadata from PostgreSQL (if enabled) and etcd
   - keeps backward compatibility during migration period

## 2026-02-19 (Milestone 3 metadata source switch)

1. Added `META_SOURCE` runtime switch in config:
   - valid values: `postgres`, `etcd`, `auto` (fallback to `auto` for invalid input)
2. Updated API metadata loader to honor source mode:
   - `auto`: PostgreSQL first, etcd fallback
   - `postgres`: PostgreSQL only
   - `etcd`: etcd only
3. Extended `/health` metadata section:
   - now includes `metadata.source`

## 2026-02-19 (Milestone 4 normalized metadata write, step 1)

1. Added normalized metadata upsert path in `internal/meta/normalized.go`:
   - transactional upsert into `objects`
   - transactional upsert into `object_versions`
2. Updated write finalization order in `writeservice.finalizeWALEntry`:
   - PostgreSQL normalized upsert (`objects` + `object_versions`)
   - PostgreSQL compatibility upsert (`metadata_kv`)
   - etcd compatibility write (unchanged)

## 2026-02-20 (Milestone 4 normalized metadata read/delete, step 2)

1. Added normalized metadata query path in `internal/meta/normalized.go`:
   - `Store.GetNormalizedMetadata(...)` joins `objects` + `object_versions`
   - converts normalized rows into compatibility metadata shape for existing read/delete services
2. Updated API metadata resolution priority:
   - `loadMetadata(...)` now checks normalized PostgreSQL metadata before `metadata_kv` fallback
   - keeps `META_SOURCE=auto|postgres|etcd` behavior unchanged
3. Added normalized metadata cleanup on delete:
   - new `Store.DeleteNormalizedMetadata(...)`
   - API delete flow now removes normalized rows and compatibility rows in PostgreSQL

## 2026-02-20 (Milestone 4 writeservice metadata source transition, step 3)

1. Updated writeservice metadata read path used by Hybrid update:
   - added internal `loadExistingMetadata(...)` with `META_SOURCE=auto|postgres|etcd` logic
   - read order matches API behavior: normalized PostgreSQL -> `metadata_kv` -> etcd (fallback)
2. Removed direct etcd-only metadata dependency in Hybrid update flow:
   - old cold hash/version lookup now uses source-aware metadata loader
   - preserves backward compatibility when metadata is missing

## 2026-02-20 (Milestone 4 metadata observability, step 4)

1. Added metadata lookup source counters in API:
   - `postgres_normalized_hit`
   - `etcd_hit`
   - `not_found`
   - `error_count`
2. Exposed lookup counters via API endpoints:
   - `/health` -> `metadata.lookup`
   - `/v2/admin/metrics-snapshot` -> `metadata_lookup`
3. Goal support:
   - enables data-driven decision for `metadata_kv` fallback retirement

## 2026-02-20 (Milestone 4 metadata_kv fallback retirement, step 5)

1. Switched metadata read path to normalized-first without `metadata_kv` read fallback:
   - API `loadMetadata(...)` now reads `objects + object_versions` first
   - falls back directly to etcd for compatibility
2. Updated writeservice metadata lookup path similarly:
   - Hybrid old-metadata lookup now uses `normalized -> etcd` order
3. Retained `metadata_kv` write/delete paths temporarily:
   - keeps rollback safety before full schema/code cleanup

## 2026-02-20 (Milestone 4 metadata_kv write retirement, step 6)

1. Removed `metadata_kv` write from foreground finalize path:
   - `writeservice.finalizeWALEntry(...)` now commits:
     - PostgreSQL normalized metadata (`objects + object_versions`)
     - etcd compatibility metadata
2. Kept `metadata_kv` delete path temporarily:
   - allows cleanup of historical rows during migration window

## 2026-02-20 (Milestone 4 metadata_kv runtime dependency removal, step 7)

1. Removed API delete-path dependency on `metadata_kv`:
   - delete metadata now targets normalized PostgreSQL rows + etcd compatibility key
2. Metadata runtime path status:
   - no read/write/delete runtime dependency on `metadata_kv`
   - table and migration files retained temporarily for controlled deprecation

## 2026-02-20 (Milestone 4 metadata_kv final cleanup, step 8)

1. Removed transitional code and schema artifacts:
   - deleted `internal/meta/metadata_kv.go`
   - deleted `000002_metadata_kv.up.sql`
   - deleted `000002_metadata_kv.down.sql`
2. Metadata path now fully converged in code:
   - PostgreSQL normalized tables (`objects` + `object_versions`) as primary source
   - etcd retained only as compatibility fallback in current phase

## 2026-02-20 (Milestone 5 postgres node discovery bootstrap, step 1)

1. Added PostgreSQL node heartbeat repository in `internal/meta/node_heartbeats.go`:
   - `UpsertNodeHeartbeat(...)`
   - `ListHealthyNodeIDs(...)`
2. Wired storage nodes to report heartbeats to PostgreSQL:
   - added periodic heartbeat loop in `cmd/storage_node/main.go`
   - heartbeat payload includes free bytes and write queue depth
3. Added API node discovery source switch:
   - new config `NODE_DISCOVERY_SOURCE=auto|postgres|etcd`
   - `auto` prefers PostgreSQL discovery when metadata DB is available
4. Added discovery observability:
   - `/health` now reports configured/active node discovery source
   - `/v2/admin/metrics-snapshot` includes active node discovery stats

## 2026-02-20 (Milestone 5 postgres discovery defaulting, step 2)

1. Updated storage-node startup behavior for discovery source:
   - when `NODE_DISCOVERY_SOURCE=postgres`, node skips etcd lease registration
   - keeps PostgreSQL heartbeat reporting active
2. Updated compose defaults to prefer PostgreSQL discovery:
   - API uses `NODE_DISCOVERY_SOURCE=postgres`
   - all storage nodes use `NODE_DISCOVERY_SOURCE=postgres`
3. Validation:
   - compile checks passed for API / storage node / metadata / writeservice
   - `docker compose config -q` passed

## 2026-02-20 (Milestone 5 api etcd hard-dependency relaxation, step 3)

1. Updated API etcd client bootstrap behavior:
   - API skips etcd client initialization when both `META_SOURCE=postgres` and `NODE_DISCOVERY_SOURCE=postgres`
2. Updated compatibility behavior for metadata paths:
   - writeservice skips etcd compatibility metadata write when `META_SOURCE=postgres`
   - metadata lookup handles `etcdClient=nil` safely in postgres-primary mode
3. Updated compose defaults:
   - API now explicitly uses `META_SOURCE=postgres`

## 2026-02-20 (Architecture freeze documentation)

1. Created architecture document split:
   - `docs/ARCHITECTURE_V2_FREEZE.md` as current authoritative architecture baseline
   - `docs/ARCHITECTURE_V1_LEGACY.md` as historical reference
2. Updated `docs/ARCHITECTURE.md` as architecture index/entry point
3. Purpose:
   - lock implementation target to v2 (PostgreSQL-first, tiered background migration, source-switchable discovery)

## 2026-02-20 (Milestone 5 postgres discovery e2e validation, step 4)

1. Environment validation:
   - rebuilt and started compose services with latest images (`docker compose up -d --build`)
   - initialized metadata schema in PostgreSQL for fresh volume
2. Health/metrics validation:
   - API health reports `node_discovery.active_source=postgres`
   - admin snapshot reports `active_node_count=6`
   - metadata lookup counters show PostgreSQL normalized hits, etcd hit remains 0
3. Smoke test validation (container-internal):
   - `POST /write` success (replication path, full node write)
   - `GET /read/{key}` success (payload verified)
   - `DELETE /delete/{key}` success (replica cleanup verified)

## 2026-02-20 (Milestone 5 compose dependency cleanup, step 5)

1. Removed default postgres-only runtime dependency on etcd for API/storage nodes in compose:
   - removed `ETCD_HOST`/`ETCD_PORT` env from `api` and `storage_node_*` services
   - removed `api.depends_on.etcd0` in default compose path
2. Kept etcd services for compatibility consumers (e.g., healer / legacy modes).
3. Validation:
   - `docker compose config -q` passed
   - compile checks for API / storage node / metadata / writeservice passed

## 2026-02-20 (Milestone 5 deployment profile split, step 6)

1. Added compose profile gating for legacy etcd path:
   - `etcd0`, `etcd1`, `etcd2`, and `healer` now use profile `legacy-etcd`
2. Default deployment target is now v2 postgres-first path:
   - running `docker compose up -d` no longer requires starting etcd services by default
3. Legacy compatibility path remains available:
   - can be enabled with `--profile legacy-etcd` when needed

## 2026-02-21 (Milestone 6 tiering worker skeleton, step 1)

1. Added PostgreSQL tiering task repository methods in `internal/meta/tiering_tasks.go`:
   - `EnqueueTieringTask(...)`
   - `ClaimNextTieringTask(...)` (`FOR UPDATE SKIP LOCKED`)
   - `MarkTieringTaskDone(...)`
   - `MarkTieringTaskRetry(...)`
2. Added `internal/tiering/worker.go`:
   - poll loop + task dispatch for `REPL_TO_EC`
   - retry backoff with cap (up to 5 minutes)
3. Validation:
   - `go test ./internal/meta ./internal/tiering ./internal/writeservice ./cmd/api ./cmd/storage_node`

## 2026-02-21 (Milestone 6 tiering worker runnable bootstrap, step 2)

1. Added runnable worker entrypoint `cmd/tiering_worker/main.go`:
   - metadata store bootstrap + ping
   - env-configurable poll interval and task type
   - graceful shutdown (`SIGINT`/`SIGTERM`)
2. Added compose/runtime packaging:
   - `Dockerfile` now builds and copies `/usr/local/bin/tiering_worker`
   - `docker-compose.yaml` now includes `tiering_worker` service (PostgreSQL path)
3. Validation:
   - `go test ./cmd/tiering_worker ./internal/tiering ./internal/meta`

## 2026-02-21 (Milestone 6 write-path task enqueue, step 3)

1. Added write-path tiering enqueue in `writeservice.finalizeWALEntry(...)`:
   - after PostgreSQL normalized metadata commit, replication writes enqueue `REPL_TO_EC` task
   - task identity is deterministic by `repl2ec:{object_id}:{hot_version}`
2. Added A1 baseline scheduling control:
   - `AGE_THRESHOLD_SEC` is now loaded in config (default `3600`)
   - queued task uses `scheduled_at = now + AGE_THRESHOLD_SEC`
3. Current behavior:
   - enqueue is best-effort (logs warning on failure), foreground write path remains available

## 2026-02-21 (Milestone 6 worker real processing path, step 4)

1. Added metadata state transition repository in `internal/meta/tiering_state.go`:
   - `GetObjectVersionSnapshot(...)`
   - `MarkObjectMigrating(...)`
   - `PromoteObjectVersionToEC(...)` (transactional update for EC tier + shard locations + object state)
2. Added real REPL->EC processor in `internal/tiering/repl_to_ec_processor.go`:
   - validate task snapshot and stale-task skip
   - read source object from healthy replica nodes
   - run EC split/encode, write shard keys to storage nodes
   - enforce `>= K` successful shard writes before metadata commit
3. Upgraded `cmd/tiering_worker/main.go`:
   - switched from stub processor to real processor using shared HTTP client and EC driver

## 2026-02-21 (Milestone 6 integration smoke script, step 5)

1. Added executable smoke script:
   - `scripts/smoke_tiering_repl_to_ec.sh`
2. Script validates end-to-end path:
   - replication write
   - tiering task enqueue check
   - force `scheduled_at` for immediate execution
   - wait for `objects.state=EC_ACTIVE`, `object_versions.tier=EC`, `tiering_tasks.task_state=DONE`
   - API read-back payload verification
3. Runtime note:
   - script assumes compose stack is already running and API reachable at `API_BASE` (default `http://127.0.0.1:8000`)

## 2026-02-21 (Milestone 6 normalized replica location write, step 6)

1. Updated replication foreground metadata commit:
   - `WriteReplication(...)` now includes `replica_nodes` (actual successful node URLs) in normalized metadata payload
2. Extended normalized upsert transaction in `internal/meta/normalized.go`:
   - when tier is `HOT`, upsert `replica_locations(object_id, version, node_id, path, status)`
   - written in the same metadata transaction as `objects` + `object_versions`
3. Result:
   - foreground replication write now records durable per-version replica placement metadata in PostgreSQL

## 2026-02-21 (Milestone 6 periodic A1 policy scanner, step 7)

1. Added metadata-side periodic candidate enqueue API:
   - `Store.EnqueueTieringCandidatesA1(...)` in `internal/meta/tiering_policy.go`
   - scans `objects.state=HOT_ACTIVE` older than `AGE_THRESHOLD_SEC`
   - enqueues deterministic `REPL_TO_EC` tasks and marks objects `MIGRATION_PENDING`
2. Added scanner runtime in `internal/tiering/policy_scanner.go`:
   - periodic run loop for A1 scheduling
   - logs enqueue counts per round
3. Wired scanner into `cmd/tiering_worker/main.go`:
   - scanner and worker now run concurrently in same process
4. Added compose env:
   - `TIERING_POLICY_PERIOD_SEC` for scanner period control

## 2026-02-21 (Milestone 6 admin task visibility, step 8)

1. Added metadata query API for tiering tasks:
   - `Store.ListTieringTasks(ctx, state, limit)` in `internal/meta/tiering_tasks.go`
2. Implemented admin endpoint:
   - `GET /v2/admin/tasks`
   - supports optional query params: `state`, `limit`
3. Purpose:
   - quick inspection of task queue lifecycle (`PENDING/RUNNING/DONE/RETRY_WAIT`) during integration/debug runs

## 2026-02-21 (Milestone 6 admin node visibility, step 9)

1. Added metadata node snapshot query:
   - `Store.ListNodeHeartbeats(ctx, limit)` in `internal/meta/node_heartbeats.go`
2. Implemented admin endpoint:
   - `GET /v2/admin/nodes`
   - supports optional query param: `limit`
3. Response fields include:
   - `node_id`, `status`, `last_seen_at`, `is_stale`
   - `free_bytes`, `io_queue_depth`, `cpu_load`
4. Staleness rule:
   - computed by `NODE_HEARTBEAT_STALE_SEC`

## 2026-02-21 (Milestone 6 admin task filters and summary, step 10)

1. Extended `GET /v2/admin/tasks` query support:
   - new filter param: `task_type`
2. Added task state summary aggregation:
   - `state_counts` in response (grouped by `task_state`)
   - summary is filter-aware by `task_type` (not limited by `limit`)
3. Metadata store changes:
   - `ListTieringTasks(ctx, state, taskType, limit)`
   - `ListTieringTaskStateCounts(ctx, taskType)`

## 2026-02-21 (Milestone 6 object detail admin endpoint, step 11)

1. Added object detail admin query in metadata layer:
   - `GetObjectAdminView(ctx, objectID)` in `internal/meta/object_admin.go`
   - joins current `objects` + `object_versions`
   - includes `replica_locations` and `ec_shard_locations` for current version
2. Added API endpoint:
   - `GET /v2/admin/objects/:id`
3. Endpoint output:
   - object state/version timestamps
   - current version metadata (tier/checksum/encoding)
   - replica and EC shard placement rows

## 2026-02-21 (Milestone 6 manual task unstick endpoint, step 12)

1. Added metadata helper:
   - `RequeueTieringTaskNow(ctx, taskID)` in `internal/meta/tiering_tasks.go`
2. Added admin action endpoint:
   - `POST /v2/admin/tasks/:id/retry-now`
3. Behavior:
   - force task back to immediate `PENDING` with `scheduled_at=NOW()`
   - applies to `PENDING/RUNNING/RETRY_WAIT/FAILED`
   - `DONE` tasks are intentionally not requeued by this endpoint

## 2026-02-21 (Milestone 6 manual task cancel endpoint, step 13)

1. Added metadata helper:
   - `CancelTieringTask(ctx, taskID, reason)` in `internal/meta/tiering_tasks.go`
2. Added admin action endpoint:
   - `POST /v2/admin/tasks/:id/cancel`
3. Behavior:
   - mark task as `FAILED` and persist cancel reason in `last_error`
   - applies to `PENDING/RUNNING/RETRY_WAIT`
   - `DONE` tasks are not cancellable via this endpoint

## 2026-02-21 (Milestone 6 task action hints in admin list, step 14)

1. Enhanced `GET /v2/admin/tasks` per-task response:
   - added `actions.retry_now` boolean
   - added `actions.cancel` boolean
2. Action hint rules:
   - `retry_now`: allowed for `PENDING/RUNNING/RETRY_WAIT/FAILED`
   - `cancel`: allowed for `PENDING/RUNNING/RETRY_WAIT`

## 2026-02-21 (Milestone 6 API documentation sync, step 15)

1. Updated `docs/API.md` to include implemented admin v2 endpoints:
   - `/v2/admin/tasks`
   - `/v2/admin/tasks/:id/retry-now`
   - `/v2/admin/tasks/:id/cancel`
   - `/v2/admin/nodes`
   - `/v2/admin/objects/:id`
2. Added query/filter semantics and key response fields:
   - task filters + `state_counts`
   - node staleness view
   - object placement metadata

## 2026-02-21 (Milestone 6 architecture status sync, step 16)

1. Updated `docs/ARCHITECTURE.md` with a live implementation snapshot table:
   - `DONE/PARTIAL/TODO` status across core architecture areas
2. Added current runtime component map:
   - `api`, `storage_node_*`, `tiering_worker`, `postgres`, `redpanda`
   - `etcd/healer` legacy profile position explicitly documented

## 2026-02-21 (Milestone 6 generic object API bootstrap, step 17)

1. Added binary v2 object endpoints in `cmd/api/main.go`:
   - `PUT /v2/objects/:id` (replication-first write, raw bytes)
   - `GET /v2/objects/:id` (raw bytes read for replication/EC objects)
2. Scope (current):
   - no JSON requirement on v2 object write path
   - `field_hybrid` objects are not exposed via binary v2 GET yet
3. Documentation:
   - updated `docs/API.md` with v2 generic object endpoint section

## 2026-02-21 (Milestone 6 normalized content-type persistence, step 18)

1. Added metadata migration for generic object HTTP metadata:
   - `object_versions.content_type` column (`000002_object_versions_content_type`)
2. Updated normalized metadata read/write paths:
   - `UpsertNormalizedMetadata(...)` persists `content_type`
   - `GetNormalizedMetadata(...)` returns `content_type`
3. Updated v2 binary object API:
   - `PUT /v2/objects/:id` now persists request `Content-Type`
   - `GET /v2/objects/:id` returns stored `Content-Type` when available
4. Updated admin object detail:
   - `/v2/admin/objects/:id` includes current version `content_type`

## 2026-02-21 (Milestone 6 dockerized metadata migration service, step 19)

1. Added `meta_migrate` binary to Docker image build (`Dockerfile`)
2. Added `meta_migrate` compose service (run-on-demand):
   - uses same project image
   - waits for PostgreSQL healthcheck
   - runs `META_MIGRATE_ACTION=up` by default
3. Result:
   - schema migration can be executed in full-Docker workflow without local Go runtime

## 2026-02-21 (Milestone 6 binary metadata test coverage, step 20)

1. Added writeservice unit test:
   - `TestWriteReplicationWithMetadata_PersistsContentTypeAndLength`
2. Validation target:
   - verifies `WriteReplicationWithMetadata(...)` commits `content_type`
   - verifies `original_length` metadata is persisted as expected

## 2026-03-03 (Milestone 6 API handler test coverage, step 21)

1. Refactored v2 object route registration for testability:
   - extracted `registerV2ObjectRoutes(...)` in `cmd/api/main.go`
   - added dependency-injection struct `v2ObjectRouteDeps`
2. Added `cmd/api/v2_objects_test.go` coverage:
   - `PUT /v2/objects/:id` success path + default `Content-Type`
   - `GET /v2/objects/:id` replication path + EC path
   - `GET /v2/objects/:id` metadata-not-found and strategy-conflict paths
3. Verification:
   - `go test ./cmd/api` passes

## 2026-03-03 (Milestone 6 API error-path test coverage, step 22)

1. Expanded `cmd/api/v2_objects_test.go` with error-path coverage:
   - `PUT /v2/objects/:id` write failure -> `500`
   - `GET /v2/objects/:id` metadata internal error -> `500`
   - `GET /v2/objects/:id` replication read failure -> `404`
   - `GET /v2/objects/:id` EC read failure -> `404`
2. Verification:
   - `go test ./cmd/api` passes

## 2026-03-03 (Milestone 6 test-suite stabilization, step 23)

1. Fixed `internal/readservice` test fixture drift:
   - `TestReadFieldHybrid_Success` now provides at least `k` healthy cold shards
   - matches current `ReadEC/GetExistingColdFields` guard (`healthyCount >= k`)
2. Verification:
   - `go test ./internal/readservice` passes
   - `go test ./...` passes

## 2026-03-03 (Milestone 6 admin-task handler test coverage, step 24)

1. Refactored admin task routes for testability:
   - extracted `registerAdminTaskRoutes(...)` in `cmd/api/main.go`
   - added dependency-injection struct `adminTaskRouteDeps`
2. Added `cmd/api/admin_tasks_test.go` coverage:
   - `GET /v2/admin/tasks` success path (`filters`, `state_counts`, `actions`)
   - `GET /v2/admin/tasks` invalid limit (`400`) and metadata unavailable (`503`)
   - `POST /v2/admin/tasks/:id/retry-now` success/not-found/internal-error
   - `POST /v2/admin/tasks/:id/cancel` default reason/query reason/not-found
3. Verification:
   - `go test ./cmd/api` passes
   - `go test ./...` passes

## 2026-03-03 (Milestone 6 WAL optionalization, step 25)

1. Added WAL feature flag:
   - `WAL_ENABLED` config in `internal/config/config.go`
   - default: `false` (postgres-first profile)
2. Write path behavior update:
   - `createWALEntry(...)` now skips Redpanda when `WAL_ENABLED=false`
   - write commit path continues through metadata commit (Postgres/Etcd compatibility path)
3. API bootstrap update:
   - `cmd/api/main.go` only initializes Redpanda client when `WAL_ENABLED=true`
   - avoids startup failure when Redpanda is not present
4. Test & compose updates:
   - added `TestWriteReplication_WALDisabled_AllowsNilMQClient`
   - `docker-compose.yaml` sets `WAL_ENABLED=false` for `api`
   - removed `api -> redpanda` startup dependency
5. Verification:
   - `go test ./internal/writeservice` passes
   - `go test ./...` passes
   - `docker compose config -q` passes

## 2026-03-03 (Milestone 6 Redpanda default-off profile cleanup, step 26)

1. Updated Docker default runtime to postgres-first profile:
   - `api` no longer exports `WAL_BROKER` / `WAL_TOPIC` in default env
   - `redpanda` moved behind compose profiles: `legacy-wal`, `legacy-etcd`
2. Result:
   - default `docker compose up` no longer requires Redpanda for API startup
   - legacy WAL path is explicit profile opt-in
3. Documentation:
   - added runtime profile note to `docs/API.md`
4. Verification:
   - `docker compose config -q` passes

## 2026-03-03 (Milestone 6 API structure cleanup, step 27)

1. Split route-heavy `cmd/api/main.go` into focused files (no behavior change):
   - `cmd/api/routes_legacy.go`
     - `/write`, `/read/:key`, `/delete/:key`
     - `/node_status`, `/storage_usage`
   - `cmd/api/routes_admin_misc.go`
     - `/health`, `/v2/admin/metrics-snapshot`
     - `/v2/admin/nodes`, `/v2/admin/objects/:id`
   - `cmd/api/node_registry.go`
     - active-node snapshot helpers for route usage
2. Kept existing testable route modules intact:
   - `registerV2ObjectRoutes(...)`
   - `registerAdminTaskRoutes(...)`
3. Verification:
   - `go test ./cmd/api` passes
   - `go test ./...` passes

## 2026-03-03 (Milestone 6 API bootstrap/runtime extraction, step 28)

1. Extracted API runtime bootstrap from `cmd/api/main.go` into `cmd/api/bootstrap_runtime.go`:
   - `initAppRuntime()`: etcd/meta/mq/service initialization + cleanup closure
   - `startNodeDiscovery(...)`: discovery source selection and watcher startup
   - `buildRouter(...)`: route wiring composition
2. `main.go` is now reduced to high-level flow:
   - init runtime
   - start discovery
   - build router
   - run server
3. Safety hardening during extraction:
   - admin task deps now use nil-safe closures (avoid method-value panic when metadata is unavailable)
4. Verification:
   - `go test ./cmd/api` passes
   - `go test ./...` passes

## 2026-03-03 (Milestone 6 field_hybrid deprecation at API layer, step 29)

1. Converged legacy API strategy surface:
   - `/write` no longer accepts `strategy=field_hybrid` (returns `422`)
   - `/read/:key` returns `409` for metadata strategy `field_hybrid`
   - `/delete/:key` returns `409` for metadata strategy `field_hybrid`
2. Simplified legacy route dependency wiring:
   - removed direct `field_hybrid` read/write/delete function dependencies from `legacyRouteDeps`
3. Added tests:
   - `cmd/api/routes_legacy_test.go` covers write/read/delete deprecation behavior
4. Documentation:
   - updated `docs/API.md` strategy notes for `/write`, `/read`, `/delete`
5. Verification:
   - `go test ./cmd/api` passes
   - `go test ./...` passes

## 2026-03-03 (Milestone 6 replication-only legacy write convergence, step 30)

1. Converged legacy `/write` to replication-only:
   - `strategy=replication` remains supported
   - `strategy=ec` now returns `422` (direct EC write deprecated)
   - `strategy=field_hybrid` remains deprecated (`422`)
2. Added/updated tests:
   - `TestLegacyWrite_ECRejected`
   - existing `field_hybrid` rejection test remains
3. Documentation:
   - updated `/write` strategy note in `docs/API.md`
4. Verification:
   - `go test ./cmd/api` passes
   - `go test ./...` passes

## 2026-03-03 (Milestone 6 hybrid implementation isolation, step 31)

1. Isolated `field_hybrid` implementation into legacy files (no behavior change):
   - `internal/writeservice/writeservice_legacy_hybrid.go`
   - `internal/readservice/readservice_legacy_hybrid.go`
   - `internal/storageops/storageops_legacy_hybrid.go`
2. Main strategy files now focus on active paths (`replication` / `ec`).
3. Added explicit deprecation comments on legacy hybrid methods:
   - `WriteFieldHybrid`
   - `GetExistingColdFields`
   - `ReadFieldHybrid`
   - `DeleteFieldHybrid`
4. Verification:
   - `go test ./internal/writeservice ./internal/readservice ./internal/storageops` passes
   - `go test ./...` passes

## 2026-03-03 (Milestone 6 active interface convergence, step 32)

1. Narrowed core interfaces to active strategies only:
   - `internal/interfaces/IReadService` now exposes:
     - `CheckFirstWrite`
     - `ReadReplication`
     - `ReadEC`
   - `internal/interfaces/IStorageOps` now exposes:
     - `DeleteReplication`
     - `DeleteEC`
2. Legacy hybrid methods remain implemented on concrete services in legacy files, but are no longer part of active core contracts.
3. Verification:
   - `go test ./...` passes

## 2026-03-03 (Milestone 6 writeservice dependency trim, step 33)

1. Removed unused read dependency from `writeservice.Service`:
   - dropped `read` field
   - simplified `writeservice.NewService(...)` signature
2. Updated call sites:
   - `cmd/api/bootstrap_runtime.go`
   - `internal/writeservice/writeservice_test.go`
3. Cleaned obsolete read mocks in writeservice tests.
4. Verification:
   - `go test ./internal/writeservice` passes
   - `go test ./...` passes

## 2026-03-03 (Milestone 6 optional legacy-hybrid build gate, step 34)

1. Added optional build exclusion gate for legacy hybrid files:
   - `internal/writeservice/writeservice_legacy_hybrid.go`
   - `internal/readservice/readservice_legacy_hybrid.go`
   - `internal/storageops/storageops_legacy_hybrid.go`
2. Build constraint:
   - default: legacy hybrid code included
   - optional exclusion: build with `-tags no_legacy_hybrid`
3. Documentation:
   - added build note in `docs/API.md`
4. Verification:
   - `go test ./...` passes

## 2026-03-03 (Milestone 6 legacy hybrid hard removal, step 35)

1. Removed legacy hybrid implementation files:
   - `internal/writeservice/writeservice_legacy_hybrid.go`
   - `internal/readservice/readservice_legacy_hybrid.go`
   - `internal/storageops/storageops_legacy_hybrid.go`
2. Removed hybrid-focused unit tests that depended on deleted methods:
   - `TestReadFieldHybrid_Success`
   - `TestDeleteFieldHybrid`
   - `TestDeleteFieldHybrid_BlindDelete`
3. Updated docs to reflect hard removal state.
4. Verification:
   - `go test ./...` passes
   - `go test -tags no_legacy_hybrid ./...` passes

## 2026-03-03 (Milestone 6 one-command v2 e2e smoke flow, step 36)

1. Added executable script:
   - `scripts/smoke_e2e_v2.sh`
2. Covered flow:
   - `meta_migrate` run
   - optional compose startup
   - `PUT /v2/objects/:id` (binary)
   - tiering task existence + admin task visibility check
   - optional force-schedule task
   - wait for `EC_ACTIVE` + `tier=EC` + task `DONE`
   - `GET /v2/objects/:id` payload validation
3. Config knobs:
   - `START_STACK`, `FORCE_TASK_NOW`, `API_BASE`, `TIMEOUT_SEC`
4. Documentation:
   - updated `docs/API.md` with smoke entry point

## 2026-03-10 (Milestone 6 hygiene cleanup pass, step 37)

1. Cleaned non-functional noise and stale wording:
   - updated outdated writeservice header comments to match current architecture
   - normalized `ReadEC` debug log wording
   - replaced storage-node Chinese debug comment with concise English diagnostic comment
2. Fixed stale API examples:
   - `/write` success example now uses `replication` response shape
   - `/delete` success example no longer references removed `field_hybrid` fields
3. Verification:
   - `go test ./...` passes

## 2026-03-10 (Milestone 6 field_hybrid symbol purge, step 38)

1. Removed remaining `field_hybrid` symbols from active code:
   - deleted `StrategyFieldHybrid` constant from `internal/config/config.go`
   - removed obsolete `HotFields` configuration
   - removed hybrid-only utility APIs from `IUtilsSvc` and `utils.Service`
2. Simplified compatibility handling:
   - legacy API conflict checks now use string compatibility guard (`"field_hybrid"`)
   - normalized metadata tier fallback now returns active default strategy (`replication`)
3. Updated stale tests/docs:
   - removed hybrid-only utility tests
   - removed hybrid-specific mock methods in read/write service tests
   - refreshed API/Healer docs wording to reflect removal
4. Verification:
   - `go test ./...` passes

## 2026-03-10 (Milestone 6 field_hybrid ecosystem cleanup, step 39)

1. Cleaned remaining ecosystem references that could mislead active usage:
   - updated `Readme.md` usage/testing sections to remove `field_hybrid` guidance
   - removed obsolete `test/hybrid_only.py`
   - converted `test/simple_test.py` hybrid scenario into deprecated-strategy negative check (`422`)
2. Updated benchmark defaults away from removed strategy:
   - `benchmark/k6/read_latency.js` default strategy now `replication`
   - removed hybrid run path from `benchmark/k6/benchmark.js`
   - `benchmark/go-ycsb/db/hybridstore/db.go` default strategy now `replication`
   - refreshed `benchmark/go-ycsb` helper scripts/comments that referenced `field_hybrid`
   - removed hybrid row from `test/verify_storage.py` report output
3. Verification:
   - pending full test pass in next step (`go test ./...`)

## 2026-03-10 (Milestone 6 legacy healer removal, step 40)

1. Removed legacy healer runtime path:
   - deleted `cmd/healer/*`
   - removed healer binary build/copy stages from `Dockerfile`
   - removed `healer` service from `docker-compose.yaml`
2. Cleaned associated references:
   - removed obsolete healer lock constant from `internal/config/config.go`
   - updated architecture and benchmark docs to no longer require healer operations
   - removed `docs/HealerTest.md` (legacy)
3. Verification:
   - `go test ./...` passes

## 2026-03-17 (Milestone 6 WAL/Redpanda hard removal from runtime path, step 41)

1. Removed WAL/Redpanda runtime dependencies from write path:
   - deleted WAL helper flow from `internal/writeservice/writeservice.go`
   - simplified metadata commit function naming (`finalizeMetadata`)
   - removed `mq` dependency from `writeservice.NewService(...)`
2. Removed API runtime initialization of mq client:
   - `cmd/api/bootstrap_runtime.go` no longer imports/creates `internal/mq` client
3. Removed legacy WAL package and config knobs:
   - deleted `internal/mq/client.go`
   - removed `WALEnabled` and `EtcdWALPrefix` from `internal/config/config.go`
4. Compose and docs cleanup:
   - removed `redpanda` service/volume and `WAL_ENABLED` env from `docker-compose.yaml`
   - refreshed `Readme.md`, `docs/API.md`, `docs/ARCHITECTURE.md`
5. Verification:
   - `go test ./...` passes

## 2026-03-17 (Milestone 6 postgres-only metadata/discovery convergence, step 42)

1. Removed etcd fallback from active API/runtime paths:
   - `cmd/api/bootstrap_runtime.go` no longer initializes etcd client
   - node discovery startup is postgres-heartbeat only
   - metadata lookup now queries PostgreSQL normalized tables only
2. Removed etcd compatibility writes from write path:
   - `internal/writeservice/writeservice.go` no longer writes compatibility metadata to etcd
   - legacy route deletion path no longer calls `deleteEtcdMetadata`
3. Removed legacy etcd code and compose services:
   - deleted `internal/etcd/client.go`
   - removed etcd services/volumes and `META_SOURCE`/`NODE_DISCOVERY_SOURCE` env usage from `docker-compose.yaml`
4. Cleanup and alignment:
   - simplified interfaces (`internal/interfaces/interfaces.go`) by removing `IEtcdClient`
   - updated write service tests to match postgres-first behavior
   - refreshed `Readme.md` and `docs/ARCHITECTURE.md` wording for postgres-only mainline
   - ran `go mod tidy` to drop obsolete dependencies
5. Verification:
   - `go test ./...` passes

## 2026-03-18 (Milestone 7 post-promotion HOT GC flow, step 43)

1. Added end-to-end GC task path after REPL->EC promotion:
   - `REPL_TO_EC` processor now enqueues deterministic GC task IDs: `gc-repl:{object}:{version}`
   - task type constant `GC` added to worker flow
2. Extended tiering worker to process both migration and GC tasks:
   - worker now dispatches `REPL_TO_EC` and `GC`
   - task-type filter supports `ALL` (empty filter) in `cmd/tiering_worker/main.go`
   - `docker-compose.yaml` sets `TIERING_WORKER_TASK_TYPE=ALL`
3. Implemented replication-GC processor:
   - validates object snapshot is still current and already `EC_ACTIVE`
   - deletes HOT replica blobs from storage nodes
   - marks `replica_locations.status='DELETED'` on success
4. Added metadata helpers for GC:
   - `ListActiveReplicaLocations(...)`
   - `MarkReplicaLocationsDeleted(...)`
5. Updated smoke and docs:
   - `scripts/smoke_e2e_v2.sh` now verifies GC task done + deleted replica rows
   - `docs/API.md` task type docs include `GC`
   - `docs/ARCHITECTURE.md` marks post-promotion HOT GC flow as `DONE`
6. Verification:
   - `go test ./...` passes
