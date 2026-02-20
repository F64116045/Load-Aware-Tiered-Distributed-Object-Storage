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
