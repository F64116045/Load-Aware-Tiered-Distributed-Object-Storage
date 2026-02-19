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
