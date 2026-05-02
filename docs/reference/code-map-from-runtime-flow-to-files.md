# Code Map Reference (Flow-to-File)

Scope: fast runtime-to-file mapping for debugging and implementation work.

## 1. Entrypoints

| Runtime | File | Main responsibility |
| --- | --- | --- |
| API | [`cmd/api/main.go`](../../cmd/api/main.go) + [`cmd/api/bootstrap_runtime.go`](../../cmd/api/bootstrap_runtime.go) | routes, dependency wiring, node discovery |
| Storage Node | [`cmd/storage_node/main.go`](../../cmd/storage_node/main.go) | local blob service process |
| Tiering Worker | [`cmd/tiering_worker/main.go`](../../cmd/tiering_worker/main.go) | leader lock + scanner + task worker |
| Metadata Service | [`cmd/meta_service/main.go`](../../cmd/meta_service/main.go) | RPC endpoint over repository |

## 2. End-to-End PUT Map

1. route handler: [`cmd/api/main.go`](../../cmd/api/main.go) (`PUT /v2/objects/:id`)
2. node selection: `getDynamicNodes(...)` in [`cmd/api/main.go`](../../cmd/api/main.go)
3. write service call: `WriteReplicationWithMetadata` in [`internal/writeservice/writeservice.go`](../../internal/writeservice/writeservice.go)
4. storage writes: storage-node `/store` calls inside write service
5. metadata commit: `UpsertNormalizedMetadata` in [`internal/meta/tikv_store_objects.go`](../../internal/meta/tikv_store_objects.go)
6. due-index write during metadata commit: `upsertTieringDueIndex` in [`internal/meta/tikv_store_due_index.go`](../../internal/meta/tikv_store_due_index.go)
7. scanner-driven task enqueue later: `EnqueueTieringCandidatesStrategy*` in [`internal/meta/tikv_store_policy.go`](../../internal/meta/tikv_store_policy.go)

## 3. End-to-End GET Map

1. route handler: [`cmd/api/main.go`](../../cmd/api/main.go) (`GET /v2/objects/:id`)
2. metadata load: `loadMetadata(...)` in [`cmd/api/main.go`](../../cmd/api/main.go)
3. strategy dispatch:
   - HOT: `ReadReplication` in [`internal/readservice/readservice.go`](../../internal/readservice/readservice.go)
   - EC: `ReadEC` in [`internal/readservice/readservice.go`](../../internal/readservice/readservice.go)
4. EC placement source: `GetNormalizedMetadata` in [`internal/meta/tikv_store_objects.go`](../../internal/meta/tikv_store_objects.go) loads active `ec/*` shard rows into normalized metadata

## 4. End-to-End DELETE Map

1. route handler: [`cmd/api/main.go`](../../cmd/api/main.go) (`DELETE /v2/objects/:id`)
2. strategy-specific data deletion through storage ops
3. metadata deletion: `DeleteNormalizedMetadata` in [`internal/meta/tikv_store_objects.go`](../../internal/meta/tikv_store_objects.go)
4. EC deletion uses recorded shard placement in [`internal/storageops/storageops.go`](../../internal/storageops/storageops.go) when normalized metadata includes `ec_shards`

## 5. Tiering and Maintenance Engine Map

### 5.1 Leadership and scanner

1. leader loop: `runScannerAsLeader` in [`cmd/tiering_worker/main.go`](../../cmd/tiering_worker/main.go)
2. lock calls: `TryAcquireLeaderLock`/`Ping`/`Release` through repository
3. scanner logic: [`internal/tiering/policy_scanner.go`](../../internal/tiering/policy_scanner.go)

### 5.2 Worker claim and dispatch

1. claim: `ClaimNextTieringTask` in [`internal/meta/tikv_store_tasks.go`](../../internal/meta/tikv_store_tasks.go)
2. CAS transaction helper: `RunInTxn` in [`internal/meta/kvstore/client.go`](../../internal/meta/kvstore/client.go)
3. ready/wait index claim logic: [`internal/meta/tikv_store_task_index.go`](../../internal/meta/tikv_store_task_index.go)
4. dispatch: switch in [`internal/tiering/worker.go`](../../internal/tiering/worker.go)
5. retry/fail policy: same file (`MarkTieringTaskRetry`, `MarkTieringTaskFailed`)

### 5.3 Processor implementations

1. REPL_TO_EC: [`internal/tiering/repl_to_ec_processor.go`](../../internal/tiering/repl_to_ec_processor.go)
2. REPAIR: [`internal/tiering/repair_replication_processor.go`](../../internal/tiering/repair_replication_processor.go)
3. GC: [`internal/tiering/gc_replication_processor.go`](../../internal/tiering/gc_replication_processor.go)
4. GC_OLD_VERSION: [`internal/tiering/old_version_gc_processor.go`](../../internal/tiering/old_version_gc_processor.go)

## 6. Metadata Stack Map

### 6.1 Contracts and protocol

1. repository interface: [`internal/meta/repository.go`](../../internal/meta/repository.go)
2. RPC protocol: [`internal/meta/rpc_protocol.go`](../../internal/meta/rpc_protocol.go)
3. RPC server: [`internal/meta/rpc_server.go`](../../internal/meta/rpc_server.go)
4. RPC client: [`internal/meta/rpc_client.go`](../../internal/meta/rpc_client.go)

### 6.2 TiKV implementation modules

1. core init/helpers: [`internal/meta/tikv_store.go`](../../internal/meta/tikv_store.go), [`internal/meta/tikv_store_helpers.go`](../../internal/meta/tikv_store_helpers.go)
2. schema/key builders: [`internal/meta/tikv_store_schema.go`](../../internal/meta/tikv_store_schema.go), [`internal/meta/tikv_store_keys.go`](../../internal/meta/tikv_store_keys.go)
3. object records: [`internal/meta/tikv_store_objects.go`](../../internal/meta/tikv_store_objects.go)
4. task queue: [`internal/meta/tikv_store_tasks.go`](../../internal/meta/tikv_store_tasks.go)
5. policy selection/due index: [`internal/meta/tikv_store_policy.go`](../../internal/meta/tikv_store_policy.go), [`internal/meta/tikv_store_due_index.go`](../../internal/meta/tikv_store_due_index.go)
6. migration transitions: [`internal/meta/tikv_store_migration.go`](../../internal/meta/tikv_store_migration.go)
7. old version GC metadata: [`internal/meta/tikv_store_old_version_gc.go`](../../internal/meta/tikv_store_old_version_gc.go)
8. cluster state and heartbeats: [`internal/meta/tikv_store_cluster.go`](../../internal/meta/tikv_store_cluster.go)
9. leader lock persistence: [`internal/meta/tikv_store_lock.go`](../../internal/meta/tikv_store_lock.go)

### 6.3 Low-level KV facade

1. client abstraction: [`internal/meta/kvstore/client.go`](../../internal/meta/kvstore/client.go)
2. transactional TiKV operations and lock helpers

## 7. Storage Node Map

1. engine and durable write queue: [`cmd/storage_node/engine.go`](../../cmd/storage_node/engine.go)
2. HTTP routes: [`cmd/storage_node/routes.go`](../../cmd/storage_node/routes.go)
3. heartbeat publisher: [`cmd/storage_node/heartbeat.go`](../../cmd/storage_node/heartbeat.go)

## 8. Test Map (Best starting points)

| Area | Tests |
| --- | --- |
| v2 object routes | [`cmd/api/v2_objects_test.go`](../../cmd/api/v2_objects_test.go) |
| admin task routes | [`cmd/api/admin_tasks_test.go`](../../cmd/api/admin_tasks_test.go) |
| write service | [`internal/writeservice/writeservice_test.go`](../../internal/writeservice/writeservice_test.go) |
| scanner logic | [`internal/tiering/policy_scanner_test.go`](../../internal/tiering/policy_scanner_test.go) |
| REPL->EC processor | [`internal/tiering/repl_to_ec_processor_test.go`](../../internal/tiering/repl_to_ec_processor_test.go) |
| REPAIR processor | [`internal/tiering/repair_replication_processor_test.go`](../../internal/tiering/repair_replication_processor_test.go) |
| metadata RPC | [`internal/meta/rpc_roundtrip_test.go`](../../internal/meta/rpc_roundtrip_test.go) |

## 9. Bug Trace Sequence

1. find route in `cmd/api/*`
2. find service call in `internal/*service/*`
3. find repository call in [`internal/meta/repository.go`](../../internal/meta/repository.go)
4. locate TiKV implementation in matching `tikv_store_*.go`
5. inspect task transitions in [`internal/tiering/worker.go`](../../internal/tiering/worker.go)
