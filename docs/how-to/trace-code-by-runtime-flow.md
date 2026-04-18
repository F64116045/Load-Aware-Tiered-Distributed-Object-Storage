# How-to: Deep Dive the Codebase Without Getting Lost

Use this when you want to regain full engineering control quickly.

## 1. Strategy

Do not read files randomly. Use feature slices:

1. API ingress slice
2. write/read service slice
3. metadata abstraction slice
4. worker + scanner slice
5. storage node slice

## 2. Step-by-Step Reading Plan

## 2.1 Slice A - API ingress (30 min)

Read:

1. `cmd/api/bootstrap_runtime.go`
2. `cmd/api/main.go`
3. `cmd/api/routes_admin_misc.go`

Goal:

1. understand route registration
2. understand dependency injection (`appRuntime`)
3. understand admin observability endpoints

## 2.2 Slice B - Data path (45 min)

Read:

1. `internal/writeservice/writeservice.go`
2. `internal/readservice/readservice.go`
3. `internal/storageops/*`

Goal:

1. write quorum behavior
2. metadata finalize timing
3. HOT vs EC read path

## 2.3 Slice C - Metadata core (60 min)

Read:

1. `internal/meta/repository.go`
2. `internal/meta/rpc_protocol.go`
3. `internal/meta/rpc_client.go`
4. `internal/meta/rpc_server.go`
5. `internal/meta/tikv_store_*.go`

Goal:

1. know every repository method family
2. know which methods are RPC-exposed
3. know how TiKV keyspaces are mapped

## 2.4 Slice D - Async control loops (60 min)

Read:

1. `cmd/tiering_worker/main.go`
2. `internal/tiering/worker.go`
3. `internal/tiering/policy_scanner.go`
4. processors in `internal/tiering/*processor*.go`

Goal:

1. leader lock lifecycle
2. scanner trigger modes and policy variants
3. task claim/retry/failure semantics

## 2.5 Slice E - Storage node internals (30 min)

Read:

1. `cmd/storage_node/main.go`
2. `cmd/storage_node/engine.go`
3. `cmd/storage_node/routes.go`
4. `cmd/storage_node/heartbeat.go`

Goal:

1. durable write acknowledgment model
2. heartbeat metrics production
3. key/path handling safety

## 3. Debug Workflow

When investigating a bug:

1. start from route handler
2. follow service call
3. follow repository call
4. inspect task transition or metadata write
5. validate with admin endpoints and logs

## 4. Useful Commands

List routes quickly:

```bash
rg -n "router\.(GET|POST|PUT|DELETE|PATCH|HEAD)\(" cmd/api cmd/storage_node
```

List config knobs:

```bash
rg -n "getEnv\(|getEnvInt\(|getEnvBool\(|getEnvFloat64\(" internal/config
```

List task-related code:

```bash
rg -n "TaskType|TaskState|ClaimNextTieringTask|MarkTieringTask" internal
```

## 5. Comprehension Check (Self-test)

You are ready when you can answer:

1. exactly where `PUT /v2/objects/:id` commits metadata
2. exactly where stale tasks are skipped
3. exactly where lock ping failure is handled
4. exactly where old version purge deletes metadata + blobs
5. exactly where admin `/v2/admin/leader` data comes from
