# Local Setup and Smoke Validation

## 1. Objective

This guide validates that the full runtime path is functional:

1. stack startup and service health
2. object `PUT -> GET -> DELETE`
3. tiering task enqueue and processing
4. metadata and admin views

## 2. Prerequisites

1. Docker and Docker Compose
2. Go toolchain (optional, only for local test execution)
3. `curl`, `bash`, `rg`

## 3. Start Stack

```bash
./scripts/up_stack.sh
```

Health check:

```bash
curl -sS http://127.0.0.1:8000/health
```

Expected result:

1. API returns healthy/degraded JSON response instead of connection error.
2. PD, TiKV, `meta_service`, API, storage nodes, and `tiering_worker` are running.

## 4. Run Core Smoke

First run (bootstrap with smoke overrides):

```bash
START_STACK=true ./scripts/smoke_e2e_v2_tikv.sh
```

Then fast re-run:

```bash
START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh
```

Important:

1. `START_STACK=false` only reuses current running containers.
2. If the stack was started by plain `./scripts/up_stack.sh`, smoke may stall at `HOT_ACTIVE` because default age/policy settings are not smoke-friendly.

Coverage:

1. `PUT` writes HOT replicas and metadata.
2. scanner enqueues tiering task from due-index.
3. worker claims task and promotes version to `EC_ACTIVE`.
4. `GET` returns original bytes after background processing.

## 5. Manual Verification

### 5.1 Write Object

```bash
printf 'hello-smoke\n' >/tmp/smoke.bin
curl -sS -X PUT 'http://127.0.0.1:8000/v2/objects/smoke-1' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @/tmp/smoke.bin
```

### 5.2 Read and Compare

```bash
curl -sS 'http://127.0.0.1:8000/v2/objects/smoke-1' -o /tmp/smoke.out
cmp /tmp/smoke.bin /tmp/smoke.out && echo 'payload ok'
```

### 5.3 Inspect Admin Data

```bash
curl -sS 'http://127.0.0.1:8000/v2/admin/objects/smoke-1'
curl -sS 'http://127.0.0.1:8000/v2/admin/tasks?object_id=smoke-1&limit=20'
```

### 5.4 Delete Object

```bash
curl -sS -X DELETE 'http://127.0.0.1:8000/v2/objects/smoke-1'
```

## 6. Code Locations for Validation

1. API routes: [`cmd/api/main.go`](../../cmd/api/main.go)
2. write path: [`internal/writeservice/writeservice.go`](../../internal/writeservice/writeservice.go)
3. read path: [`internal/readservice/readservice.go`](../../internal/readservice/readservice.go)
4. worker and scanner entry: [`cmd/tiering_worker/main.go`](../../cmd/tiering_worker/main.go)
5. worker loop: [`internal/tiering/worker.go`](../../internal/tiering/worker.go)
6. scanner policy: [`internal/tiering/policy_scanner.go`](../../internal/tiering/policy_scanner.go)
7. metadata interface: [`internal/meta/repository.go`](../../internal/meta/repository.go)
8. metadata TiKV implementation: `internal/meta/tikv_store_*.go`

## 7. Common Startup Issues

1. Port `8000` conflict: stop conflicting process/container and rerun startup.
2. TiKV not ready: inspect TiKV and PD logs, then retry startup.
3. metadata RPC errors: verify `META_ENDPOINT`, `META_DSN`, `META_RPC_AUTH_TOKEN`.
