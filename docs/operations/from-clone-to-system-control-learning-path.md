# Getting Up to Speed

This guide provides a direct path from stack bring-up to code-level understanding.

## 1. Stage 1 - Runtime Bring-up and Basic Confidence

### 1.1 Goal

1. start stack from zero
2. run smoke
3. prove write/read/delete works

### 1.2 Steps

```bash
./scripts/up_stack.sh
START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh
```

Manual object check:

```bash
printf 'rampup-stage1\n' >/tmp/m1.bin
curl -sS -X PUT 'http://127.0.0.1:8000/v2/objects/m1' --data-binary @/tmp/m1.bin
curl -sS 'http://127.0.0.1:8000/v2/objects/m1' -o /tmp/m1.out
cmp /tmp/m1.bin /tmp/m1.out
curl -sS -X DELETE 'http://127.0.0.1:8000/v2/objects/m1'
```

## 2. Stage 2 - Full Data Flow Understanding

Read in this order:

1. [System Architecture and Responsibilities](../explanation/system-architecture-and-responsibilities.md)
2. [PUT, GET, DELETE and Task Lifecycles](../explanation/put-get-delete-and-task-lifecycles.md)
3. [Runtime Control Loops and Schedulers](../explanation/runtime-control-loops-and-schedulers.md)
4. [Task State Machine Reference](../reference/task-state-machine-reference.md)

## 3. Stage 3 - Code-Level Ownership

### 3.1 Mandatory file walk

1. API bootstrap: [`cmd/api/bootstrap_runtime.go`](../../cmd/api/bootstrap_runtime.go)
2. API routes: [`cmd/api/main.go`](../../cmd/api/main.go), [`cmd/api/routes_admin_misc.go`](../../cmd/api/routes_admin_misc.go)
3. Write path: [`internal/writeservice/writeservice.go`](../../internal/writeservice/writeservice.go)
4. Read path: [`internal/readservice/readservice.go`](../../internal/readservice/readservice.go)
5. Worker loop: [`cmd/tiering_worker/main.go`](../../cmd/tiering_worker/main.go), [`internal/tiering/worker.go`](../../internal/tiering/worker.go)
6. Scanner policy: [`internal/tiering/policy_scanner.go`](../../internal/tiering/policy_scanner.go)
7. Processors:
   - [`internal/tiering/repl_to_ec_processor.go`](../../internal/tiering/repl_to_ec_processor.go)
   - [`internal/tiering/repair_replication_processor.go`](../../internal/tiering/repair_replication_processor.go)
   - [`internal/tiering/gc_replication_processor.go`](../../internal/tiering/gc_replication_processor.go)
   - [`internal/tiering/old_version_gc_processor.go`](../../internal/tiering/old_version_gc_processor.go)
8. Metadata contract: [`internal/meta/repository.go`](../../internal/meta/repository.go)
9. Metadata implementation slices: [`internal/meta/tikv_store_data.go`](../../internal/meta/tikv_store_data.go), [`internal/meta/tikv_store_tasks.go`](../../internal/meta/tikv_store_tasks.go), [`internal/meta/tikv_store_nodes.go`](../../internal/meta/tikv_store_nodes.go), [`internal/meta/tikv_store_locks.go`](../../internal/meta/tikv_store_locks.go)
10. RPC boundary: [`internal/meta/rpc_client.go`](../../internal/meta/rpc_client.go), [`internal/meta/rpc_server.go`](../../internal/meta/rpc_server.go), [`internal/meta/rpc_protocol.go`](../../internal/meta/rpc_protocol.go)

### 3.2 Breakpoint checklist

Set breakpoints/logpoints at:

1. `registerV2ObjectRoutes` PUT handler entry
2. `WriteReplicationWithMetadata`
3. `UpsertNormalizedMetadata`
4. `EnqueueTieringTask`
5. `ClaimNextTieringTask`
6. `ProcessReplicationToEC`
7. `PromoteObjectVersionToEC`

## 4. Stage 4 - Operational Control

Read:

1. [Incident Triage, Restart, and Recovery Runbook](incident-triage-restart-and-recovery-runbook.md)
2. [Debug Scanner Leader Lock Flapping](../how-to/debug-scanner-leader-lock-flapping.md)
3. [Recover from TiKV Startup Failure](../how-to/recover-from-tikv-startup-failure.md)

### 4.1 Drills

1. simulate restart: `docker compose -f docker-compose.yaml restart tiering_worker`
2. observe leader endpoint before/after restart
3. validate tasks still progress

## 5. Continuous Re-entry Routine

If you come back after a long break, use this short sequence:

1. `./scripts/up_stack.sh`
2. `START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh`
3. Read [PUT, GET, DELETE and Task Lifecycles](../explanation/put-get-delete-and-task-lifecycles.md)
4. Read [Code Map from Runtime Flow to Files](../reference/code-map-from-runtime-flow-to-files.md)
5. Read [Incident Triage, Restart, and Recovery Runbook](incident-triage-restart-and-recovery-runbook.md) section 2-4
