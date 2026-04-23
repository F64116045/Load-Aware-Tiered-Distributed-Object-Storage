# Runtime and Code Understanding Guide

## 1. Runtime Verification Baseline

Execute startup and smoke before any code-level analysis:

```bash
./scripts/up_stack.sh
START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh
```

Manual object check:

```bash
printf 'runtime-check\n' >/tmp/runtime-check.bin
curl -sS -X PUT 'http://127.0.0.1:8000/v2/objects/runtime-check-1' --data-binary @/tmp/runtime-check.bin
curl -sS 'http://127.0.0.1:8000/v2/objects/runtime-check-1' -o /tmp/runtime-check.out
cmp /tmp/runtime-check.bin /tmp/runtime-check.out
curl -sS -X DELETE 'http://127.0.0.1:8000/v2/objects/runtime-check-1'
```

## 2. Architecture Reading Order

1. [System Architecture and Responsibilities](../explanation/system-architecture-and-responsibilities.md)
2. [PUT, GET, DELETE and Task Lifecycles](../explanation/put-get-delete-and-task-lifecycles.md)
3. [Runtime Control Loops and Schedulers](../explanation/runtime-control-loops-and-schedulers.md)
4. [Task State Machine Reference](../reference/task-state-machine-reference.md)

## 3. Code Reading Order

1. API bootstrap: [`cmd/api/bootstrap_runtime.go`](../../cmd/api/bootstrap_runtime.go)
2. API routes: [`cmd/api/main.go`](../../cmd/api/main.go), [`cmd/api/routes_admin_misc.go`](../../cmd/api/routes_admin_misc.go)
3. Write path: [`internal/writeservice/writeservice.go`](../../internal/writeservice/writeservice.go)
4. Read path: [`internal/readservice/readservice.go`](../../internal/readservice/readservice.go)
5. Worker entry and loop: [`cmd/tiering_worker/main.go`](../../cmd/tiering_worker/main.go), [`internal/tiering/worker.go`](../../internal/tiering/worker.go)
6. Scanner policy: [`internal/tiering/policy_scanner.go`](../../internal/tiering/policy_scanner.go)
7. Task processors:
   - [`internal/tiering/repl_to_ec_processor.go`](../../internal/tiering/repl_to_ec_processor.go)
   - [`internal/tiering/repair_replication_processor.go`](../../internal/tiering/repair_replication_processor.go)
   - [`internal/tiering/gc_replication_processor.go`](../../internal/tiering/gc_replication_processor.go)
   - [`internal/tiering/old_version_gc_processor.go`](../../internal/tiering/old_version_gc_processor.go)
8. Metadata contract: [`internal/meta/repository.go`](../../internal/meta/repository.go)
9. Metadata implementation:
   - [`internal/meta/tikv_store_objects.go`](../../internal/meta/tikv_store_objects.go)
   - [`internal/meta/tikv_store_policy.go`](../../internal/meta/tikv_store_policy.go)
   - [`internal/meta/tikv_store_tasks.go`](../../internal/meta/tikv_store_tasks.go)
   - [`internal/meta/tikv_store_task_index.go`](../../internal/meta/tikv_store_task_index.go)
10. RPC boundary: [`internal/meta/rpc_client.go`](../../internal/meta/rpc_client.go), [`internal/meta/rpc_server.go`](../../internal/meta/rpc_server.go), [`internal/meta/rpc_protocol.go`](../../internal/meta/rpc_protocol.go)

## 4. Debug Trace Anchors

Set breakpoints or log points at:

1. `registerV2ObjectRoutes` (`PUT` handler)
2. `WriteReplicationWithMetadata`
3. `UpsertNormalizedMetadata`
4. `EnqueueTieringTask`
5. `ClaimNextTieringTask`
6. `ProcessReplicationToEC`
7. `PromoteObjectVersionToEC`

## 5. Operational Reading Order

1. [Incident Triage, Restart, and Recovery Runbook](incident-triage-restart-and-recovery-runbook.md)
2. [Debug Scanner Leader Lock Flapping](../how-to/debug-scanner-leader-lock-flapping.md)
3. [Recover from TiKV Startup Failure](../how-to/recover-from-tikv-startup-failure.md)

## 6. Verification Questions

The documentation and code mapping are complete when each item can be answered with code references:

1. where `PUT /v2/objects/:id` commits metadata
2. where stale tiering tasks are skipped
3. where scanner lock loss is handled
4. where old-version blob and metadata purge is executed
5. where `/v2/admin/leader` reads runtime leader state
