# Getting Up to Speed

Audience: you (or any maintainer) who wants to recover deep system control quickly.

Target: after completing all stages, you can explain and modify the system without guesswork.

## 0. How to Use This Guide

1. Follow stages in order.
2. Do not skip verification steps.
3. Keep a local notes file and answer all stage questions.
4. If a stage fails, fix the gap before moving on.

Estimated time: 6-10 focused hours.

## 1. Stage 1 - Runtime Bring-up and Basic Confidence (60-90 min)

### 1.1 Goal

1. start stack from zero
2. pass smoke
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

### 1.3 Stage-1 questions (must answer)

1. Which service receives external traffic first?
2. Why does API require healthy node discovery before serving writes?
3. What must succeed before PUT returns 201?

## 2. Stage 2 - Full Data Flow Understanding (90-120 min)

Read in this order:

1. `explanation/system-architecture-and-responsibilities.md`
2. `explanation/put-get-delete-and-task-lifecycles.md`
3. `explanation/runtime-control-loops-and-schedulers.md`
4. `reference/task-state-machine-reference.md`

### 2.1 Verification tasks

1. Draw PUT sequence from memory (client -> metadata commit -> task enqueue).
2. Draw GET(HOT) and GET(EC) sequence separately.
3. Explain why stale task versions are skipped.

### 2.2 Stage-2 questions

1. Where is scanner single-leader enforced?
2. Which component decides policy A1/A2/A3?
3. Why can worker crash recover without external queue broker?

## 3. Stage 3 - Code-Level Ownership (120-180 min)

### 3.1 Mandatory file walk

1. API bootstrap: `cmd/api/bootstrap_runtime.go`
2. API routes: `cmd/api/main.go`, `cmd/api/routes_admin_misc.go`
3. Write path: `internal/writeservice/writeservice.go`
4. Read path: `internal/readservice/readservice.go`
5. Worker loop: `cmd/tiering_worker/main.go`, `internal/tiering/worker.go`
6. Scanner policy: `internal/tiering/policy_scanner.go`
7. Processors:
   - `internal/tiering/repl_to_ec_processor.go`
   - `internal/tiering/repair_replication_processor.go`
   - `internal/tiering/gc_replication_processor.go`
   - `internal/tiering/old_version_gc_processor.go`
8. Metadata contract: `internal/meta/repository.go`
9. Metadata implementation slices: `internal/meta/tikv_store_*.go`
10. RPC boundary: `internal/meta/rpc_client.go`, `internal/meta/rpc_server.go`, `internal/meta/rpc_protocol.go`

### 3.2 Breakpoint checklist

Set breakpoints/logpoints at:

1. `registerV2ObjectRoutes` PUT handler entry
2. `WriteReplicationWithMetadata`
3. `UpsertNormalizedMetadata`
4. `EnqueueTieringTask`
5. `ClaimNextTieringTask`
6. `ProcessReplicationToEC`
7. `PromoteObjectVersionToEC`

### 3.3 Stage-3 tasks

1. Add one log line that prints `task_id`, `task_type`, `retry_count` at worker dispatch.
2. Run `go test ./...`.
3. Prove no behavior regression via smoke.

## 4. Stage 4 - Operational Control (60-90 min)

Read:

1. `operations/incident-triage-restart-and-recovery-runbook.md`
2. `how-to/debug-scanner-leader-lock-flapping.md`
3. `how-to/recover-from-tikv-startup-failure.md`

### 4.1 Drills

1. simulate restart: `docker compose -f docker-compose.yaml restart tiering_worker`
2. observe leader endpoint before/after restart
3. validate tasks still progress

### 4.2 Stage-4 questions

1. What is first 5-minute triage order?
2. Which logs are most valuable for metadata lock incidents?
3. When should you use destructive `down -v`?

## 5. Stage 5 - Architecture Defense Readiness (60+ min)

You should now be able to defend the system in project review.

### 5.1 Prepare answers for

1. Why metadata goes through RPC service instead of direct TiKV access by all components.
2. Why foreground write is replication-first and EC async.
3. How policy variants A1/A2/A3 differ experimentally.
4. How consistency is maintained during concurrent writes and background tasks.
5. How stale tasks and retries are handled safely.

### 5.2 Pass criteria (90% control)

You pass when you can do all below without reading code while speaking:

1. explain end-to-end PUT/GET/DELETE flow
2. explain worker claim/retry/fail lifecycle
3. explain scanner leader lock behavior
4. locate any endpoint handler in under 30 seconds
5. perform one bug triage from logs to likely source file

## 6. Quick Recovery Version (if you return after weeks)

Run this compressed sequence:

1. `./scripts/up_stack.sh`
2. `START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh`
3. read `explanation/put-get-delete-and-task-lifecycles.md`
4. read `reference/code-map-from-runtime-flow-to-files.md`
5. read `operations/incident-triage-restart-and-recovery-runbook.md` section 2-4

You should recover effective context in around 90 minutes.
