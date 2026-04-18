# Onboarding (Day-1 to First Commit)

Audience: new contributor joining this repository.

Outcome: by the end of day 1, you can run the stack, validate an end-to-end object lifecycle, read key code paths, and push a small safe code change.

## 1. What You Are Building

This project is a load-aware tiered object storage system:

1. Foreground writes are HOT replication first.
2. Background workers migrate HOT replicas to EC shards.
3. Metadata is centralized in TiKV (through `meta_service` RPC).
4. Repair and old-version cleanup are asynchronous task types.

## 2. Day-1 Success Criteria

You are done when all checks below pass:

1. `./scripts/up_stack.sh` succeeds.
2. `START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh` passes.
3. Manual `PUT -> GET -> DELETE` works for one object.
4. You can explain where write/read/tiering logic is implemented.
5. You add one tiny code change and run `go test ./...`.

## 3. Required Tools

1. Docker and Docker Compose.
2. Go toolchain (for tests and local code changes).
3. `curl`, `bash`, `rg`.

## 4. Start the Stack

```bash
./scripts/up_stack.sh
```

Expected:

1. PD and TiKV boot successfully.
2. `meta_service_1..3` become healthy.
3. API and storage nodes start.
4. Tiering worker starts.

Basic health check:

```bash
curl -sS http://127.0.0.1:8000/health
```

## 5. Run the Core Smoke

```bash
START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh
```

What this smoke verifies:

1. PUT creates metadata and HOT replica placement.
2. Tiering task is enqueued.
3. Worker claims task and promotes version to `EC_ACTIVE`.
4. GET still returns original payload.

## 6. Manual End-to-End Check

### 6.1 PUT raw bytes

```bash
printf 'hello-onboarding\n' >/tmp/onboarding.bin
curl -sS -X PUT 'http://127.0.0.1:8000/v2/objects/onboarding-1' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @/tmp/onboarding.bin
```

### 6.2 GET and compare

```bash
curl -sS 'http://127.0.0.1:8000/v2/objects/onboarding-1' -o /tmp/onboarding.out
cmp /tmp/onboarding.bin /tmp/onboarding.out && echo 'payload ok'
```

### 6.3 Inspect admin views

```bash
curl -sS 'http://127.0.0.1:8000/v2/admin/objects/onboarding-1'
curl -sS 'http://127.0.0.1:8000/v2/admin/tasks?object_id=onboarding-1&limit=20'
```

### 6.4 DELETE

```bash
curl -sS -X DELETE 'http://127.0.0.1:8000/v2/objects/onboarding-1'
```

## 7. Codebase Walkthrough (First Pass)

Read these files in order:

1. `cmd/api/main.go`
2. `internal/writeservice/writeservice.go`
3. `internal/readservice/readservice.go`
4. `cmd/tiering_worker/main.go`
5. `internal/tiering/worker.go`
6. `internal/tiering/policy_scanner.go`
7. `internal/meta/repository.go`
8. `internal/meta/tikv_store_*.go`

## 8. First Commit Suggestions

Safe day-1 changes:

1. Add a log line with clear context in one worker processor.
2. Add input validation for one admin query parameter.
3. Add one unit test around task-state transition.

Before commit:

```bash
go test ./...
```

## 9. Common Day-1 Blockers

1. Port `8000` already used: stop conflicting service/container.
2. TiKV readiness delayed: wait and check `docker logs ...-tikv-1`.
3. Metadata health flaps: verify `meta_service` and RPC token settings.

## 10. Day-1 Exit Checklist

1. Smoke passes.
2. You can draw runtime flow from memory.
3. One tiny commit is created with test pass.
4. You know where to read next: `docs/explanation/system-architecture-and-responsibilities.md`.
