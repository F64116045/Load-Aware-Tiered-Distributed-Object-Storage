# Runbook (Operations and Incident Response)

Scope: production-style triage and recovery for local/compose runtime incidents.

Objective: restore service safely with deterministic triage and recovery steps.

## 1. Stack Control

Start full stack:

```bash
./scripts/up_stack.sh
```

Stop stack:

```bash
docker compose -f docker-compose.yaml down
```

Stop and remove volumes (destructive):

```bash
docker compose -f docker-compose.yaml down -v
```

## 2. 5-Minute Triage Flow

Use this exact order:

1. API liveness: `curl -sS http://127.0.0.1:8000/health`
2. Metadata liveness/readiness:
   - `docker compose -f docker-compose.yaml exec -T meta_service_1 wget -q -O - http://127.0.0.1:8091/health`
   - `docker compose -f docker-compose.yaml exec -T meta_service_1 wget -q -O - http://127.0.0.1:8091/ready`
3. Node health: `curl -sS 'http://127.0.0.1:8000/v2/admin/nodes?limit=20'`
4. Leader status: `curl -sS 'http://127.0.0.1:8000/v2/admin/leader'`
5. Task pressure: `curl -sS 'http://127.0.0.1:8000/v2/admin/tasks?limit=50'`
6. Worker errors: `docker logs replication_erasurecoding_object_store-tiering_worker-1 --tail 200`

## 3. High-Value Diagnostics

API/metadata:

```bash
docker logs replication_erasurecoding_object_store-api-1 --tail 200
docker logs replication_erasurecoding_object_store-meta_service-1 --tail 200
```

Data plane:

```bash
docker logs replication_erasurecoding_object_store-storage_node_1-1 --tail 200
docker logs replication_erasurecoding_object_store-storage_node_2-1 --tail 200
```

TiKV/PD:

```bash
docker logs replication_erasurecoding_object_store-pd-1 --tail 200
docker logs replication_erasurecoding_object_store-tikv-1 --tail 200
```

## 4. Incident Playbooks

### 4.1 API Port Conflict (`:8000`)

Symptom:

1. `Bind for 0.0.0.0:8000 failed: port is already allocated`

Actions:

1. find owner: `docker ps --format '{{.Names}} {{.Ports}}' | rg 8000`
2. stop conflicting process/container
3. restart stack

### 4.2 meta_service Unhealthy During Startup

Symptom:

1. `metadata ping failed: tikv ping get failed: context deadline exceeded`

Actions:

1. verify TiKV eventually reaches `TiKV is ready to serve`
2. check PD health in logs
3. retry clean restart:

```bash
docker compose -f docker-compose.yaml down
docker compose -f docker-compose.yaml up -d pd tikv
./scripts/up_stack.sh
```

### 4.3 Scanner Leader Flapping

Symptom:

1. repeated `leader lock session lost`

Actions:

1. inspect meta_service errors and RPC latency
2. run leader failover smoke:

```bash
START_STACK=false ./scripts/smoke_leader_failover_tikv.sh
```

3. verify lock token state through admin endpoint:

```bash
curl -sS 'http://127.0.0.1:8000/v2/admin/leader'
```

### 4.4 Task Queue Backlog Grows Continuously

Symptoms:

1. many `PENDING`/`RETRY_WAIT` tasks
2. low `DONE` growth

Actions:

1. check worker availability and logs
2. verify storage node health
3. increase worker count temporarily:

```bash
docker compose -f docker-compose.yaml up -d --scale tiering_worker=2 tiering_worker
```

4. inspect oldest failing tasks via admin API

### 4.5 TiKV Data Corruption After Crash

Symptom:

1. TiKV logs show raft log truncation/recovery warnings repeatedly and service cannot stabilize

Actions:

1. stop stack
2. if data is non-critical lab data, reset volumes and cold start
3. rerun smoke to verify clean state

## 5. Rollback Strategy

Current practical rollback strategy (lab profile):

1. roll back to previous known-good Git commit
2. rebuild containers
3. recreate stack
4. run smoke tests before accepting traffic

## 6. Post-Incident Checklist

1. Capture exact failing logs.
2. Record timeline and impact.
3. Add/adjust smoke test when bug class was missing.
4. Update this runbook if steps were unclear.

## 7. Related Documents

1. [Debug Scanner Leader Lock Flapping](../how-to/debug-scanner-leader-lock-flapping.md)
2. [Recover from TiKV Startup Failure](../how-to/recover-from-tikv-startup-failure.md)
3. [Start Local Stack and Verify Health](../how-to/start-local-stack-and-verify-health.md)
