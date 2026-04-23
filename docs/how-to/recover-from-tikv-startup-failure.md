# How-to: Recover from TiKV Startup Failure

Typical symptoms:

1. `meta_service` readiness timeout
2. TiKV repeatedly restarting
3. region request timeout in logs

## 1. Check TiKV readiness markers

```bash
docker logs replication_erasurecoding_object_store-tikv-1 --tail 400
```

Look for:

1. `TiKV is ready to serve` (healthy)
2. repeated panic/fatal or endless recovery loops (unhealthy)

## 2. Controlled restart

```bash
docker compose -f docker-compose.yaml down
docker compose -f docker-compose.yaml up -d pd tikv
```

Wait until TiKV ready, then start rest:

```bash
./scripts/up_stack.sh
```

## 3. If volumes are corrupted (lab environment)

```bash
docker compose -f docker-compose.yaml down -v
./scripts/up_stack.sh
```

Warning: this removes all local metadata/blob test data.

## 4. Verify recovery

```bash
START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh
```

## 5. Related Documents

1. [Run the Full Stack Locally](start-local-stack-and-verify-health.md)
2. [Incident Triage, Restart, and Recovery Runbook](../operations/incident-triage-restart-and-recovery-runbook.md)
3. [System Dependencies and Runtime Topology Reference](../reference/system-dependencies-and-runtime-topology-reference.md)
