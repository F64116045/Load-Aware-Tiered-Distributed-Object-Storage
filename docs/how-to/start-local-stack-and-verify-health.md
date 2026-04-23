# How-to: Run the Full Stack Locally

Scope: single-command local startup and baseline runtime verification.

## 1. Start

```bash
./scripts/up_stack.sh
```

What it does:

1. starts PD + TiKV
2. waits TiKV readiness
3. starts 3 meta_service replicas + meta LB
4. starts API, storage nodes, tiering worker, nginx

## 2. Verify

```bash
curl -sS http://127.0.0.1:8000/health
curl -sS 'http://127.0.0.1:8000/v2/admin/nodes?limit=20'
```

## 3. Run smoke

```bash
START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh
```

## 4. Stop

```bash
docker compose -f docker-compose.yaml down
```

## 5. Hard reset (destructive)

```bash
docker compose -f docker-compose.yaml down -v
```

Use only when test data can be discarded.

## 6. Related Documents

1. [Local Setup and Smoke Validation](../operations/local-setup-and-smoke-validation.md)
2. [Incident Triage, Restart, and Recovery Runbook](../operations/incident-triage-restart-and-recovery-runbook.md)
3. [Recover from TiKV Startup Failure](recover-from-tikv-startup-failure.md)
