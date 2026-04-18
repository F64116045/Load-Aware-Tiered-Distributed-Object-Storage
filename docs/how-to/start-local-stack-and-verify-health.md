# How-to: Run the Full Stack Locally

Use this when you want a single command local environment for development.

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
