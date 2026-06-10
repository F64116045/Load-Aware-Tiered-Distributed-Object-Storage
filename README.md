# Load-Aware Asynchronous Tiering for Distributed Object Storage - A Study and Implementation of Load-Aware Asynchronous Migration (資訊專題)

A TiKV-backed object storage prototype for studying load-aware asynchronous tiering.

Foreground requests serve HOT replicated objects, while background workers migrate eligible objects to EC.
We compare three scheduling policies (A: age-based, B: budget-limited, C: pressure-aware idle-window)
and evaluate their trade-offs on P99 latency, throughput, migration efficiency, and space savings
under resource-constrained conditions.



## Quick Start

Prerequisites:

- Docker + Docker Compose
- `curl`

Start stack:

```bash
./scripts/up_stack.sh
```

Health check:

```bash
curl -sS http://127.0.0.1:8000/health
```

Run core smoke (first run / bootstrap):

```bash
START_STACK=true ./scripts/smoke_e2e_v2_tikv.sh
```

Fast re-run (only when current stack was started with the same smoke compose settings):

```bash
START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh
```

Notes:

- `START_STACK=false` reuses the current running stack and does not apply smoke overrides.
- If you started stack via plain `./scripts/up_stack.sh` (default `AGE_THRESHOLD_SEC=3600`), run one bootstrap with `START_STACK=true` first.

Stop stack:

```bash
docker compose -f docker-compose.yaml down
```

## API Quick Reference

### Object API (`/v2/objects`)

| Method | Path | Purpose | Success | Common failure |
| --- | --- | --- | --- | --- |
| `PUT` | `/v2/objects/:id` | Write HOT replicas and commit metadata | `201` | `400` invalid id/body, `500` quorum/metadata failure |
| `GET` | `/v2/objects/:id` | Read by current metadata strategy (`replication` or `ec`) | `200` | `404` not found, `409` non-binary strategy |
| `DELETE` | `/v2/objects/:id` | Delete physical data and metadata | `200` | `404` not found, `500` delete/metadata failure |

Minimal request flow:

```bash
# PUT
printf 'hello-v2\n' >/tmp/payload.bin
curl -sS -X PUT \
  'http://127.0.0.1:8000/v2/objects/demo-001' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @/tmp/payload.bin

# GET
curl -sS 'http://127.0.0.1:8000/v2/objects/demo-001' -o /tmp/out.bin

# DELETE
curl -sS -X DELETE 'http://127.0.0.1:8000/v2/objects/demo-001'
```

### Admin API (`/v2/admin`)

| Method | Path | Purpose | Key query/body |
| --- | --- | --- | --- |
| `GET` | `/v2/admin/nodes` | Node heartbeat snapshots and pressure fields | `limit` |
| `GET` | `/v2/admin/tasks` | Task list and state counts | `limit`, `state`, `task_type`, `object_id` |
| `POST` | `/v2/admin/tasks/:id/retry-now` | Force task to runnable state immediately | none |
| `POST` | `/v2/admin/tasks/:id/cancel` | Cancel task to terminal state | `reason` (query or JSON body) |
| `GET` | `/v2/admin/objects/:id` | Object admin view (head/version/placements) | none |
| `GET` | `/v2/admin/leader` | Scanner leader and lock observability | none |
| `GET` | `/v2/admin/metrics-snapshot` | Runtime metrics snapshot | none |

Common admin checks:

```bash
curl -sS 'http://127.0.0.1:8000/v2/admin/nodes?limit=20'
curl -sS 'http://127.0.0.1:8000/v2/admin/tasks?limit=50'
curl -sS 'http://127.0.0.1:8000/v2/admin/objects/demo-001'
curl -sS 'http://127.0.0.1:8000/v2/admin/leader'
curl -sS 'http://127.0.0.1:8000/v2/admin/metrics-snapshot'
```

Full endpoint contract:

- [docs/reference/api-endpoints-reference.md](docs/reference/api-endpoints-reference.md)

## Tiering Policy Knobs

Core strategy mode:

- `TIERING_POLICY_VARIANT` (`A`, `B`, `C`)
- Strategy A (time-based baseline): periodic migration by age eligibility
- Strategy B (static throttling): fixed migration budgets/concurrency caps
- Strategy C (idle-window gating): migrate only when cluster load remains below safety thresholds for N consecutive checks

Core budget and admission:

- `AGE_THRESHOLD_SEC`
- `MAX_OBJECTS_PER_ROUND`
- `MAX_BYTES_PER_ROUND`
- `TIERING_IDLE_STABLE_ROUNDS`
- `TIERING_IDLE_CPU_PCT`
- `TIERING_IDLE_MEMORY_PCT`
- `TIERING_IDLE_IOWAIT_PCT`
- `TIERING_IDLE_QUEUE_DEPTH`

## Smoke and Validation Scripts

- `scripts/smoke_e2e_v2_tikv.sh`
- `scripts/smoke_leader_failover_tikv.sh`
- `scripts/smoke_policy_idle_window.sh`
- `scripts/smoke_matrix.sh`


## Documentation Entry

Main documentation index:

- [docs/README.md](docs/README.md)

Recommended start:

- [System Architecture and Responsibilities](docs/explanation/system-architecture-and-responsibilities.md)
- [Request and Task Lifecycles](docs/explanation/put-get-delete-and-task-lifecycles.md)
- [Tiering Policy Strategies and Trigger Modes](docs/explanation/tiering-policy-strategies-and-trigger-modes.md)
- [Task State Machine Reference](docs/reference/task-state-machine-reference.md)
- [TiKV Keyspace and Key Encoding Reference](docs/reference/tikv-keyspace-and-key-encoding-reference.md)


