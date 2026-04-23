# How-to: Run HA Metadata Profile (3 PD + 3 TiKV)

Scope: HA metadata profile startup, verification, and shutdown.

## 1. Start with HA compose overlay

```bash
docker compose -f docker-compose.yaml -f docker-compose.ha.yaml up -d
```

## 2. Verify core metadata components

```bash
docker compose -f docker-compose.yaml -f docker-compose.ha.yaml ps pd pd2 pd3 tikv tikv2 tikv3 meta_service_1 meta_service_2 meta_service_3
```

## 3. Run HA smoke

```bash
START_STACK=false ./scripts/smoke_e2e_v2_tikv_ha.sh
START_STACK=false ./scripts/smoke_leader_failover_tikv_ha.sh
```

## 4. Stop HA stack

```bash
docker compose -f docker-compose.yaml -f docker-compose.ha.yaml down
```

## 5. Common HA pitfalls

1. old single-node volumes mixed with HA startup sequence
2. stale container network from previous failed boot
3. insufficient machine resources for 3PD+3TiKV on laptop

## 6. Related Documents

1. [Run the Full Stack Locally](start-local-stack-and-verify-health.md)
2. [Recover from TiKV Startup Failure](recover-from-tikv-startup-failure.md)
3. [System Dependencies and Runtime Topology Reference](../reference/system-dependencies-and-runtime-topology-reference.md)
