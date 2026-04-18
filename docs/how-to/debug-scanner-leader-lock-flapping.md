# How-to: Debug Scanner Leader Lock Flapping

Symptom:

1. `tiering_worker` repeatedly logs `leader lock session lost`
2. scanner repeatedly starts/stops

## 1. Confirm Current Leader View

```bash
curl -sS 'http://127.0.0.1:8000/v2/admin/leader'
```

Check:

1. leader id changes too frequently
2. stale flag oscillates

## 2. Inspect Worker Logs

```bash
docker logs replication_erasurecoding_object_store-tiering_worker-1 --tail 300
```

## 3. Inspect meta_service RPC Errors

```bash
docker logs replication_erasurecoding_object_store-meta_service-1 --tail 300
```

## 4. Validate metadata backend stability

```bash
docker logs replication_erasurecoding_object_store-tikv-1 --tail 300
docker logs replication_erasurecoding_object_store-pd-1 --tail 300
```

## 5. Immediate Mitigations

1. increase lock ping and retry tolerance
2. reduce metadata RPC latency hotspots
3. keep one worker instance temporarily for isolation

## 6. Regression Test

```bash
START_STACK=false ./scripts/smoke_leader_failover_tikv.sh
```

Pass condition:

1. lock failover occurs without scanner permanent outage
