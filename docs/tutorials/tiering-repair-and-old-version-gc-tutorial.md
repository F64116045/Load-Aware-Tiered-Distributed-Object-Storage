# Tutorial 02: Observe Tiering, Repair, and Old-Version Cleanup

Audience: contributors validating control-plane behavior.

Goal: watch background task lifecycle, leader behavior, and maintenance loops.

## 1. Start Stack and Create Test Data

```bash
./scripts/up_stack.sh
START_STACK=false ./scripts/smoke_e2e_v2_tikv.sh
```

## 2. Inspect Leader State

```bash
curl -sS 'http://127.0.0.1:8000/v2/admin/leader'
```

Key fields:

1. leader id
2. status
3. stale decision

## 3. Inspect Task Queue Distribution

```bash
curl -sS 'http://127.0.0.1:8000/v2/admin/tasks?limit=100'
```

What you should see over time:

1. `PENDING -> RUNNING -> DONE`
2. transient failures become `RETRY_WAIT` then retry

## 4. Trigger Retry for a Task

```bash
curl -sS -X POST 'http://127.0.0.1:8000/v2/admin/tasks/<TASK_ID>/retry-now'
```

Expected:

1. API reports whether task was requeued.
2. worker picks it on next polling cycle.

## 5. Observe Node Health Inputs Used by Policies

```bash
curl -sS 'http://127.0.0.1:8000/v2/admin/nodes?limit=20'
```

Focus on:

1. queue depth
2. cpu/memory/iowait
3. heartbeat freshness

## 6. Observe Old-Version Reaper Effects

Create multiple writes on the same object id:

```bash
for i in 1 2 3 4; do
  printf "version-%s\n" "$i" >/tmp/v.bin
  curl -sS -X PUT 'http://127.0.0.1:8000/v2/objects/reaper-demo' --data-binary @/tmp/v.bin >/dev/null
  sleep 1
done
```

Inspect object admin view and tasks:

```bash
curl -sS 'http://127.0.0.1:8000/v2/admin/objects/reaper-demo'
curl -sS 'http://127.0.0.1:8000/v2/admin/tasks?object_id=reaper-demo&limit=50'
```

Look for:

1. old versions generating `GC_OLD_VERSION` tasks
2. eventual metadata cleanup for non-retained versions
