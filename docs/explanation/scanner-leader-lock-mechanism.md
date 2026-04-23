# Explanation: Scanner Leader Lock Mechanism

This document defines scanner leadership control behavior.

It explains:

1. why the lock exists
2. exact acquire/renew/release behavior
3. local TiKV and remote meta RPC paths
4. timing model and failure boundaries

Source files:

1. [`cmd/tiering_worker/main.go`](../../cmd/tiering_worker/main.go)
2. [`internal/meta/tikv_store.go`](../../internal/meta/tikv_store.go)
3. [`internal/meta/tikv_store_lock.go`](../../internal/meta/tikv_store_lock.go)
4. [`internal/meta/kvstore/client.go`](../../internal/meta/kvstore/client.go)
5. [`internal/meta/rpc_client.go`](../../internal/meta/rpc_client.go)
6. [`internal/meta/rpc_server.go`](../../internal/meta/rpc_server.go)
7. [`internal/meta/tikv_store_cluster.go`](../../internal/meta/tikv_store_cluster.go)

## 1. Purpose and Scope

The lock exists to ensure:

1. only one scanner instance enqueues policy tasks at a time
2. many task workers can still run concurrently

Important boundary:

1. this lock controls scanner leadership only
2. task claim and task execution are not protected by this lock

## 2. Data Keys and Authority

Two keyspaces are involved:

1. `leader_lock/<lock_key>`: lease authority (real lock)
2. `leader/<lock_key>`: observability snapshot (admin view)

Authority rule:

1. scanner leadership is decided only by `leader_lock/*`
2. `leader/*` can lag and is not lock truth

## 3. Lock Value Model

`leader_lock/*` stores JSON payload:

```text
owner: string
expires_at_unix_nano: int64
```

Behavior implications:

1. owner token identifies lock owner session
2. expiry controls takeover after lease timeout

## 4. End-to-End Lifecycle

### 4.1 Attempt acquire

`runScannerAsLeader` calls `TryAcquireLeaderLock(lockKey)`:

1. generate random owner token
2. if lock key not found, write new lease and acquire
3. if found and not expired, acquire fails
4. if found but expired, overwrite and take over
5. TiKV write conflict during race is treated as "not acquired"

### 4.2 Become leader

After acquire succeeds:

1. keep `LeaderLock` handle in memory
2. write `leader/<lock_key>` with `scanner_status=LEADING`
3. start `scanner.Run(scannerCtx)` in background goroutine

### 4.3 Renew lease (ping)

Every retry tick:

1. call `lock.Ping()`
2. backend validates same owner and unexpired lease
3. if valid, extends expiry to `now + ttl`
4. update `leader/*` heartbeat

### 4.4 Lost lock

If ping fails or owner no longer valid:

1. stop scanner immediately
2. mark `leader/*` status as `LOCK_LOST`
3. release local lock handle
4. retry election on next tick

### 4.5 Graceful shutdown

On process stop:

1. cancel scanner context
2. mark `leader/*` as `STOPPED`
3. call lock release

If process crashes before release:

1. no explicit release happens
2. others acquire after lease expiration

## 5. Local TiKV Path (No META_ENDPOINT)

Path:

1. `Repository` is `TiKVStore`
2. `TryAcquireLeaderLock` returns `tiKVLeaderLock`
3. `Ping` maps to `RefreshLock`
4. `Release` maps to `ReleaseLock`

Current lease TTL:

1. fixed at 10 seconds in `NewTiKVStore`
2. not currently exposed as env variable

## 6. Remote Meta RPC Path (META_ENDPOINT)

Path:

1. worker uses `RPCClient`
2. acquire via `try_acquire_leader_lock`
3. server acquires lease in backend and returns lock token
4. renew via `leader_lock_ping` with token
5. release via `leader_lock_release` with token

Token contents:

1. `lock_key`
2. encoded `owner` token
3. optional HMAC signature using `META_RPC_AUTH_TOKEN`

Why this matters:

1. lock state is stored in backend, not in one `meta_service` process
2. ping/release can hit different `meta_service` replicas behind LB

## 7. Timing Model

Relevant timings:

1. lease ttl: 10s (backend lock value)
2. election retry ticker: `TIERING_POLICY_LEADER_RETRY_SEC` (default 2s)
3. acquire RPC timeout: 2s
4. ping timeout: 1s
5. leader state update timeout: 2s

Operational reading:

1. retry interval should stay meaningfully smaller than ttl
2. otherwise false lock-loss and leadership flapping become easier

## 8. Failure Semantics

### 8.1 Split-brain protection

Owner check + expiry window enforce a lease model:

1. only current owner can renew/release
2. takeover requires expiration

### 8.2 Observability drift

`leader/*` may temporarily show stale status during failures.

Correct interpretation:

1. trust `leader_lock/*` behavior as authority
2. treat `leader/*` as an admin diagnostic snapshot

### 8.3 Network instability

If ping cannot finish within timeout:

1. current leader may self-demote
2. scanner stops and election retries
3. repeated instability appears as lock flapping

## 9. Interaction with Task Workers

This lock does not serialize task workers:

1. all workers still run claim loop
2. lock only decides who runs scanner enqueue loop

So:

1. "single scanner"
2. "multi worker" can coexist by design

## 10. Troubleshooting Procedure

1. check `/v2/admin/leader` for leader id churn and stale oscillation
2. inspect `tiering_worker` logs for repeated `leader lock session lost`
3. inspect `meta_service` and TiKV logs for latency/conflict/network errors
4. verify all workers share same lock key and auth token settings
5. temporarily reduce worker count to isolate flapping source

Related documents:

1. [Runtime Control Loops and Schedulers](runtime-control-loops-and-schedulers.md)
2. [Debug Scanner Leader Lock Flapping](../how-to/debug-scanner-leader-lock-flapping.md)
3. [Metadata RPC Method Mapping Reference](../reference/metadata-rpc-method-mapping-reference.md)
4. [TiKV Keyspace and Key Encoding Reference](../reference/tikv-keyspace-and-key-encoding-reference.md)
