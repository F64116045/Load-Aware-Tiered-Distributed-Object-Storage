# internal/meta (TiKV-first Metadata Layer)

`internal/meta` is the metadata/control-plane backend used by API service,
tiering worker, and storage nodes.

## Scope

- object metadata CRUD
- tiering task queue and indexes
- due-index candidate source for scanner
- node heartbeat persistence
- scanner leader lock and leader-state observability
- RPC boundary (`meta_service`) and repository abstraction

## File Responsibilities

- `store.go`: repository bootstrap and backend selector (`tikv` / `rpc`)
- `repository.go`: metadata contract used by runtime components
- `rpc_client.go`, `rpc_server.go`, `rpc_protocol.go`: metadata RPC transport
- `tikv_store.go`: TiKV store lifecycle + lease APIs
- `tikv_store_keys.go`: key builders and ordered key encoding
- `tikv_store_schema.go`: internal TiKV value models
- `tikv_store_objects.go`: normalized metadata CRUD + admin object view
- `tikv_store_tasks.go`: enqueue/claim/state transition/admin task operations
- `tikv_store_task_index.go`: task runnable/wait/terminal indexes
- `tikv_store_policy.go`: scanner enqueue strategies (A/B/C) and repair enqueue
- `tikv_store_due_index.go`: due-index write/read/remove primitives
- `tikv_store_migration.go`: migration state transitions and EC promotion commit
- `tikv_store_old_version_gc.go`: old-version metadata GC primitives
- `tikv_store_cluster.go`: heartbeats and leader-state records
- `tikv_store_lock.go`: leader lock object (`Ping`/`Release`)
- `kvstore/*`: TiKV/memory client abstraction (`Client`, `Batch`, `Iterator`)

## Metadata Runtime Lifecycle

1. API write path commits object/version/placement metadata and due-index records.
2. Scanner reads due-index and enqueues deterministic tasks.
3. Worker claims runnable tasks and transitions state to `RUNNING`.
4. Processor updates metadata (`MIGRATING`, `EC_ACTIVE`, repair, GC updates).
5. Task result transitions to `DONE`, `RETRY_WAIT`, or `FAILED`.

## Concurrency and Consistency Model

1. Per-store local serialization uses store-level `mu` (`sync.RWMutex`).
2. Multi-key updates use one `Batch` commit for atomic row/index transition.
3. Cross-replica conflict handling relies on TiKV transaction commit semantics.
4. Task delivery guarantee is `at-least-once`; processors must be idempotent.
5. Stale task protection is version-based (`task.version` vs `object.current_version`).

## Keyspace Reference

Primary prefixes:

- `obj/`, `objv/`, `repl/`, `ec/`
- `task/`, `task_ready/`, `task_wait/`, `task_terminal/`
- `tdue/`, `tdue_ref/`
- `hb/`, `leader/`, `leader_lock/`

Detailed schema and encoding:

- [docs/reference/tikv-keyspace-and-key-encoding-reference.md](../../docs/reference/tikv-keyspace-and-key-encoding-reference.md)
- [docs/reference/metadata-record-schema-reference.md](../../docs/reference/metadata-record-schema-reference.md)
- [docs/reference/logical-data-schema-reference.md](../../docs/reference/logical-data-schema-reference.md)

## Related Documentation

- [Scanner Leader Lock Mechanism](../../docs/explanation/scanner-leader-lock-mechanism.md)
- [Tiering Task Path from PUT to Worker Claim](../../docs/explanation/tiering-task-path-from-put-to-worker-claim.md)
- [Task State Machine Reference](../../docs/reference/task-state-machine-reference.md)
- [Metadata RPC Method Mapping Reference](../../docs/reference/metadata-rpc-method-mapping-reference.md)
