# API Reference

Base URL: `http://localhost:8000`

Migration note (Docker workflow):
- Run metadata migrations before starting/rolling API after schema changes:
  - `docker compose run --rm meta_migrate`
- One-command v2 smoke flow:
  - `scripts/smoke_e2e_v2.sh`

Runtime profile note (Docker workflow):
- Default profile is postgres-first and does not require Redpanda/WAL services.

Build note:
- Current mainline build no longer includes legacy `field_hybrid` implementation paths.

## Data Plane Endpoints

### 1. Write Data

Writes a JSON object or binary data to the distributed store.

- **URL**: `/write`
- **Method**: `POST`
- **Headers**: `Content-Type: application/json`
- **Query Parameters**:
    - `key` (Required): Unique identifier for the object.
    - `strategy` (Optional): `replication` only (default: `replication`).
- **Deprecation note**:
  - direct `ec` write via `/write` is deprecated; use replication write + background tiering worker.

**Request Body (Example)**:

```
{
    "user_id": 12345,
    "name": "Alice",
    "bio": "A very long string that will be erasure coded...",
    "login_count": 42
}

```

**Success Response (200 OK)**:

```
{
    "status": "ok",
    "key": "user:12345",
    "strategy": "replication",
    "latency_ms": 15,
    "nodes_written": [
      "http://storage_node_1:8001",
      "http://storage_node_2:8002",
      "http://storage_node_3:8003"
    ],
    "partial": false
}

```

### 2. Read Data

Retrieves data. The system automatically determines the storage strategy and reconstructs the object.

- **URL**: `/read/:key`
- **Method**: `GET`
- **Note**:
  - `field_hybrid` strategy is removed from mainline implementation.

**Success Response (200 OK)**:
Returns the original JSON object.

**Error Response (404 Not Found)**:

```
{
    "detail": "Key 'unknown' not found"
}

```

### 3. Delete Data

Permanently removes the object metadata and physical files.

- **URL**: `/delete/:key`
- **Method**: `DELETE`
- **Note**:
  - `field_hybrid` strategy is removed from mainline implementation.

**Success Response (200 OK)**:

```
{
    "status": "ok",
    "key": "user:12345",
    "strategy": "replication",
    "nodes_deleted": 3
}

```

## Monitoring Endpoints

### 4. Node Health

**Node Health**:

- **URL**: `/node_status`
- **Method**: `GET`
- **Response**: Returns the status (Health/Size/Ops) of all active storage nodes.

### 5. Storage Usage

**Storage Usage**:

- **URL**: `/storage_usage`
- **Method**: `GET`
- **Response**: Aggregated disk usage statistics.

### 6. API Health

- **URL**: `/health`
- **Method**: `GET`
- **Response**: API health, metadata health/source, node discovery source.

### 7. Metrics Snapshot

- **URL**: `/v2/admin/metrics-snapshot`
- **Method**: `GET`
- **Response**: metadata lookup counters + node discovery counters + tiering leader snapshot.

### 8. Tiering Leader Snapshot

- **URL**: `/v2/admin/leader`
- **Method**: `GET`
- **Response**:
  - `lock_key`
  - `leader` (or `null` when no leader heartbeat yet):
    - `leader_id`
    - `scanner_status` (`LEADING` / `STOPPED` / `LOCK_LOST`)
    - `acquired_at`, `last_heartbeat_at`
    - `last_heartbeat_ago_sec`, `is_stale`, `stale_sec`

## Admin v2 Endpoints

### 9. List Tiering Tasks

- **URL**: `/v2/admin/tasks`
- **Method**: `GET`
- **Query Parameters**:
  - `state` (Optional): filter by task state, e.g. `PENDING`, `RUNNING`, `DONE`, `FAILED`, `RETRY_WAIT`.
  - `task_type` (Optional): filter by type, currently `REPL_TO_EC` and `GC`.
  - `limit` (Optional): max rows returned, default `100`, max `1000`.

**Response fields**:
- `filters`: effective query filters.
- `state_counts`: aggregated counts by state (filtered by `task_type` if provided).
- `tasks[]`: task records with timestamps and action hints.
  - `actions.retry_now` indicates if `POST /v2/admin/tasks/:id/retry-now` is allowed.
  - `actions.cancel` indicates if `POST /v2/admin/tasks/:id/cancel` is allowed.

### 10. Requeue Task Immediately

- **URL**: `/v2/admin/tasks/:id/retry-now`
- **Method**: `POST`
- **Behavior**:
  - sets task to `PENDING`
  - sets `scheduled_at=NOW()`
  - clears `started_at`, `finished_at`, `last_error`
- **Allowed states**: `PENDING`, `RUNNING`, `RETRY_WAIT`, `FAILED`.
- **Not allowed**: `DONE`.

### 11. Cancel Task

- **URL**: `/v2/admin/tasks/:id/cancel`
- **Method**: `POST`
- **Query Parameters / JSON Body**:
  - `reason` (Optional)
  - also accepted in body: `{"reason":"..."}`.
- **Behavior**:
  - sets task to `FAILED`
  - writes reason to `last_error`
  - sets `finished_at=NOW()`
- **Allowed states**: `PENDING`, `RUNNING`, `RETRY_WAIT`.
- **Not allowed**: `DONE`.

### 12. List Node Heartbeats

- **URL**: `/v2/admin/nodes`
- **Method**: `GET`
- **Query Parameters**:
  - `limit` (Optional): default `100`, max `1000`.
- **Response fields**:
  - `node_id`, `status`, `last_seen_at`, `is_stale`
  - `free_bytes`, `io_queue_depth`, `cpu_load`
  - `stale_sec` (current staleness threshold).

### 13. Get Object Metadata and Placement

- **URL**: `/v2/admin/objects/:id`
- **Method**: `GET`
- **Response fields**:
  - object header: `object_id`, `state`, `current_version`, `created_at`, `updated_at`
  - current version metadata: `tier`, `checksum_sha256`, `encoding_k`, `encoding_m`, `size_bytes`
  - `replica_locations[]` for current version
  - `ec_shard_locations[]` for current version

## v2 Generic Object Endpoints (Binary, Replication-First)

### 14. Put Object (Binary)

- **URL**: `/v2/objects/:id`
- **Method**: `PUT`
- **Body**: raw bytes (`--data-binary`)
- **Current behavior**:
  - stores object using replication strategy (HOT tier)
  - does not require JSON
- **Notes**:
  - `Content-Type` is persisted in normalized metadata (`object_versions.content_type`) after running latest metadata migration.

### 15. Get Object (Binary)

- **URL**: `/v2/objects/:id`
- **Method**: `GET`
- **Response**: raw bytes
- **Response Header**:
  - `Content-Type` is returned from normalized metadata when available; fallback is `application/octet-stream`.
- **Supported object strategies (current)**:
  - `replication`
  - `ec`
- **Not supported in this endpoint (current)**:
  - removed legacy strategies
