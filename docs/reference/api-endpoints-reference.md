# API Reference

This is the authoritative endpoint catalog for current runtime behavior.

## 1. Public Object API (v2)

### 1.1 `PUT /v2/objects/:id`

Purpose:

1. write object as HOT replication.
2. commit metadata and due-index records (tiering/repair tasks are scanner-enqueued later).

Request:

1. path param `:id` (required)
2. body: raw bytes
3. `Content-Type` optional (defaults to `application/octet-stream`)

Success response:

1. status `201 Created`
2. fields include:
   - `status`
   - `object_id`
   - `tier` (`HOT`)
   - `strategy` (`replication`)
   - `size_bytes`
   - `content_type`
   - `latency_ms`
   - `nodes_written`
   - `write_quorum`
   - `writes_success`

Failure response:

1. `400` invalid object id/body
2. `500` write quorum failure or metadata failure

### 1.2 `GET /v2/objects/:id`

Purpose:

1. return object bytes from current strategy.

Behavior:

1. API resolves metadata strategy.
2. strategy `replication`: reads HOT replica key.
3. strategy `ec`: reads/reconstructs EC shards.

Response:

1. `200 OK` + raw bytes
2. `Content-Type` from metadata if available

Errors:

1. `400` invalid object id
2. `404` metadata/object not found or data not retrievable
3. `409` unsupported strategy for binary read

### 1.3 `DELETE /v2/objects/:id`

Purpose:

1. delete physical data and metadata for object current strategy.

Response:

1. `200 OK`
2. includes deleted counts and `latency_ms`

Errors:

1. `404` object not found
2. `500` delete or metadata cleanup failure

## 2. Admin API

### 2.1 `GET /health`

API gateway liveness with metadata/discovery status summary.

### 2.2 `GET /v2/admin/metrics-snapshot`

Returns:

1. metadata lookup counters
2. node discovery snapshot
3. tiering due-index stats
4. old-version reaper task counts
5. leader state summary

### 2.3 `GET /v2/admin/leader`

Returns scanner lock state:

1. `leader_id`
2. `scanner_status`
3. `last_heartbeat_at`
4. stale calculation (`is_stale`)

### 2.4 `GET /v2/admin/nodes?limit=<n>`

Returns heartbeat snapshots with derived utilization:

1. `free_bytes`, `total_bytes`, `used_pct`
2. `io_queue_depth`, `cpu_load`
3. staleness flag

### 2.5 `GET /v2/admin/objects/:id`

Returns full object admin view:

1. head record (`state`, `current_version`)
2. current version metadata
3. replica placements
4. EC shard placements

### 2.6 `GET /v2/admin/tasks`

Query params:

1. `state`
2. `task_type`
3. `object_id`
4. `limit`

Returns:

1. filtered task list
2. task-state counts
3. per-task actions (`retry_now`, `cancel`)

### 2.7 `POST /v2/admin/tasks/:id/retry-now`

Forces task runnable now.

### 2.8 `POST /v2/admin/tasks/:id/cancel`

Cancels task.

Reason can be provided by:

1. query string `reason=...`
2. JSON body `{ "reason": "..." }`

## 3. Legacy API (Compatibility)

Current legacy endpoints still present:

1. `POST /write`
2. `GET /read/:key`
3. `DELETE /delete/:key`
4. `GET /node_status`
5. `GET /storage_usage`

Use v2 endpoints for all new development.

## 4. Storage Node Internal API

These endpoints are internal data-plane primitives:

1. `POST /store?key=<blob_key>`
2. `GET /retrieve/*key`
3. `HEAD /retrieve/*key`
4. `DELETE /delete/*key`
5. `GET /health`
6. `GET /info`

## 5. Related Documents

1. [Request and Task Lifecycles](../explanation/put-get-delete-and-task-lifecycles.md)
2. [Metadata RPC Method Mapping Reference](metadata-rpc-method-mapping-reference.md)
3. [Configuration Env Vars Reference](configuration-env-vars-reference.md)
