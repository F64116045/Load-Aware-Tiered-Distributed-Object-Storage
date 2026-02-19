CREATE TABLE IF NOT EXISTS objects (
    object_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    current_version BIGINT NOT NULL,
    state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS object_versions (
    object_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    tier TEXT NOT NULL,
    encoding_k INT,
    encoding_m INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (object_id, version)
);

CREATE TABLE IF NOT EXISTS replica_locations (
    object_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    path TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (object_id, version, node_id)
);

CREATE TABLE IF NOT EXISTS ec_shard_locations (
    object_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    shard_index INT NOT NULL,
    node_id TEXT NOT NULL,
    path TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (object_id, version, shard_index)
);

CREATE TABLE IF NOT EXISTS tiering_tasks (
    task_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    task_type TEXT NOT NULL,
    task_state TEXT NOT NULL,
    priority INT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS node_heartbeats (
    node_id TEXT PRIMARY KEY,
    last_seen_at TIMESTAMPTZ NOT NULL,
    free_bytes BIGINT NOT NULL,
    io_queue_depth INT NOT NULL,
    cpu_load DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS write_journal (
    journal_id BIGSERIAL PRIMARY KEY,
    object_id TEXT NOT NULL,
    version BIGINT,
    op_type TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_objects_state_updated_at
    ON objects (state, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_tiering_tasks_state_scheduled_priority
    ON tiering_tasks (task_state, scheduled_at, priority DESC);

CREATE INDEX IF NOT EXISTS idx_object_versions_tier_created_at
    ON object_versions (tier, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_node_heartbeats_status_last_seen
    ON node_heartbeats (status, last_seen_at DESC);
