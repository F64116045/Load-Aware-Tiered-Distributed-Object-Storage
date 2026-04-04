CREATE TABLE IF NOT EXISTS tiering_leader_state (
    lock_key BIGINT PRIMARY KEY,
    leader_id TEXT NOT NULL,
    scanner_status TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tiering_leader_state_last_heartbeat
    ON tiering_leader_state (last_heartbeat_at DESC);
