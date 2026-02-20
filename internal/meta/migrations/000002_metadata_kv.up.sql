CREATE TABLE IF NOT EXISTS metadata_kv (
    meta_key TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metadata_kv_updated_at
    ON metadata_kv (updated_at DESC);
