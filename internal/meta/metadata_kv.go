package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// UpsertMetadataKV stores metadata payload by key for transitional dual-write path.
func (s *Store) UpsertMetadataKV(ctx context.Context, metaKey string, payload map[string]interface{}) error {
	if s == nil || s.db == nil {
		return nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal metadata payload failed: %w", err)
	}

	const q = `
INSERT INTO metadata_kv (meta_key, payload, updated_at)
VALUES ($1, $2::jsonb, NOW())
ON CONFLICT (meta_key)
DO UPDATE SET
	payload = EXCLUDED.payload,
	updated_at = NOW()
`
	if _, err := s.db.ExecContext(ctx, q, metaKey, string(data)); err != nil {
		return fmt.Errorf("upsert metadata_kv failed: %w", err)
	}
	return nil
}

// GetMetadataKV fetches metadata payload by key.
// It returns (nil, nil) if the key does not exist.
func (s *Store) GetMetadataKV(ctx context.Context, metaKey string) (map[string]interface{}, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	const q = `SELECT payload FROM metadata_kv WHERE meta_key = $1`

	var payloadRaw []byte
	if err := s.db.QueryRowContext(ctx, q, metaKey).Scan(&payloadRaw); err != nil {
		// Keep behavior simple: "no rows" means not found.
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query metadata_kv failed: %w", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal metadata_kv payload failed: %w", err)
	}
	return payload, nil
}

// DeleteMetadataKV removes metadata payload by key.
func (s *Store) DeleteMetadataKV(ctx context.Context, metaKey string) error {
	if s == nil || s.db == nil {
		return nil
	}

	const q = `DELETE FROM metadata_kv WHERE meta_key = $1`
	if _, err := s.db.ExecContext(ctx, q, metaKey); err != nil {
		return fmt.Errorf("delete metadata_kv failed: %w", err)
	}
	return nil
}
