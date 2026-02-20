package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"hybrid_distributed_store/internal/config"
)

// UpsertNormalizedMetadata writes object metadata into normalized tables:
// objects + object_versions.
func (s *Store) UpsertNormalizedMetadata(ctx context.Context, objectID string, metadata map[string]interface{}) error {
	if s == nil || s.db == nil {
		return nil
	}

	version := resolveVersion(metadata)
	state := resolveState(metadata)
	tier := resolveTier(metadata)
	sizeBytes := toInt64(metadata["original_length"], 0)
	checksum := toString(metadata["cold_hash"], "")
	encodingK := toNullableInt(metadata["k"])
	encodingM := toNullableInt(metadata["m"])

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin metadata tx failed: %w", err)
	}

	const upsertObjectSQL = `
INSERT INTO objects (object_id, tenant_id, current_version, state, created_at, updated_at)
VALUES ($1, 'default', $2, $3, NOW(), NOW())
ON CONFLICT (object_id)
DO UPDATE SET
	current_version = EXCLUDED.current_version,
	state = EXCLUDED.state,
	updated_at = NOW()
`
	if _, err := tx.ExecContext(ctx, upsertObjectSQL, objectID, version, state); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("upsert objects failed: %w", err)
	}

	const upsertVersionSQL = `
INSERT INTO object_versions (
	object_id, version, size_bytes, checksum_sha256, tier, encoding_k, encoding_m, created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
ON CONFLICT (object_id, version)
DO UPDATE SET
	size_bytes = EXCLUDED.size_bytes,
	checksum_sha256 = EXCLUDED.checksum_sha256,
	tier = EXCLUDED.tier,
	encoding_k = EXCLUDED.encoding_k,
	encoding_m = EXCLUDED.encoding_m
`
	if _, err := tx.ExecContext(ctx, upsertVersionSQL, objectID, version, sizeBytes, checksum, tier, encodingK, encodingM); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("upsert object_versions failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metadata tx failed: %w", err)
	}
	return nil
}

// GetNormalizedMetadata reads the latest object metadata from normalized tables
// and converts it to compatibility fields used by existing read/delete services.
// It returns (nil, nil) when the object is not found.
func (s *Store) GetNormalizedMetadata(ctx context.Context, objectID string) (map[string]interface{}, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	const q = `
SELECT o.current_version, o.state, ov.size_bytes, ov.checksum_sha256, ov.tier, ov.encoding_k, ov.encoding_m
FROM objects o
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.version = o.current_version
WHERE o.object_id = $1
`

	var (
		currentVersion int64
		state          string
		sizeBytes      int64
		checksum       string
		tier           string
		encodingK      sql.NullInt64
		encodingM      sql.NullInt64
	)
	if err := s.db.QueryRowContext(ctx, q, objectID).Scan(
		&currentVersion, &state, &sizeBytes, &checksum, &tier, &encodingK, &encodingM,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query normalized metadata failed: %w", err)
	}

	meta := map[string]interface{}{
		"key_name":     objectID,
		"strategy":     strategyFromTier(tier),
		"cold_hash":    checksum,
		"hot_key":      fmt.Sprintf("%s_hot", objectID),
		"cold_prefix":  fmt.Sprintf("%s_cold_chunk_", objectID),
		"chunk_prefix": fmt.Sprintf("%s_cold_chunk_", objectID),
	}

	switch state {
	case "EC_ACTIVE":
		meta["hot_version"] = int64(0)
		meta["cold_version"] = currentVersion
	default:
		meta["hot_version"] = currentVersion
		meta["cold_version"] = int64(0)
	}

	if sizeBytes > 0 {
		meta["original_length"] = sizeBytes
	}
	if encodingK.Valid {
		meta["k"] = int(encodingK.Int64)
	}
	if encodingM.Valid {
		meta["m"] = int(encodingM.Int64)
	}
	if _, ok := meta["k"]; !ok {
		meta["k"] = config.K
	}
	if _, ok := meta["m"]; !ok {
		meta["m"] = config.M
	}

	return meta, nil
}

// DeleteNormalizedMetadata removes metadata rows for one object.
func (s *Store) DeleteNormalizedMetadata(ctx context.Context, objectID string) error {
	if s == nil || s.db == nil {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete normalized metadata tx failed: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM object_versions WHERE object_id = $1`, objectID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete object_versions failed: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE object_id = $1`, objectID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete objects failed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete normalized metadata tx failed: %w", err)
	}
	return nil
}

func resolveVersion(metadata map[string]interface{}) int64 {
	hot := toInt64(metadata["hot_version"], 0)
	cold := toInt64(metadata["cold_version"], 0)
	if hot > cold {
		return hot
	}
	if cold > 0 {
		return cold
	}
	return time.Now().UnixNano()
}

func resolveState(metadata map[string]interface{}) string {
	switch toString(metadata["strategy"], "") {
	case "ec":
		return "EC_ACTIVE"
	default:
		return "HOT_ACTIVE"
	}
}

func resolveTier(metadata map[string]interface{}) string {
	switch toString(metadata["strategy"], "") {
	case "replication":
		return "HOT"
	case "ec":
		return "EC"
	default:
		return "HYBRID"
	}
}

func toString(v interface{}, fallback string) string {
	if v == nil {
		return fallback
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fallback
}

func toInt64(v interface{}, fallback int64) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	default:
		return fallback
	}
}

func toNullableInt(v interface{}) interface{} {
	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	case float64:
		return int(x)
	default:
		return nil
	}
}

func strategyFromTier(tier string) string {
	switch tier {
	case "HOT":
		return string(config.StrategyReplication)
	case "EC":
		return string(config.StrategyEC)
	default:
		return string(config.StrategyFieldHybrid)
	}
}
