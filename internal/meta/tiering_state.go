package meta

import (
	"context"
	"database/sql"
	"fmt"
)

// ObjectVersionSnapshot describes metadata state used by tiering processor decisions.
type ObjectVersionSnapshot struct {
	ObjectID       string
	CurrentVersion int64
	TaskVersion    int64
	State          string
	Tier           string
}

// ECShardLocation describes one shard placement record.
type ECShardLocation struct {
	ShardIndex int
	NodeID     string
	Path       string
	Status     string
}

// GetObjectVersionSnapshot returns current object state plus tier for task version.
func (s *Store) GetObjectVersionSnapshot(ctx context.Context, objectID string, taskVersion int64) (*ObjectVersionSnapshot, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	const q = `
SELECT o.object_id, o.current_version, o.state, ov.tier
FROM objects o
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.version = $2
WHERE o.object_id = $1
`

	var snap ObjectVersionSnapshot
	snap.TaskVersion = taskVersion
	if err := s.db.QueryRowContext(ctx, q, objectID, taskVersion).Scan(
		&snap.ObjectID,
		&snap.CurrentVersion,
		&snap.State,
		&snap.Tier,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query object version snapshot failed: %w", err)
	}
	return &snap, nil
}

// MarkObjectMigrating sets object state to MIGRATING for the expected version.
func (s *Store) MarkObjectMigrating(ctx context.Context, objectID string, version int64) error {
	if s == nil || s.db == nil {
		return nil
	}

	const q = `
UPDATE objects
SET state = 'MIGRATING', updated_at = NOW()
WHERE object_id = $1
  AND current_version = $2
  AND state IN ('HOT_ACTIVE', 'MIGRATION_PENDING', 'MIGRATING')
`
	res, err := s.db.ExecContext(ctx, q, objectID, version)
	if err != nil {
		return fmt.Errorf("mark object migrating failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark object migrating rows affected failed: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("object not eligible for migrating state transition")
	}
	return nil
}

// PromoteObjectVersionToEC commits EC placement and state transition transactionally.
func (s *Store) PromoteObjectVersionToEC(
	ctx context.Context,
	objectID string,
	version int64,
	checksum string,
	k int,
	m int,
	locations []ECShardLocation,
) error {
	if s == nil || s.db == nil {
		return nil
	}
	if len(locations) == 0 {
		return fmt.Errorf("no ec shard locations to commit")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin promote ec tx failed: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	const upsertShard = `
INSERT INTO ec_shard_locations (object_id, version, shard_index, node_id, path, status)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (object_id, version, shard_index)
DO UPDATE SET
	node_id = EXCLUDED.node_id,
	path = EXCLUDED.path,
	status = EXCLUDED.status
`
	for _, loc := range locations {
		status := loc.Status
		if status == "" {
			status = "ACTIVE"
		}
		if _, err := tx.ExecContext(
			ctx,
			upsertShard,
			objectID,
			version,
			loc.ShardIndex,
			loc.NodeID,
			loc.Path,
			status,
		); err != nil {
			return fmt.Errorf("upsert ec shard location failed: %w", err)
		}
	}

	const updateVersion = `
UPDATE object_versions
SET tier = 'EC',
	encoding_k = $3,
	encoding_m = $4,
	checksum_sha256 = CASE WHEN $5 <> '' THEN $5 ELSE checksum_sha256 END
WHERE object_id = $1
  AND version = $2
`
	if _, err := tx.ExecContext(ctx, updateVersion, objectID, version, k, m, checksum); err != nil {
		return fmt.Errorf("update object version tier failed: %w", err)
	}

	const updateObject = `
UPDATE objects
SET state = 'EC_ACTIVE', updated_at = NOW()
WHERE object_id = $1
  AND current_version = $2
`
	res, err := tx.ExecContext(ctx, updateObject, objectID, version)
	if err != nil {
		return fmt.Errorf("update object ec state failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update object ec state rows affected failed: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("object version is no longer current during ec promotion")
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit promote ec tx failed: %w", err)
	}
	return nil
}
