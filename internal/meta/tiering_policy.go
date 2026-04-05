package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"hybrid_distributed_store/internal/config"
)

// TieringCandidate represents one HOT object selected by periodic policy scan.
type TieringCandidate struct {
	ObjectID  string
	Version   int64
	SizeBytes int64
}

// EnqueueTieringCandidatesA1 periodically scans HOT objects by age and enqueues REPL_TO_EC tasks.
// It also moves object state from HOT_ACTIVE to MIGRATION_PENDING for selected candidates.
func (s *Store) EnqueueTieringCandidatesA1(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error) {
	return s.enqueueTieringCandidates(ctx, ageThresholdSec, maxObjects, 0, 0, false)
}

// EnqueueTieringCandidatesA2 scans HOT objects by age + size threshold and enqueues REPL_TO_EC tasks.
func (s *Store) EnqueueTieringCandidatesA2(ctx context.Context, ageThresholdSec int, sizeThresholdBytes int64, maxObjects int) (int, error) {
	if sizeThresholdBytes < 0 {
		sizeThresholdBytes = 0
	}
	return s.enqueueTieringCandidates(ctx, ageThresholdSec, maxObjects, sizeThresholdBytes, 0, false)
}

// EnqueueTieringCandidatesA3 scans HOT objects by age and enqueues REPL_TO_EC tasks under a per-round byte budget.
func (s *Store) EnqueueTieringCandidatesA3(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error) {
	return s.enqueueTieringCandidates(ctx, ageThresholdSec, maxObjects, 0, maxBytes, true)
}

func (s *Store) enqueueTieringCandidates(
	ctx context.Context,
	ageThresholdSec int,
	maxObjects int,
	minSizeBytes int64,
	maxBytes int64,
	applyByteBudget bool,
) (int, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	if ageThresholdSec < 0 {
		ageThresholdSec = 0
	}
	if maxObjects <= 0 {
		maxObjects = 200
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin policy scan tx failed: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	candidates, err := loadTieringCandidates(
		ctx,
		tx,
		ageThresholdSec,
		maxObjects,
		minSizeBytes,
		maxBytes,
		applyByteBudget,
	)
	if err != nil {
		return 0, err
	}
	if len(candidates) == 0 {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("commit empty policy scan tx failed: %w", err)
		}
		return 0, nil
	}

	enqueued := 0
	for _, c := range candidates {
		taskID := fmt.Sprintf("repl2ec:%s:%d", c.ObjectID, c.Version)
		inserted, err := enqueueTieringTaskTx(ctx, tx, taskID, c.ObjectID, c.Version)
		if err != nil {
			return enqueued, err
		}
		if inserted {
			enqueued++
		}
		if err := markObjectMigrationPendingTx(ctx, tx, c.ObjectID, c.Version); err != nil {
			return enqueued, err
		}
	}

	if err := tx.Commit(); err != nil {
		return enqueued, fmt.Errorf("commit policy scan tx failed: %w", err)
	}
	return enqueued, nil
}

func loadTieringCandidates(
	ctx context.Context,
	tx *sql.Tx,
	ageThresholdSec int,
	maxObjects int,
	minSizeBytes int64,
	maxBytes int64,
	applyByteBudget bool,
) ([]TieringCandidate, error) {
	if maxObjects <= 0 {
		return nil, nil
	}

	fetchLimit := maxObjects
	if applyByteBudget && maxBytes > 0 {
		// Pull a wider candidate set so budget skipping still has enough options.
		fetchLimit = maxObjects * 5
		if fetchLimit < 200 {
			fetchLimit = 200
		}
		if fetchLimit > 5000 {
			fetchLimit = 5000
		}
	}

	const q = `
SELECT o.object_id, o.current_version, ov.size_bytes
FROM objects o
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.version = o.current_version
WHERE o.state = 'HOT_ACTIVE'
  AND ov.tier = 'HOT'
  AND o.updated_at <= NOW() - ($1 * INTERVAL '1 second')
  AND ($2 <= 0 OR ov.size_bytes >= $2)
ORDER BY o.updated_at ASC, ov.size_bytes DESC, o.object_id ASC
LIMIT $3
`
	rows, err := tx.QueryContext(ctx, q, ageThresholdSec, minSizeBytes, fetchLimit)
	if err != nil {
		return nil, fmt.Errorf("query policy candidates failed: %w", err)
	}
	defer rows.Close()

	var usedBytes int64
	out := make([]TieringCandidate, 0, maxObjects)
	for rows.Next() {
		var c TieringCandidate
		if err := rows.Scan(&c.ObjectID, &c.Version, &c.SizeBytes); err != nil {
			return nil, fmt.Errorf("scan policy candidate failed: %w", err)
		}

		if applyByteBudget && maxBytes > 0 {
			if c.SizeBytes > 0 && usedBytes+c.SizeBytes > maxBytes {
				continue
			}
			if c.SizeBytes > 0 {
				usedBytes += c.SizeBytes
			}
		}

		out = append(out, c)
		if len(out) >= maxObjects {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate policy candidates failed: %w", err)
	}
	return out, nil
}

func enqueueTieringTaskTx(ctx context.Context, tx *sql.Tx, taskID, objectID string, version int64) (bool, error) {
	const q = `
INSERT INTO tiering_tasks (
	task_id, object_id, version, task_type, task_state, priority, scheduled_at
)
VALUES ($1, $2, $3, 'REPL_TO_EC', 'PENDING', 100, $4)
ON CONFLICT (task_id) DO NOTHING
`
	res, err := tx.ExecContext(ctx, q, taskID, objectID, version, time.Now())
	if err != nil {
		return false, fmt.Errorf("enqueue policy tiering task failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("enqueue policy tiering task rows affected failed: %w", err)
	}
	return affected > 0, nil
}

func markObjectMigrationPendingTx(ctx context.Context, tx *sql.Tx, objectID string, version int64) error {
	const q = `
UPDATE objects
SET state = 'MIGRATION_PENDING', updated_at = NOW()
WHERE object_id = $1
  AND current_version = $2
  AND state = 'HOT_ACTIVE'
`
	if _, err := tx.ExecContext(ctx, q, objectID, version); err != nil {
		return fmt.Errorf("mark object migration_pending failed: %w", err)
	}
	return nil
}

// EnqueueRepairCandidates scans current object versions and enqueues REPAIR tasks
// for under-replicated HOT objects and under-sharded EC objects.
func (s *Store) EnqueueRepairCandidates(ctx context.Context, maxObjects int) (int, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	if maxObjects <= 0 {
		maxObjects = 200
	}

	targetReplicaCount := config.HotReplicaCount
	if targetReplicaCount <= 0 {
		targetReplicaCount = 1
	}
	defaultK := config.K
	defaultM := config.M

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin repair scan tx failed: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	enqueued := 0
	remaining := maxObjects

	hotCandidates, err := loadRepairCandidatesHOT(ctx, tx, targetReplicaCount, remaining)
	if err != nil {
		return 0, err
	}
	for _, c := range hotCandidates {
		taskID := fmt.Sprintf("repair-repl:%s:%d", c.ObjectID, c.Version)
		inserted, err := enqueueRepairTaskTx(ctx, tx, taskID, c.ObjectID, c.Version)
		if err != nil {
			return enqueued, err
		}
		if inserted {
			enqueued++
		}
	}
	remaining -= len(hotCandidates)

	if remaining > 0 {
		ecCandidates, err := loadRepairCandidatesEC(ctx, tx, defaultK, defaultM, remaining)
		if err != nil {
			return enqueued, err
		}
		for _, c := range ecCandidates {
			taskID := fmt.Sprintf("repair-ec:%s:%d", c.ObjectID, c.Version)
			inserted, err := enqueueRepairTaskTx(ctx, tx, taskID, c.ObjectID, c.Version)
			if err != nil {
				return enqueued, err
			}
			if inserted {
				enqueued++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return enqueued, fmt.Errorf("commit repair scan tx failed: %w", err)
	}
	return enqueued, nil
}

func loadRepairCandidatesHOT(ctx context.Context, tx *sql.Tx, targetReplicaCount, maxObjects int) ([]TieringCandidate, error) {
	const q = `
SELECT o.object_id, o.current_version
FROM objects o
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.version = o.current_version
LEFT JOIN replica_locations rl
  ON rl.object_id = o.object_id
 AND rl.version = o.current_version
 AND rl.status = 'ACTIVE'
WHERE ov.tier = 'HOT'
GROUP BY o.object_id, o.current_version, o.updated_at
HAVING COUNT(rl.node_id) < $1
ORDER BY o.updated_at ASC
LIMIT $2
`
	rows, err := tx.QueryContext(ctx, q, targetReplicaCount, maxObjects)
	if err != nil {
		return nil, fmt.Errorf("query hot repair candidates failed: %w", err)
	}
	defer rows.Close()

	out := make([]TieringCandidate, 0, maxObjects)
	for rows.Next() {
		var c TieringCandidate
		if err := rows.Scan(&c.ObjectID, &c.Version); err != nil {
			return nil, fmt.Errorf("scan hot repair candidate failed: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hot repair candidates failed: %w", err)
	}
	return out, nil
}

func loadRepairCandidatesEC(ctx context.Context, tx *sql.Tx, defaultK, defaultM, maxObjects int) ([]TieringCandidate, error) {
	const q = `
SELECT o.object_id, o.current_version
FROM objects o
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.version = o.current_version
LEFT JOIN ec_shard_locations es
  ON es.object_id = o.object_id
 AND es.version = o.current_version
 AND es.status = 'ACTIVE'
WHERE ov.tier = 'EC'
GROUP BY o.object_id, o.current_version, o.updated_at, ov.encoding_k, ov.encoding_m
HAVING COUNT(es.shard_index) < (COALESCE(ov.encoding_k, $1) + COALESCE(ov.encoding_m, $2))
ORDER BY o.updated_at ASC
LIMIT $3
`
	rows, err := tx.QueryContext(ctx, q, defaultK, defaultM, maxObjects)
	if err != nil {
		return nil, fmt.Errorf("query ec repair candidates failed: %w", err)
	}
	defer rows.Close()

	out := make([]TieringCandidate, 0, maxObjects)
	for rows.Next() {
		var c TieringCandidate
		if err := rows.Scan(&c.ObjectID, &c.Version); err != nil {
			return nil, fmt.Errorf("scan ec repair candidate failed: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ec repair candidates failed: %w", err)
	}
	return out, nil
}

func enqueueRepairTaskTx(ctx context.Context, tx *sql.Tx, taskID, objectID string, version int64) (bool, error) {
	const q = `
INSERT INTO tiering_tasks (
	task_id, object_id, version, task_type, task_state, priority, retry_count, scheduled_at
)
VALUES ($1, $2, $3, 'REPAIR', 'PENDING', 200, 0, $4)
ON CONFLICT (task_id) DO UPDATE SET
	task_state = 'PENDING',
	priority = 200,
	retry_count = 0,
	last_error = NULL,
	scheduled_at = EXCLUDED.scheduled_at,
	started_at = NULL,
	finished_at = NULL
WHERE tiering_tasks.task_state IN ('DONE', 'FAILED')
`
	res, err := tx.ExecContext(ctx, q, taskID, objectID, version, time.Now())
	if err != nil {
		return false, fmt.Errorf("enqueue repair task failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("enqueue repair task rows affected failed: %w", err)
	}
	return affected > 0, nil
}
