package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// TieringCandidate represents one HOT object selected by periodic policy scan.
type TieringCandidate struct {
	ObjectID string
	Version  int64
}

// EnqueueTieringCandidatesA1 periodically scans HOT objects by age and enqueues REPL_TO_EC tasks.
// It also moves object state from HOT_ACTIVE to MIGRATION_PENDING for selected candidates.
func (s *Store) EnqueueTieringCandidatesA1(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error) {
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

	candidates, err := loadTieringCandidates(ctx, tx, ageThresholdSec, maxObjects)
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

func loadTieringCandidates(ctx context.Context, tx *sql.Tx, ageThresholdSec, maxObjects int) ([]TieringCandidate, error) {
	const q = `
SELECT object_id, current_version
FROM objects
WHERE state = 'HOT_ACTIVE'
  AND updated_at <= NOW() - ($1 * INTERVAL '1 second')
ORDER BY updated_at ASC
LIMIT $2
`
	rows, err := tx.QueryContext(ctx, q, ageThresholdSec, maxObjects)
	if err != nil {
		return nil, fmt.Errorf("query policy candidates failed: %w", err)
	}
	defer rows.Close()

	out := make([]TieringCandidate, 0, maxObjects)
	for rows.Next() {
		var c TieringCandidate
		if err := rows.Scan(&c.ObjectID, &c.Version); err != nil {
			return nil, fmt.Errorf("scan policy candidate failed: %w", err)
		}
		out = append(out, c)
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
