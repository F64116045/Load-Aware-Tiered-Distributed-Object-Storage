package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// TieringTask is the metadata record consumed by tiering workers.
type TieringTask struct {
	TaskID     string
	ObjectID   string
	Version    int64
	TaskType   string
	TaskState  string
	Priority   int
	RetryCount int
	LastError  sql.NullString
}

// EnqueueTieringTask inserts a pending task if task_id is not present yet.
func (s *Store) EnqueueTieringTask(
	ctx context.Context,
	taskID, objectID string,
	version int64,
	taskType string,
	priority int,
	scheduledAt time.Time,
) error {
	if s == nil || s.db == nil {
		return nil
	}

	if scheduledAt.IsZero() {
		scheduledAt = time.Now()
	}

	const q = `
INSERT INTO tiering_tasks (
	task_id, object_id, version, task_type, task_state, priority, scheduled_at
)
VALUES ($1, $2, $3, $4, 'PENDING', $5, $6)
ON CONFLICT (task_id) DO NOTHING
`
	if _, err := s.db.ExecContext(ctx, q, taskID, objectID, version, taskType, priority, scheduledAt); err != nil {
		return fmt.Errorf("enqueue tiering task failed: %w", err)
	}
	return nil
}

// ClaimNextTieringTask claims one runnable task and transitions it to RUNNING.
// Returns (nil, nil) when no runnable task is found.
func (s *Store) ClaimNextTieringTask(ctx context.Context, taskType string) (*TieringTask, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin claim task tx failed: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	query := `
SELECT task_id, object_id, version, task_type, task_state, priority, retry_count, last_error
FROM tiering_tasks
WHERE task_state IN ('PENDING', 'RETRY_WAIT')
  AND scheduled_at <= NOW()
`
	args := []interface{}{}
	if taskType != "" {
		query += "  AND task_type = $1\n"
		args = append(args, taskType)
	}
	query += `
ORDER BY priority DESC, scheduled_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1
`

	task := &TieringTask{}
	if err := tx.QueryRowContext(ctx, query, args...).Scan(
		&task.TaskID,
		&task.ObjectID,
		&task.Version,
		&task.TaskType,
		&task.TaskState,
		&task.Priority,
		&task.RetryCount,
		&task.LastError,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query claim task failed: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE tiering_tasks
SET task_state='RUNNING', started_at=NOW(), last_error=NULL
WHERE task_id = $1
`, task.TaskID); err != nil {
		return nil, fmt.Errorf("update claimed task state failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim task tx failed: %w", err)
	}
	return task, nil
}

// MarkTieringTaskDone sets a running task to DONE.
func (s *Store) MarkTieringTaskDone(ctx context.Context, taskID string) error {
	if s == nil || s.db == nil {
		return nil
	}
	const q = `
UPDATE tiering_tasks
SET task_state='DONE', finished_at=NOW()
WHERE task_id=$1
`
	if _, err := s.db.ExecContext(ctx, q, taskID); err != nil {
		return fmt.Errorf("mark tiering task done failed: %w", err)
	}
	return nil
}

// MarkTieringTaskRetry moves a task to RETRY_WAIT and schedules next run.
func (s *Store) MarkTieringTaskRetry(ctx context.Context, taskID, lastErr string, nextRunAt time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	if nextRunAt.IsZero() {
		nextRunAt = time.Now()
	}

	const q = `
UPDATE tiering_tasks
SET task_state='RETRY_WAIT',
	retry_count=retry_count+1,
	last_error=$2,
	scheduled_at=$3,
	finished_at=NULL
WHERE task_id=$1
`
	if _, err := s.db.ExecContext(ctx, q, taskID, lastErr, nextRunAt); err != nil {
		return fmt.Errorf("mark tiering task retry failed: %w", err)
	}
	return nil
}
