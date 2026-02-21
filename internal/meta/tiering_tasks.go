package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// TieringTask is the metadata record consumed by tiering workers.
type TieringTask struct {
	TaskID      string
	ObjectID    string
	Version     int64
	TaskType    string
	TaskState   string
	Priority    int
	RetryCount  int
	LastError   sql.NullString
	ScheduledAt time.Time
	StartedAt   sql.NullTime
	FinishedAt  sql.NullTime
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

// ListTieringTasks returns recent tiering tasks with optional task_state filter.
func (s *Store) ListTieringTasks(ctx context.Context, taskState string, limit int) ([]TieringTask, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	query := `
SELECT task_id, object_id, version, task_type, task_state, priority, retry_count, last_error, scheduled_at, started_at, finished_at
FROM tiering_tasks
`
	args := make([]interface{}, 0, 2)
	if taskState != "" {
		query += "WHERE task_state = $1\n"
		args = append(args, taskState)
	}
	query += "ORDER BY scheduled_at DESC LIMIT $"
	query += fmt.Sprintf("%d", len(args)+1)
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list tiering tasks failed: %w", err)
	}
	defer rows.Close()

	tasks := make([]TieringTask, 0, limit)
	for rows.Next() {
		var t TieringTask
		if err := rows.Scan(
			&t.TaskID,
			&t.ObjectID,
			&t.Version,
			&t.TaskType,
			&t.TaskState,
			&t.Priority,
			&t.RetryCount,
			&t.LastError,
			&t.ScheduledAt,
			&t.StartedAt,
			&t.FinishedAt,
		); err != nil {
			return nil, fmt.Errorf("scan tiering task failed: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tiering tasks failed: %w", err)
	}
	return tasks, nil
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
