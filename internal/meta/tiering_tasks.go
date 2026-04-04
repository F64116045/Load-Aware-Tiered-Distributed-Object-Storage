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

// ListTieringTasks returns recent tiering tasks with optional task_state/task_type filters.
func (s *Store) ListTieringTasks(ctx context.Context, taskState, taskType string, limit int) ([]TieringTask, error) {
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
	args := make([]interface{}, 0, 3)
	where := make([]string, 0, 2)
	if taskState != "" {
		where = append(where, fmt.Sprintf("task_state = $%d", len(args)+1))
		args = append(args, taskState)
	}
	if taskType != "" {
		where = append(where, fmt.Sprintf("task_type = $%d", len(args)+1))
		args = append(args, taskType)
	}
	if len(where) > 0 {
		query += "WHERE " + where[0]
		for i := 1; i < len(where); i++ {
			query += " AND " + where[i]
		}
		query += "\n"
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

// ListTieringTaskStateCounts aggregates task counts by state, optionally filtered by task_type.
func (s *Store) ListTieringTaskStateCounts(ctx context.Context, taskType string) (map[string]int64, error) {
	if s == nil || s.db == nil {
		return map[string]int64{}, nil
	}

	query := `
SELECT task_state, COUNT(*)
FROM tiering_tasks
`
	args := make([]interface{}, 0, 1)
	if taskType != "" {
		query += "WHERE task_type = $1\n"
		args = append(args, taskType)
	}
	query += "GROUP BY task_state"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list tiering task state counts failed: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{
		"PENDING":    0,
		"RUNNING":    0,
		"DONE":       0,
		"FAILED":     0,
		"RETRY_WAIT": 0,
	}
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("scan tiering state count failed: %w", err)
		}
		out[state] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tiering state counts failed: %w", err)
	}
	return out, nil
}

// RequeueTieringTaskNow forces a task back to immediate runnable state.
// It only applies to PENDING/RUNNING/RETRY_WAIT/FAILED tasks; DONE tasks are not requeued.
func (s *Store) RequeueTieringTaskNow(ctx context.Context, taskID string) (bool, error) {
	if s == nil || s.db == nil {
		return false, nil
	}
	if taskID == "" {
		return false, nil
	}

	const q = `
UPDATE tiering_tasks
SET task_state='PENDING',
	scheduled_at=NOW(),
	started_at=NULL,
	finished_at=NULL,
	last_error=NULL
WHERE task_id=$1
  AND task_state IN ('PENDING', 'RUNNING', 'RETRY_WAIT', 'FAILED')
`
	res, err := s.db.ExecContext(ctx, q, taskID)
	if err != nil {
		return false, fmt.Errorf("requeue tiering task failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("requeue tiering task rows affected failed: %w", err)
	}
	return affected > 0, nil
}

// CancelTieringTask marks a task as FAILED with explicit reason.
// It applies to PENDING/RUNNING/RETRY_WAIT tasks and intentionally excludes DONE.
func (s *Store) CancelTieringTask(ctx context.Context, taskID, reason string) (bool, error) {
	if s == nil || s.db == nil {
		return false, nil
	}
	if taskID == "" {
		return false, nil
	}
	if reason == "" {
		reason = "cancelled_by_admin"
	}

	const q = `
UPDATE tiering_tasks
SET task_state='FAILED',
	last_error=$2,
	finished_at=NOW()
WHERE task_id=$1
  AND task_state IN ('PENDING', 'RUNNING', 'RETRY_WAIT')
`
	res, err := s.db.ExecContext(ctx, q, taskID, reason)
	if err != nil {
		return false, fmt.Errorf("cancel tiering task failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cancel tiering task rows affected failed: %w", err)
	}
	return affected > 0, nil
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

// MarkTieringTaskFailed sets task terminal state to FAILED with explicit error detail.
func (s *Store) MarkTieringTaskFailed(ctx context.Context, taskID, lastErr string) error {
	if s == nil || s.db == nil {
		return nil
	}
	if taskID == "" {
		return nil
	}
	if lastErr == "" {
		lastErr = "failed_without_error_message"
	}

	const q = `
UPDATE tiering_tasks
SET task_state='FAILED',
	last_error=$2,
	finished_at=NOW()
WHERE task_id=$1
`
	if _, err := s.db.ExecContext(ctx, q, taskID, lastErr); err != nil {
		return fmt.Errorf("mark tiering task failed failed: %w", err)
	}
	return nil
}
