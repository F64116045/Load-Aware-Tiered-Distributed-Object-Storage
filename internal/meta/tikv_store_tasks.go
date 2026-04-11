package meta

import (
	"context"
	"fmt"
	"sort"
	"time"
)

func (s *TiKVStore) EnqueueTieringTask(ctx context.Context, taskID, objectID string, version int64, taskType string, priority int, scheduledAt time.Time) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if taskID == "" {
		return fmt.Errorf("task id is empty")
	}
	if scheduledAt.IsZero() {
		scheduledAt = time.Now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := tiKVTaskKey(taskID)
	_, found, err := s.getTaskRecord(key)
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	rec := &tiKVTaskRecord{
		TaskID:      taskID,
		ObjectID:    objectID,
		Version:     version,
		TaskType:    taskType,
		TaskState:   "PENDING",
		Priority:    priority,
		RetryCount:  0,
		ScheduledAt: scheduledAt,
	}
	return s.putJSON(key, rec)
}

func (s *TiKVStore) ListTieringTasks(ctx context.Context, taskState, taskType string, limit int) ([]TieringTask, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	recs, err := s.listTaskRecords()
	if err != nil {
		return nil, err
	}
	filtered := make([]tiKVTaskRecord, 0, len(recs))
	for _, t := range recs {
		if taskState != "" && t.TaskState != taskState {
			continue
		}
		if taskType != "" && t.TaskType != taskType {
			continue
		}
		filtered = append(filtered, t)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].ScheduledAt.After(filtered[j].ScheduledAt)
	})
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}
	out := make([]TieringTask, 0, len(filtered))
	for _, r := range filtered {
		out = append(out, toTieringTaskFromTiKV(r))
	}
	return out, nil
}

func (s *TiKVStore) ListTieringTaskStateCounts(ctx context.Context, taskType string) (map[string]int64, error) {
	if s == nil || s.kv == nil {
		return map[string]int64{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	recs, err := s.listTaskRecords()
	if err != nil {
		return nil, err
	}
	out := map[string]int64{
		"PENDING":    0,
		"RUNNING":    0,
		"DONE":       0,
		"FAILED":     0,
		"RETRY_WAIT": 0,
	}
	for _, r := range recs {
		if taskType != "" && r.TaskType != taskType {
			continue
		}
		out[r.TaskState]++
	}
	return out, nil
}

func (s *TiKVStore) RequeueTieringTaskNow(ctx context.Context, taskID string) (bool, error) {
	if s == nil || s.kv == nil {
		return false, nil
	}
	if taskID == "" {
		return false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := tiKVTaskKey(taskID)
	rec, found, err := s.getTaskRecord(key)
	if err != nil || !found {
		return false, err
	}
	switch rec.TaskState {
	case "PENDING", "RUNNING", "RETRY_WAIT", "FAILED":
	default:
		return false, nil
	}
	rec.TaskState = "PENDING"
	rec.ScheduledAt = time.Now()
	rec.StartedAt = nil
	rec.FinishedAt = nil
	rec.LastError = nil
	if err := s.putJSON(key, rec); err != nil {
		return false, err
	}
	return true, nil
}

func (s *TiKVStore) CancelTieringTask(ctx context.Context, taskID, reason string) (bool, error) {
	if s == nil || s.kv == nil {
		return false, nil
	}
	if taskID == "" {
		return false, nil
	}
	if reason == "" {
		reason = "cancelled_by_admin"
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := tiKVTaskKey(taskID)
	rec, found, err := s.getTaskRecord(key)
	if err != nil || !found {
		return false, err
	}
	switch rec.TaskState {
	case "PENDING", "RUNNING", "RETRY_WAIT":
	default:
		return false, nil
	}
	now := time.Now()
	rec.TaskState = "FAILED"
	rec.LastError = &reason
	rec.FinishedAt = &now
	if err := s.putJSON(key, rec); err != nil {
		return false, err
	}
	return true, nil
}

func (s *TiKVStore) ClaimNextTieringTask(ctx context.Context, taskType string) (*TieringTask, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	recs, err := s.listTaskRecords()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	candidates := make([]tiKVTaskRecord, 0, len(recs))
	for _, r := range recs {
		if r.TaskState != "PENDING" && r.TaskState != "RETRY_WAIT" {
			continue
		}
		if r.ScheduledAt.After(now) {
			continue
		}
		if taskType != "" && r.TaskType != taskType {
			continue
		}
		candidates = append(candidates, r)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority > candidates[j].Priority
		}
		return candidates[i].ScheduledAt.Before(candidates[j].ScheduledAt)
	})
	selected := candidates[0]
	selected.TaskState = "RUNNING"
	start := time.Now()
	selected.StartedAt = &start
	selected.LastError = nil
	if err := s.putJSON(tiKVTaskKey(selected.TaskID), &selected); err != nil {
		return nil, err
	}
	t := toTieringTaskFromTiKV(selected)
	return &t, nil
}

func (s *TiKVStore) MarkTieringTaskDone(ctx context.Context, taskID string) error {
	if s == nil || s.kv == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tiKVTaskKey(taskID)
	rec, found, err := s.getTaskRecord(key)
	if err != nil || !found {
		return err
	}
	now := time.Now()
	rec.TaskState = "DONE"
	rec.FinishedAt = &now
	return s.putJSON(key, rec)
}

func (s *TiKVStore) MarkTieringTaskRetry(ctx context.Context, taskID, lastErr string, nextRunAt time.Time) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if nextRunAt.IsZero() {
		nextRunAt = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tiKVTaskKey(taskID)
	rec, found, err := s.getTaskRecord(key)
	if err != nil || !found {
		return err
	}
	rec.TaskState = "RETRY_WAIT"
	rec.RetryCount++
	rec.LastError = &lastErr
	rec.ScheduledAt = nextRunAt
	rec.FinishedAt = nil
	return s.putJSON(key, rec)
}

func (s *TiKVStore) MarkTieringTaskFailed(ctx context.Context, taskID, lastErr string) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if lastErr == "" {
		lastErr = "failed_without_error_message"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tiKVTaskKey(taskID)
	rec, found, err := s.getTaskRecord(key)
	if err != nil || !found {
		return err
	}
	now := time.Now()
	rec.TaskState = "FAILED"
	rec.LastError = &lastErr
	rec.FinishedAt = &now
	return s.putJSON(key, rec)
}
