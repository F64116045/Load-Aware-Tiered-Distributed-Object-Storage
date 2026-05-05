package meta

import (
	"context"
	"fmt"
	"sort"
	"time"

	"hybrid_distributed_store/internal/config"
	kvstore "hybrid_distributed_store/internal/meta/kvstore"
)

func resolveTaskWaitPromotePlan() (int, int, int) {
	base := config.TieringTaskWaitPromoteBase
	if base <= 0 {
		base = 256
	}
	burstRounds := config.TieringTaskWaitPromoteBurstRounds
	if burstRounds <= 0 {
		burstRounds = 1
	}
	if burstRounds > 16 {
		burstRounds = 16
	}
	adaptiveMax := config.TieringTaskWaitPromoteAdaptiveMax
	if adaptiveMax < base {
		adaptiveMax = base
	}
	if adaptiveMax <= 0 {
		adaptiveMax = base
	}
	return base, burstRounds, adaptiveMax
}

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
	return s.writeTaskRecordWithRunnableIndexLocked(nil, rec, time.Now())
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
	now := time.Now()
	prev := copyTaskRecord(rec)
	rec.TaskState = "PENDING"
	rec.ScheduledAt = now
	rec.StartedAt = nil
	rec.FinishedAt = nil
	rec.LastError = nil
	if err := s.writeTaskRecordWithRunnableIndexLocked(prev, rec, now); err != nil {
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
	prev := copyTaskRecord(rec)
	rec.TaskState = "FAILED"
	rec.LastError = &reason
	rec.FinishedAt = &now
	if err := s.writeTaskRecordWithRunnableIndexLocked(prev, rec, now); err != nil {
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

	now := time.Now()
	promoteLimit, promoteBurstRounds, promoteAdaptiveMax := resolveTaskWaitPromotePlan()
	for round := 0; round < promoteBurstRounds; round++ {
		promoted, err := s.promoteDueWaitingTasksLocked(now, promoteLimit)
		if err != nil {
			return nil, err
		}
		if promoted < promoteLimit {
			break
		}
		if promoteLimit < promoteAdaptiveMax {
			next := promoteLimit * 2
			if next > promoteAdaptiveMax {
				next = promoteAdaptiveMax
			}
			if next > promoteLimit {
				promoteLimit = next
			}
		}
	}
	if task, claimed, err := s.claimNextTieringTaskFromReadyIndexLocked(ctx, now, taskType); err != nil {
		return nil, err
	} else if claimed {
		return task, nil
	}
	return nil, nil
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
	prev := copyTaskRecord(rec)
	now := time.Now()
	rec.TaskState = "DONE"
	rec.FinishedAt = &now
	return s.writeTaskRecordWithRunnableIndexLocked(prev, rec, now)
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
	prev := copyTaskRecord(rec)
	now := time.Now()
	rec.TaskState = "RETRY_WAIT"
	rec.RetryCount++
	rec.LastError = &lastErr
	rec.ScheduledAt = nextRunAt
	rec.FinishedAt = nil
	return s.writeTaskRecordWithRunnableIndexLocked(prev, rec, now)
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
	prev := copyTaskRecord(rec)
	now := time.Now()
	rec.TaskState = "FAILED"
	rec.LastError = &lastErr
	rec.FinishedAt = &now
	return s.writeTaskRecordWithRunnableIndexLocked(prev, rec, now)
}

func (s *TiKVStore) PurgeTerminalTieringTasks(ctx context.Context, olderThan time.Time, limit int) (int, error) {
	if s == nil || s.kv == nil {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if olderThan.IsZero() {
		return 0, nil
	}
	if limit <= 0 {
		limit = 200
	}
	if limit > 5000 {
		limit = 5000
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	it, err := s.newPrefixIter(tiKVTaskTerminalPrefix())
	if err != nil {
		return 0, err
	}
	defer it.Close()

	b := s.kv.NewBatch()
	defer b.Close()

	cutoff := olderThan.UnixNano()
	now := time.Now()
	purged := 0
	changed := false

	for ok := it.First(); ok && it.Valid(); ok = it.Next() {
		select {
		case <-ctx.Done():
			return purged, ctx.Err()
		default:
		}

		keyRaw := append([]byte(nil), it.Key()...)
		key := string(keyRaw)
		finishedAtUnixNano, taskID, parsed := tiKVParseTaskTerminalKey(key)
		if !parsed {
			if err := b.Delete(keyRaw, kvstore.NoSync); err != nil {
				return purged, err
			}
			changed = true
			continue
		}
		if finishedAtUnixNano > cutoff {
			break
		}

		taskKey := tiKVTaskKey(taskID)
		rec, found, err := s.getTaskRecord(taskKey)
		if err != nil {
			return purged, err
		}
		if !found {
			if err := b.Delete(keyRaw, kvstore.NoSync); err != nil {
				return purged, err
			}
			changed = true
			continue
		}

		if !isTaskTerminalState(rec.TaskState) || rec.FinishedAt == nil {
			// Stale terminal entry: rebuild index set from row state.
			if err := b.Delete(keyRaw, kvstore.NoSync); err != nil {
				return purged, err
			}
			if err := s.deleteTaskRunnableIndexInBatch(b, rec); err != nil {
				return purged, err
			}
			if err := s.deleteTaskTerminalIndexInBatch(b, rec); err != nil {
				return purged, err
			}
			if err := s.putTaskRunnableIndexInBatch(b, rec, now); err != nil {
				return purged, err
			}
			if err := s.putTaskTerminalIndexInBatch(b, rec); err != nil {
				return purged, err
			}
			changed = true
			continue
		}

		if rec.FinishedAt.UnixNano() > cutoff {
			// Old terminal key can remain after crashes; repair it in-place.
			if err := b.Delete(keyRaw, kvstore.NoSync); err != nil {
				return purged, err
			}
			if err := s.putTaskTerminalIndexInBatch(b, rec); err != nil {
				return purged, err
			}
			changed = true
			continue
		}

		if err := b.Delete(keyRaw, kvstore.NoSync); err != nil {
			return purged, err
		}
		if err := s.deleteTaskRunnableIndexInBatch(b, rec); err != nil {
			return purged, err
		}
		if err := s.deleteTaskTerminalIndexInBatch(b, rec); err != nil {
			return purged, err
		}
		if err := b.Delete([]byte(taskKey), kvstore.NoSync); err != nil {
			return purged, err
		}
		changed = true
		purged++
		if purged >= limit {
			break
		}
	}
	if err := it.Error(); err != nil {
		return purged, fmt.Errorf("iterate task terminal index failed: %w", err)
	}
	if changed {
		if err := b.Commit(kvstore.Sync); err != nil {
			return purged, fmt.Errorf("commit task history purge failed: %w", err)
		}
	}
	return purged, nil
}
