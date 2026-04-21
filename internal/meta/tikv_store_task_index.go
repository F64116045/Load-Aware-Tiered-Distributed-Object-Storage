package meta

import (
	"fmt"
	"time"

	kvstore "hybrid_distributed_store/internal/meta/kvstore"
)

func isTaskRunnableState(state string) bool {
	switch state {
	case "PENDING", "RETRY_WAIT":
		return true
	default:
		return false
	}
}

func copyTaskRecord(rec *tiKVTaskRecord) *tiKVTaskRecord {
	if rec == nil {
		return nil
	}
	cp := *rec
	return &cp
}

func (s *TiKVStore) taskReadyIndexKey(rec *tiKVTaskRecord) string {
	if rec == nil {
		return ""
	}
	return tiKVTaskReadyKey(rec.TaskType, rec.Priority, rec.ScheduledAt, rec.TaskID)
}

func (s *TiKVStore) taskWaitIndexKey(rec *tiKVTaskRecord) string {
	if rec == nil {
		return ""
	}
	return tiKVTaskWaitKey(rec.TaskType, rec.ScheduledAt, rec.TaskID)
}

func (s *TiKVStore) deleteTaskRunnableIndexInBatch(b *kvstore.Batch, rec *tiKVTaskRecord) error {
	if b == nil || rec == nil || rec.TaskID == "" {
		return nil
	}
	readyKey := s.taskReadyIndexKey(rec)
	waitKey := s.taskWaitIndexKey(rec)
	if readyKey != "" {
		if err := b.Delete([]byte(readyKey), kvstore.NoSync); err != nil {
			return fmt.Errorf("delete task-ready index failed: %w", err)
		}
	}
	if waitKey != "" {
		if err := b.Delete([]byte(waitKey), kvstore.NoSync); err != nil {
			return fmt.Errorf("delete task-wait index failed: %w", err)
		}
	}
	return nil
}

func (s *TiKVStore) putTaskRunnableIndexInBatch(b *kvstore.Batch, rec *tiKVTaskRecord, now time.Time) error {
	if b == nil || rec == nil || rec.TaskID == "" {
		return nil
	}
	if !isTaskRunnableState(rec.TaskState) {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	indexKey := s.taskReadyIndexKey(rec)
	if rec.ScheduledAt.After(now) {
		indexKey = s.taskWaitIndexKey(rec)
	}
	if indexKey == "" {
		return nil
	}
	if err := b.Set([]byte(indexKey), []byte{1}, kvstore.NoSync); err != nil {
		return fmt.Errorf("put task runnable index failed: %w", err)
	}
	return nil
}

func (s *TiKVStore) rewriteTaskRunnableIndexLocked(rec *tiKVTaskRecord, now time.Time) error {
	if s == nil || s.kv == nil || rec == nil {
		return nil
	}
	b := s.kv.NewBatch()
	defer b.Close()

	if err := s.deleteTaskRunnableIndexInBatch(b, rec); err != nil {
		return err
	}
	if err := s.putTaskRunnableIndexInBatch(b, rec, now); err != nil {
		return err
	}
	if err := b.Commit(kvstore.Sync); err != nil {
		return fmt.Errorf("commit task runnable index rewrite failed: %w", err)
	}
	return nil
}

// writeTaskRecordWithRunnableIndexLocked persists task row and runnable index atomically.
// prev can be nil for first insert.
func (s *TiKVStore) writeTaskRecordWithRunnableIndexLocked(prev *tiKVTaskRecord, next *tiKVTaskRecord, now time.Time) error {
	if s == nil || s.kv == nil || next == nil || next.TaskID == "" {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	b := s.kv.NewBatch()
	defer b.Close()

	if err := s.deleteTaskRunnableIndexInBatch(b, prev); err != nil {
		return err
	}
	if err := s.batchPutJSON(b, tiKVTaskKey(next.TaskID), next); err != nil {
		return err
	}
	if err := s.putTaskRunnableIndexInBatch(b, next, now); err != nil {
		return err
	}
	if err := b.Commit(kvstore.Sync); err != nil {
		return fmt.Errorf("commit task row/index write failed: %w", err)
	}
	return nil
}

func (s *TiKVStore) promoteDueWaitingTasksLocked(now time.Time, maxPromote int) (int, error) {
	if s == nil || s.kv == nil {
		return 0, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	if maxPromote <= 0 {
		maxPromote = 256
	}

	it, err := s.newPrefixIter(tiKVTaskWaitPrefix())
	if err != nil {
		return 0, err
	}
	defer it.Close()

	b := s.kv.NewBatch()
	defer b.Close()

	nowUnixNano := now.UnixNano()
	touched := 0
	changed := false
	for ok := it.First(); ok && it.Valid(); ok = it.Next() {
		waitKey := string(it.Key())
		scheduledAtUnixNano, _, taskID, parsed := tiKVParseTaskWaitKey(waitKey)
		if !parsed {
			if err := b.Delete(it.Key(), kvstore.NoSync); err != nil {
				return touched, err
			}
			changed = true
			touched++
			if touched >= maxPromote {
				break
			}
			continue
		}
		if scheduledAtUnixNano > nowUnixNano {
			break
		}

		if err := b.Delete(it.Key(), kvstore.NoSync); err != nil {
			return touched, err
		}
		changed = true
		touched++

		rec, found, err := s.getTaskRecord(tiKVTaskKey(taskID))
		if err != nil {
			return touched, err
		}
		if !found {
			if touched >= maxPromote {
				break
			}
			continue
		}
		if err := s.deleteTaskRunnableIndexInBatch(b, rec); err != nil {
			return touched, err
		}
		if err := s.putTaskRunnableIndexInBatch(b, rec, now); err != nil {
			return touched, err
		}
		if touched >= maxPromote {
			break
		}
	}
	if err := it.Error(); err != nil {
		return touched, fmt.Errorf("iterate task wait index failed: %w", err)
	}
	if changed {
		if err := b.Commit(kvstore.Sync); err != nil {
			return touched, fmt.Errorf("commit wait->ready promotion failed: %w", err)
		}
	}
	return touched, nil
}

func (s *TiKVStore) claimNextTieringTaskFromReadyIndexLocked(now time.Time, taskType string) (*TieringTask, bool, error) {
	if s == nil || s.kv == nil {
		return nil, false, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	it, err := s.newPrefixIter(tiKVTaskReadyPrefix())
	if err != nil {
		return nil, false, err
	}
	defer it.Close()

	for ok := it.First(); ok && it.Valid(); ok = it.Next() {
		readyKeyRaw := append([]byte(nil), it.Key()...)
		readyKey := string(readyKeyRaw)
		_, indexedType, taskID, parsed := tiKVParseTaskReadyKey(readyKey)
		if !parsed {
			if err := s.kv.Delete(readyKeyRaw, kvstore.Sync); err != nil {
				return nil, false, err
			}
			continue
		}
		if taskType != "" && indexedType != taskType {
			continue
		}

		rec, found, err := s.getTaskRecord(tiKVTaskKey(taskID))
		if err != nil {
			return nil, false, err
		}
		if !found {
			if err := s.kv.Delete(readyKeyRaw, kvstore.Sync); err != nil {
				return nil, false, err
			}
			continue
		}

		// Index can lag for rare crashes/restarts. Self-heal and continue.
		if rec.TaskType != indexedType || !isTaskRunnableState(rec.TaskState) || rec.ScheduledAt.After(now) {
			if err := s.rewriteTaskRunnableIndexLocked(rec, now); err != nil {
				return nil, false, err
			}
			continue
		}

		prev := copyTaskRecord(rec)
		startedAt := now
		rec.TaskState = "RUNNING"
		rec.StartedAt = &startedAt
		rec.FinishedAt = nil
		rec.LastError = nil
		if err := s.writeTaskRecordWithRunnableIndexLocked(prev, rec, now); err != nil {
			return nil, false, err
		}
		task := toTieringTaskFromTiKV(*rec)
		return &task, true, nil
	}
	if err := it.Error(); err != nil {
		return nil, false, fmt.Errorf("iterate task ready index failed: %w", err)
	}
	return nil, false, nil
}
