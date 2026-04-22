package meta

import (
	"context"
	"fmt"
	"sort"
	"time"

	"hybrid_distributed_store/internal/config"
)

// EnqueueTieringCandidatesStrategyA implements the time-based baseline:
// candidates are selected only by age eligibility and capped by maxObjects.
func (s *TiKVStore) EnqueueTieringCandidatesStrategyA(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error) {
	return s.enqueueTieringCandidates(ctx, ageThresholdSec, maxObjects, 0, false)
}

// EnqueueTieringCandidatesStrategyB implements static throttling:
// age-eligible candidates are selected under per-round object/byte budgets.
func (s *TiKVStore) EnqueueTieringCandidatesStrategyB(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error) {
	return s.enqueueTieringCandidates(ctx, ageThresholdSec, maxObjects, maxBytes, true)
}

// EnqueueTieringCandidatesStrategyC shares the same selection/budget logic as B.
// Strategy-C admission gating (idle window) is enforced by the policy scanner.
func (s *TiKVStore) EnqueueTieringCandidatesStrategyC(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error) {
	return s.enqueueTieringCandidates(ctx, ageThresholdSec, maxObjects, maxBytes, true)
}

type tiKVTieringCandidate struct {
	ObjectID  string
	Version   int64
	DueKey    string
	SizeBytes int64
	UpdatedAt time.Time
}

func (s *TiKVStore) enqueueTieringCandidates(
	ctx context.Context,
	ageThresholdSec int,
	maxObjects int,
	maxBytes int64,
	applyByteBudget bool,
) (int, error) {
	if s == nil || s.kv == nil {
		return 0, nil
	}
	if ageThresholdSec < 0 {
		ageThresholdSec = 0
	}
	if maxObjects <= 0 {
		maxObjects = 200
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	eligibleBefore := now.Add(-time.Duration(ageThresholdSec) * time.Second)
	dueCandidates, err := s.listTieringDueCandidatesReady(now, config.TieringDueIndexMaxScan)
	if err != nil {
		return 0, err
	}
	candidates := make([]tiKVTieringCandidate, 0, len(dueCandidates))
	for _, due := range dueCandidates {
		rec := due.Record
		if rec.ObjectID == "" || rec.Version <= 0 {
			continue
		}

		obj, found, err := s.getObjectRecord(tiKVObjectKey(rec.ObjectID))
		if err != nil {
			return 0, err
		}
		if !found {
			_ = s.removeTieringDueIndexByVersion(rec.ObjectID, rec.Version)
			continue
		}
		if obj.CurrentVersion != rec.Version {
			_ = s.removeTieringDueIndexByVersion(rec.ObjectID, rec.Version)
			continue
		}
		if obj.State != "HOT_ACTIVE" && obj.State != "MIGRATION_PENDING" {
			_ = s.removeTieringDueIndexByVersion(rec.ObjectID, rec.Version)
			continue
		}
		if obj.UpdatedAt.After(eligibleBefore) {
			continue
		}
		ver, found, err := s.getObjectVersionRecord(tiKVObjectVersionKey(rec.ObjectID, rec.Version))
		if err != nil {
			return 0, err
		}
		if !found || ver.Tier != "HOT" {
			_ = s.removeTieringDueIndexByVersion(rec.ObjectID, rec.Version)
			continue
		}
		candidates = append(candidates, tiKVTieringCandidate{
			ObjectID:  rec.ObjectID,
			Version:   rec.Version,
			DueKey:    due.Key,
			SizeBytes: ver.SizeBytes,
			UpdatedAt: obj.UpdatedAt,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].UpdatedAt.Equal(candidates[j].UpdatedAt) {
			if candidates[i].SizeBytes != candidates[j].SizeBytes {
				return candidates[i].SizeBytes > candidates[j].SizeBytes
			}
			return candidates[i].ObjectID < candidates[j].ObjectID
		}
		return candidates[i].UpdatedAt.Before(candidates[j].UpdatedAt)
	})
	if !applyByteBudget && len(candidates) > maxObjects {
		candidates = candidates[:maxObjects]
	}

	var usedBytes int64
	enqueued := 0
	selected := 0
	for _, c := range candidates {
		if selected >= maxObjects {
			break
		}
		if applyByteBudget && maxBytes > 0 {
			if c.SizeBytes > 0 && usedBytes+c.SizeBytes > maxBytes {
				continue
			}
			if c.SizeBytes > 0 {
				usedBytes += c.SizeBytes
			}
		}
		selected++

		taskID := fmt.Sprintf("repl2ec:%s:%d", c.ObjectID, c.Version)
		taskKey := tiKVTaskKey(taskID)
		_, found, err := s.getTaskRecord(taskKey)
		if err != nil {
			return enqueued, err
		}
		if !found {
			task := &tiKVTaskRecord{
				TaskID:      taskID,
				ObjectID:    c.ObjectID,
				Version:     c.Version,
				TaskType:    "REPL_TO_EC",
				TaskState:   "PENDING",
				Priority:    100,
				RetryCount:  0,
				ScheduledAt: now,
			}
			if err := s.writeTaskRecordWithRunnableIndexLocked(nil, task, now); err != nil {
				return enqueued, err
			}
			enqueued++
		}
		if err := s.removeTieringDueIndexByVersion(c.ObjectID, c.Version); err != nil {
			return enqueued, err
		}

		obj, found, err := s.getObjectRecord(tiKVObjectKey(c.ObjectID))
		if err != nil {
			return enqueued, err
		}
		if !found {
			continue
		}
		if obj.CurrentVersion != c.Version || obj.State != "HOT_ACTIVE" {
			continue
		}
		obj.State = "MIGRATION_PENDING"
		obj.UpdatedAt = now
		if err := s.putJSON(tiKVObjectKey(c.ObjectID), obj); err != nil {
			return enqueued, err
		}
	}

	return enqueued, nil
}

func (s *TiKVStore) EnqueueRepairCandidates(ctx context.Context, maxObjects int) (int, error) {
	if s == nil || s.kv == nil {
		return 0, nil
	}
	if maxObjects <= 0 {
		maxObjects = 200
	}

	targetReplicaCount := config.HotReplicaCount
	if targetReplicaCount <= 0 {
		targetReplicaCount = 1
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	objects, err := s.listObjects()
	if err != nil {
		return 0, err
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].UpdatedAt.Before(objects[j].UpdatedAt)
	})

	selected := 0
	enqueued := 0
	for _, o := range objects {
		if selected >= maxObjects {
			break
		}
		ver, found, err := s.getObjectVersionRecord(tiKVObjectVersionKey(o.ObjectID, o.CurrentVersion))
		if err != nil {
			return enqueued, err
		}
		if !found {
			continue
		}

		taskID := ""
		switch ver.Tier {
		case "HOT":
			replicas, err := s.listReplicaRecords(o.ObjectID, o.CurrentVersion, "ACTIVE")
			if err != nil {
				return enqueued, err
			}
			if len(replicas) < targetReplicaCount {
				taskID = fmt.Sprintf("repair-repl:%s:%d", o.ObjectID, o.CurrentVersion)
			}

		case "EC":
			requiredK := config.K
			requiredM := config.M
			if ver.EncodingK != nil && *ver.EncodingK > 0 {
				requiredK = *ver.EncodingK
			}
			if ver.EncodingM != nil && *ver.EncodingM > 0 {
				requiredM = *ver.EncodingM
			}
			requiredTotal := requiredK + requiredM
			if requiredTotal <= 0 {
				continue
			}

			shards, err := s.listECShardRecords(o.ObjectID, o.CurrentVersion)
			if err != nil {
				return enqueued, err
			}
			activeShards := 0
			for _, sh := range shards {
				if sh.Status == "ACTIVE" {
					activeShards++
				}
			}
			if activeShards < requiredTotal {
				taskID = fmt.Sprintf("repair-ec:%s:%d", o.ObjectID, o.CurrentVersion)
			}
		}

		if taskID == "" {
			continue
		}
		selected++
		changed, err := s.enqueueRepairTask(taskID, o.ObjectID, o.CurrentVersion)
		if err != nil {
			return enqueued, err
		}
		if changed {
			enqueued++
		}
	}

	return enqueued, nil
}

func (s *TiKVStore) enqueueRepairTask(taskID, objectID string, version int64) (bool, error) {
	now := time.Now()
	key := tiKVTaskKey(taskID)
	rec, found, err := s.getTaskRecord(key)
	if err != nil {
		return false, err
	}
	if !found {
		task := &tiKVTaskRecord{
			TaskID:      taskID,
			ObjectID:    objectID,
			Version:     version,
			TaskType:    "REPAIR",
			TaskState:   "PENDING",
			Priority:    200,
			RetryCount:  0,
			ScheduledAt: now,
		}
		if err := s.writeTaskRecordWithRunnableIndexLocked(nil, task, now); err != nil {
			return false, err
		}
		return true, nil
	}

	switch rec.TaskState {
	case "DONE", "FAILED":
		prev := copyTaskRecord(rec)
		rec.TaskState = "PENDING"
		rec.Priority = 200
		rec.RetryCount = 0
		rec.LastError = nil
		rec.ScheduledAt = now
		rec.StartedAt = nil
		rec.FinishedAt = nil
		if err := s.writeTaskRecordWithRunnableIndexLocked(prev, rec, now); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, nil
	}
}
