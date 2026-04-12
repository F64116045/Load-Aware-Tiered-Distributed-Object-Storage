package meta

import (
	"context"
	"fmt"
	"sort"
	"time"

	kvstore "hybrid_distributed_store/internal/meta/kvstore"
)

const taskTypeOldVersionGC = "GC_OLD_VERSION"

func (s *TiKVStore) EnqueueOldVersionGCCandidates(ctx context.Context, keepLatest int, minAgeSec int, maxTasks int) (int, error) {
	_ = ctx
	if s == nil || s.kv == nil {
		return 0, nil
	}
	if keepLatest <= 0 {
		keepLatest = 1
	}
	if maxTasks <= 0 {
		maxTasks = 200
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	objects, err := s.listObjects()
	if err != nil {
		return 0, err
	}
	now := time.Now()
	minAge := time.Duration(minAgeSec) * time.Second
	enqueued := 0

	for _, obj := range objects {
		if enqueued >= maxTasks {
			break
		}
		versions, err := s.listObjectVersionRecords(obj.ObjectID)
		if err != nil {
			return enqueued, err
		}
		if len(versions) <= keepLatest {
			continue
		}

		sort.Slice(versions, func(i, j int) bool {
			if versions[i].Version != versions[j].Version {
				return versions[i].Version > versions[j].Version
			}
			return versions[i].CreatedAt.After(versions[j].CreatedAt)
		})

		retained := 0
		for _, ver := range versions {
			if ver.Version == obj.CurrentVersion {
				retained++
				continue
			}
			if retained < keepLatest {
				retained++
				continue
			}
			if minAgeSec > 0 && now.Sub(ver.CreatedAt) < minAge {
				continue
			}
			taskID := fmt.Sprintf("gc-old:%s:%d", obj.ObjectID, ver.Version)
			changed, err := s.enqueueOrRequeueOldVersionGCTask(taskID, obj.ObjectID, ver.Version, now)
			if err != nil {
				return enqueued, err
			}
			if changed {
				enqueued++
				if enqueued >= maxTasks {
					break
				}
			}
		}
	}

	return enqueued, nil
}

func (s *TiKVStore) enqueueOrRequeueOldVersionGCTask(taskID, objectID string, version int64, now time.Time) (bool, error) {
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
			TaskType:    taskTypeOldVersionGC,
			TaskState:   "PENDING",
			Priority:    80,
			RetryCount:  0,
			ScheduledAt: now,
		}
		if err := s.putJSON(key, task); err != nil {
			return false, err
		}
		return true, nil
	}
	switch rec.TaskState {
	case "DONE", "FAILED":
		rec.TaskState = "PENDING"
		rec.Priority = 80
		rec.RetryCount = 0
		rec.LastError = nil
		rec.ScheduledAt = now
		rec.StartedAt = nil
		rec.FinishedAt = nil
		if err := s.putJSON(key, rec); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, nil
	}
}

func (s *TiKVStore) GetObjectVersionGCView(ctx context.Context, objectID string, version int64) (*ObjectVersionGCView, error) {
	_ = ctx
	if s == nil || s.kv == nil {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, found, err := s.getObjectRecord(tiKVObjectKey(objectID))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	ver, found, err := s.getObjectVersionRecord(tiKVObjectVersionKey(objectID, version))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	replicaRows, err := s.listReplicaRecords(objectID, version, "ACTIVE")
	if err != nil {
		return nil, err
	}
	replicas := make([]ReplicaLocation, 0, len(replicaRows))
	for _, row := range replicaRows {
		replicas = append(replicas, ReplicaLocation{
			NodeID: row.NodeID,
			Path:   row.Path,
			Status: row.Status,
		})
	}

	shardRows, err := s.listECShardRecords(objectID, version)
	if err != nil {
		return nil, err
	}
	shards := make([]ECShardLocation, 0, len(shardRows))
	for _, row := range shardRows {
		if row.Status != "ACTIVE" {
			continue
		}
		shards = append(shards, ECShardLocation{
			ShardIndex: row.ShardIndex,
			NodeID:     row.NodeID,
			Path:       row.Path,
			Status:     row.Status,
		})
	}

	return &ObjectVersionGCView{
		ObjectID:         objectID,
		Version:          version,
		CurrentVersion:   obj.CurrentVersion,
		Tier:             ver.Tier,
		CreatedAt:        ver.CreatedAt,
		ReplicaLocations: replicas,
		ECShardLocations: shards,
	}, nil
}

func (s *TiKVStore) PurgeObjectVersionMetadata(ctx context.Context, objectID string, version int64) error {
	_ = ctx
	if s == nil || s.kv == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	obj, found, err := s.getObjectRecord(tiKVObjectKey(objectID))
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if obj.CurrentVersion == version {
		return fmt.Errorf("refuse to purge current version object=%s version=%d", objectID, version)
	}

	b := s.kv.NewBatch()
	defer b.Close()

	if err := b.Delete([]byte(tiKVObjectVersionKey(objectID, version)), kvstore.NoSync); err != nil {
		return fmt.Errorf("delete object version metadata failed: %w", err)
	}
	if err := s.batchDeletePrefix(b, tiKVReplicaVersionPrefix(objectID, version)); err != nil {
		return err
	}
	if err := s.batchDeletePrefix(b, tiKVECShardVersionPrefix(objectID, version)); err != nil {
		return err
	}
	if err := s.removeTieringDueIndexByVersionInBatch(b, objectID, version); err != nil {
		return err
	}
	if err := b.Commit(kvstore.Sync); err != nil {
		return fmt.Errorf("commit old-version metadata purge failed: %w", err)
	}
	return nil
}
