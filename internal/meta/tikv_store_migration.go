package meta

import (
	"context"
	"fmt"
	"sort"
	"time"

	kvstore "hybrid_distributed_store/internal/meta/kvstore"
)

func (s *TiKVStore) GetObjectVersionSnapshot(ctx context.Context, objectID string, taskVersion int64) (*ObjectVersionSnapshot, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, found, err := s.getObjectRecord(tiKVObjectKey(objectID))
	if err != nil || !found {
		return nil, err
	}
	ver, found, err := s.getObjectVersionRecord(tiKVObjectVersionKey(objectID, taskVersion))
	if err != nil || !found {
		return nil, err
	}
	return &ObjectVersionSnapshot{
		ObjectID:       objectID,
		CurrentVersion: obj.CurrentVersion,
		TaskVersion:    taskVersion,
		State:          obj.State,
		Tier:           ver.Tier,
	}, nil
}

func (s *TiKVStore) MarkObjectMigrating(ctx context.Context, objectID string, version int64) error {
	if s == nil || s.kv == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, found, err := s.getObjectRecord(tiKVObjectKey(objectID))
	if err != nil || !found {
		return fmt.Errorf("object not eligible for migrating state transition")
	}
	if obj.CurrentVersion != version {
		return fmt.Errorf("object not eligible for migrating state transition")
	}
	switch obj.State {
	case "HOT_ACTIVE", "MIGRATION_PENDING", "MIGRATING":
	default:
		return fmt.Errorf("object not eligible for migrating state transition")
	}
	obj.State = "MIGRATING"
	obj.UpdatedAt = time.Now()
	return s.putJSON(tiKVObjectKey(objectID), obj)
}

func (s *TiKVStore) PromoteObjectVersionToEC(ctx context.Context, objectID string, version int64, checksum string, k int, m int, locations []ECShardLocation) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if len(locations) == 0 {
		return fmt.Errorf("no ec shard locations to commit")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	obj, found, err := s.getObjectRecord(tiKVObjectKey(objectID))
	if err != nil || !found {
		return fmt.Errorf("object version is no longer current during ec promotion")
	}
	if obj.CurrentVersion != version {
		return fmt.Errorf("object version is no longer current during ec promotion")
	}
	ver, found, err := s.getObjectVersionRecord(tiKVObjectVersionKey(objectID, version))
	if err != nil || !found {
		return fmt.Errorf("object version missing during ec promotion")
	}

	b := s.kv.NewBatch()
	defer b.Close()

	for _, loc := range locations {
		status := loc.Status
		if status == "" {
			status = "ACTIVE"
		}
		rec := tiKVECShardRecord{
			ObjectID:   objectID,
			Version:    version,
			ShardIndex: loc.ShardIndex,
			NodeID:     loc.NodeID,
			Path:       loc.Path,
			Status:     status,
		}
		if err := s.batchPutJSON(b, tiKVECShardKey(objectID, version, loc.ShardIndex), &rec); err != nil {
			return err
		}
	}

	ver.Tier = "EC"
	if checksum != "" {
		ver.ChecksumSHA256 = checksum
	}
	kk, mm := k, m
	ver.EncodingK = &kk
	ver.EncodingM = &mm
	if err := s.batchPutJSON(b, tiKVObjectVersionKey(objectID, version), ver); err != nil {
		return err
	}

	obj.State = "EC_ACTIVE"
	obj.UpdatedAt = time.Now()
	if err := s.batchPutJSON(b, tiKVObjectKey(objectID), obj); err != nil {
		return err
	}

	if err := b.Commit(kvstore.Sync); err != nil {
		return fmt.Errorf("commit promote ec batch failed: %w", err)
	}
	return nil
}

func (s *TiKVStore) ListActiveReplicaLocations(ctx context.Context, objectID string, version int64) ([]ReplicaLocation, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	recs, err := s.listReplicaRecords(objectID, version, "ACTIVE")
	if err != nil {
		return nil, err
	}
	out := make([]ReplicaLocation, 0, len(recs))
	for _, r := range recs {
		out = append(out, ReplicaLocation{
			NodeID: r.NodeID,
			Path:   r.Path,
			Status: r.Status,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

func (s *TiKVStore) UpsertReplicaLocations(ctx context.Context, objectID string, version int64, nodeIDs []string) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if len(nodeIDs) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, nodeID := range nodeIDs {
		if nodeID == "" {
			continue
		}
		rec := tiKVReplicaRecord{
			ObjectID: objectID,
			Version:  version,
			NodeID:   nodeID,
			Path:     objectID,
			Status:   "ACTIVE",
		}
		if err := s.putJSON(tiKVReplicaKey(objectID, version, nodeID), &rec); err != nil {
			return err
		}
	}
	return nil
}

func (s *TiKVStore) MarkReplicaLocationsDeleted(ctx context.Context, objectID string, version int64, nodeIDs []string) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if len(nodeIDs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, nodeID := range nodeIDs {
		key := tiKVReplicaKey(objectID, version, nodeID)
		rec := &tiKVReplicaRecord{}
		found, err := s.getJSON(key, rec)
		if err != nil || !found {
			continue
		}
		rec.Status = "DELETED"
		if err := s.putJSON(key, rec); err != nil {
			return err
		}
	}
	return nil
}
