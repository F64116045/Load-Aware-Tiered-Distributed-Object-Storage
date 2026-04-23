package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"hybrid_distributed_store/internal/config"
	kvstore "hybrid_distributed_store/internal/meta/kvstore"
)

type tiKVTierDueCandidate struct {
	Key    string
	Record tiKVTierDueRecord
}

func (s *TiKVStore) upsertTieringDueIndex(
	b *kvstore.Batch,
	objectID string,
	version int64,
	tier string,
	sizeBytes int64,
	now time.Time,
) error {
	if b == nil {
		return fmt.Errorf("nil batch for due-index upsert")
	}
	if err := s.removeTieringDueIndexByVersionInBatch(b, objectID, version); err != nil {
		return err
	}
	if tier != "HOT" {
		return nil
	}

	eligibleAt := now.Add(time.Duration(config.AgeThresholdSec) * time.Second)
	dueKey := tiKVTierDueKey(eligibleAt, objectID, version)
	dueRecord := &tiKVTierDueRecord{
		ObjectID:   objectID,
		Version:    version,
		EligibleAt: eligibleAt,
		SizeBytes:  sizeBytes,
		CreatedAt:  now,
	}
	if err := s.batchPutJSON(b, dueKey, dueRecord); err != nil {
		return err
	}

	refKey := tiKVTierDueRefKey(objectID, version)
	refRecord := &tiKVTierDueRefRecord{
		ObjectID:   objectID,
		Version:    version,
		DueKey:     dueKey,
		EligibleAt: eligibleAt,
		UpdatedAt:  now,
	}
	if err := s.batchPutJSON(b, refKey, refRecord); err != nil {
		return err
	}
	return nil
}

func (s *TiKVStore) removeTieringDueIndexByVersionInBatch(b *kvstore.Batch, objectID string, version int64) error {
	if b == nil {
		return fmt.Errorf("nil batch for due-index delete")
	}
	refKey := tiKVTierDueRefKey(objectID, version)
	refRecord := &tiKVTierDueRefRecord{}
	found, err := s.getJSON(refKey, refRecord)
	if err != nil {
		return err
	}
	if found && refRecord.DueKey != "" {
		if err := b.Delete([]byte(refRecord.DueKey), kvstore.NoSync); err != nil {
			return fmt.Errorf("delete due-index key failed: %w", err)
		}
	}
	if err := b.Delete([]byte(refKey), kvstore.NoSync); err != nil {
		return fmt.Errorf("delete due-index ref failed: %w", err)
	}
	return nil
}

func (s *TiKVStore) removeTieringDueIndexByVersion(objectID string, version int64) error {
	b := s.kv.NewBatch()
	defer b.Close()
	if err := s.removeTieringDueIndexByVersionInBatch(b, objectID, version); err != nil {
		return err
	}
	return b.Commit(kvstore.Sync)
}

func (s *TiKVStore) removeTieringDueIndexForObjectInBatch(b *kvstore.Batch, objectID string) error {
	it, err := s.newPrefixIter(tiKVTierDueRefPrefix(objectID))
	if err != nil {
		return err
	}
	defer it.Close()

	for it.First(); it.Valid(); it.Next() {
		ref := &tiKVTierDueRefRecord{}
		if err := json.Unmarshal(it.Value(), ref); err != nil {
			return fmt.Errorf("decode due-index ref failed: %w", err)
		}
		if ref.DueKey != "" {
			if err := b.Delete([]byte(ref.DueKey), kvstore.NoSync); err != nil {
				return fmt.Errorf("delete due-index key failed: %w", err)
			}
		}
		if err := b.Delete(it.Key(), kvstore.NoSync); err != nil {
			return fmt.Errorf("delete due-index ref failed: %w", err)
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("iterate due-index refs failed: %w", err)
	}
	return nil
}

func (s *TiKVStore) listTieringDueCandidatesReady(now time.Time, maxScan int) ([]tiKVTierDueCandidate, error) {
	out, _, _, err := s.listTieringDueCandidatesReadyWindow(now, maxScan, "")
	return out, err
}

func (s *TiKVStore) listTieringDueCandidatesReadyWindow(now time.Time, maxScan int, startAfterKey string) ([]tiKVTierDueCandidate, bool, string, error) {
	if maxScan <= 0 {
		maxScan = config.TieringDueIndexMaxScan
	}
	if maxScan <= 0 {
		maxScan = 2000
	}

	it, err := s.newPrefixIter(tiKVTierDuePrefix())
	if err != nil {
		return nil, false, "", err
	}
	defer it.Close()

	out := make([]tiKVTierDueCandidate, 0, maxScan)
	hasMoreReady := false
	lastKey := startAfterKey
	for it.First(); it.Valid(); it.Next() {
		key := string(it.Key())
		if startAfterKey != "" && key <= startAfterKey {
			continue
		}
		eligibleNano, _, _, ok := tiKVParseTierDueKey(key)
		if ok && eligibleNano > now.UnixNano() {
			break
		}

		rec := tiKVTierDueRecord{}
		if err := json.Unmarshal(it.Value(), &rec); err != nil {
			return nil, false, "", fmt.Errorf("decode due-index record failed: %w", err)
		}
		if rec.EligibleAt.After(now) {
			break
		}
		if len(out) >= maxScan {
			hasMoreReady = true
			break
		}
		out = append(out, tiKVTierDueCandidate{
			Key:    key,
			Record: rec,
		})
		lastKey = key
	}
	if err := it.Error(); err != nil {
		return nil, false, "", fmt.Errorf("iterate due-index failed: %w", err)
	}
	return out, hasMoreReady, lastKey, nil
}

func (s *TiKVStore) GetTieringIndexStats(ctx context.Context) (*TieringIndexStats, error) {
	_ = ctx
	if s == nil || s.kv == nil {
		return &TieringIndexStats{}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	it, err := s.newPrefixIter(tiKVTierDuePrefix())
	if err != nil {
		return nil, err
	}
	defer it.Close()

	now := time.Now()
	stats := &TieringIndexStats{}
	for it.First(); it.Valid(); it.Next() {
		rec := tiKVTierDueRecord{}
		if err := json.Unmarshal(it.Value(), &rec); err != nil {
			return nil, fmt.Errorf("decode due-index record failed: %w", err)
		}
		stats.DueTotal++
		if !rec.EligibleAt.After(now) {
			stats.DueReady++
		}
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("iterate due-index stats failed: %w", err)
	}
	return stats, nil
}
