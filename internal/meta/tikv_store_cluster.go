package meta

import (
	"context"
	"fmt"
	"sort"
	"time"
)

func (s *TiKVStore) UpsertTieringLeaderState(ctx context.Context, lockKey int64, leaderID, status string) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if leaderID == "" {
		return fmt.Errorf("leader id is empty")
	}
	if status == "" {
		status = "LEADING"
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := tiKVLeaderKey(lockKey)
	now := time.Now()
	rec, found, err := s.getLeaderRecord(key)
	if err != nil {
		return err
	}
	if !found {
		rec = &TieringLeaderState{
			LockKey:    lockKey,
			AcquiredAt: now,
		}
	}
	rec.LockKey = lockKey
	rec.LeaderID = leaderID
	rec.ScannerStatus = status
	if rec.AcquiredAt.IsZero() {
		rec.AcquiredAt = now
	}
	rec.LastHeartbeatAt = now
	return s.putJSON(key, rec)
}

func (s *TiKVStore) MarkTieringLeaderStopped(ctx context.Context, lockKey int64, leaderID, status string) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if leaderID == "" {
		return nil
	}
	if status == "" {
		status = "STOPPED"
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := tiKVLeaderKey(lockKey)
	rec, found, err := s.getLeaderRecord(key)
	if err != nil {
		return err
	}
	if !found || rec.LeaderID != leaderID {
		return nil
	}
	rec.ScannerStatus = status
	rec.LastHeartbeatAt = time.Now()
	return s.putJSON(key, rec)
}

func (s *TiKVStore) GetTieringLeaderState(ctx context.Context, lockKey int64) (*TieringLeaderState, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := tiKVLeaderKey(lockKey)
	rec, found, err := s.getLeaderRecord(key)
	if err != nil || !found {
		return nil, err
	}
	return rec, nil
}

func (s *TiKVStore) UpsertNodeHeartbeat(ctx context.Context, nodeID string, freeBytes int64, totalBytes int64, ioQueueDepth int, cpuLoad float64, status string) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if nodeID == "" {
		return fmt.Errorf("node id is empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	rec := NodeHeartbeatSnapshot{
		NodeID:       nodeID,
		LastSeenAt:   time.Now(),
		FreeBytes:    freeBytes,
		TotalBytes:   totalBytes,
		IOQueueDepth: ioQueueDepth,
		CPULoad:      cpuLoad,
		Status:       status,
	}
	return s.putJSON(tiKVHeartbeatKey(nodeID), &rec)
}

func (s *TiKVStore) ListHealthyNodeIDs(ctx context.Context, staleSec int) ([]string, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}
	if staleSec <= 0 {
		staleSec = 15
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	records, err := s.listHeartbeats()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	nodes := make([]string, 0, len(records))
	for _, n := range records {
		if n.Status != "UP" {
			continue
		}
		if now.Sub(n.LastSeenAt) > time.Duration(staleSec)*time.Second {
			continue
		}
		nodes = append(nodes, n.NodeID)
	}
	sort.Strings(nodes)
	return nodes, nil
}

func (s *TiKVStore) ListNodeHeartbeats(ctx context.Context, limit int) ([]NodeHeartbeatSnapshot, error) {
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

	records, err := s.listHeartbeats()
	if err != nil {
		return nil, err
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].LastSeenAt.After(records[j].LastSeenAt)
	})
	if len(records) > limit {
		records = records[:limit]
	}
	return records, nil
}
