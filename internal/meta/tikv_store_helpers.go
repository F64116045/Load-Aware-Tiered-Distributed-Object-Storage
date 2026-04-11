package meta

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	kvstore "hybrid_distributed_store/internal/meta/kvstore"
)

func (s *TiKVStore) listObjects() ([]tiKVObjectRecord, error) {
	it, err := s.newPrefixIter(tiKVPrefixObject)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]tiKVObjectRecord, 0)
	for it.First(); it.Valid(); it.Next() {
		var rec tiKVObjectRecord
		if err := json.Unmarshal(it.Value(), &rec); err != nil {
			return nil, fmt.Errorf("decode object record failed: %w", err)
		}
		out = append(out, rec)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("iterate objects failed: %w", err)
	}
	return out, nil
}

func (s *TiKVStore) listHeartbeats() ([]NodeHeartbeatSnapshot, error) {
	it, err := s.newPrefixIter(tiKVPrefixHB)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]NodeHeartbeatSnapshot, 0)
	for it.First(); it.Valid(); it.Next() {
		var rec NodeHeartbeatSnapshot
		if err := json.Unmarshal(it.Value(), &rec); err != nil {
			return nil, fmt.Errorf("decode heartbeat failed: %w", err)
		}
		out = append(out, rec)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("iterate heartbeats failed: %w", err)
	}
	return out, nil
}

func (s *TiKVStore) listTaskRecords() ([]tiKVTaskRecord, error) {
	it, err := s.newPrefixIter(tiKVPrefixTask)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]tiKVTaskRecord, 0)
	for it.First(); it.Valid(); it.Next() {
		var rec tiKVTaskRecord
		if err := json.Unmarshal(it.Value(), &rec); err != nil {
			return nil, fmt.Errorf("decode task record failed: %w", err)
		}
		out = append(out, rec)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("iterate tasks failed: %w", err)
	}
	return out, nil
}

func (s *TiKVStore) listReplicaRecords(objectID string, version int64, status string) ([]tiKVReplicaRecord, error) {
	prefix := tiKVReplicaVersionPrefix(objectID, version)
	it, err := s.newPrefixIter(prefix)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]tiKVReplicaRecord, 0)
	for it.First(); it.Valid(); it.Next() {
		var rec tiKVReplicaRecord
		if err := json.Unmarshal(it.Value(), &rec); err != nil {
			return nil, fmt.Errorf("decode replica record failed: %w", err)
		}
		if status != "" && rec.Status != status {
			continue
		}
		out = append(out, rec)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("iterate replica records failed: %w", err)
	}
	return out, nil
}

func (s *TiKVStore) listECShardRecords(objectID string, version int64) ([]tiKVECShardRecord, error) {
	prefix := tiKVECShardVersionPrefix(objectID, version)
	it, err := s.newPrefixIter(prefix)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]tiKVECShardRecord, 0)
	for it.First(); it.Valid(); it.Next() {
		var rec tiKVECShardRecord
		if err := json.Unmarshal(it.Value(), &rec); err != nil {
			return nil, fmt.Errorf("decode ec shard record failed: %w", err)
		}
		out = append(out, rec)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("iterate ec shard records failed: %w", err)
	}
	return out, nil
}

func (s *TiKVStore) getObjectRecord(key string) (*tiKVObjectRecord, bool, error) {
	var rec tiKVObjectRecord
	found, err := s.getJSON(key, &rec)
	if err != nil || !found {
		return nil, found, err
	}
	return &rec, true, nil
}

func (s *TiKVStore) getObjectVersionRecord(key string) (*tiKVObjectVersionRecord, bool, error) {
	var rec tiKVObjectVersionRecord
	found, err := s.getJSON(key, &rec)
	if err != nil || !found {
		return nil, found, err
	}
	return &rec, true, nil
}

func (s *TiKVStore) getTaskRecord(key string) (*tiKVTaskRecord, bool, error) {
	var rec tiKVTaskRecord
	found, err := s.getJSON(key, &rec)
	if err != nil || !found {
		return nil, found, err
	}
	return &rec, true, nil
}

func (s *TiKVStore) getLeaderRecord(key string) (*TieringLeaderState, bool, error) {
	var rec TieringLeaderState
	found, err := s.getJSON(key, &rec)
	if err != nil || !found {
		return nil, found, err
	}
	return &rec, true, nil
}

func (s *TiKVStore) putJSON(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value for key=%s failed: %w", key, err)
	}
	if err := s.kv.Set([]byte(key), data, kvstore.Sync); err != nil {
		return fmt.Errorf("set key=%s failed: %w", key, err)
	}
	return nil
}

func (s *TiKVStore) getJSON(key string, out interface{}) (bool, error) {
	v, closer, err := s.kv.Get([]byte(key))
	if err != nil {
		if errors.Is(err, kvstore.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("get key=%s failed: %w", key, err)
	}
	data := append([]byte(nil), v...)
	_ = closer.Close()
	if err := json.Unmarshal(data, out); err != nil {
		return false, fmt.Errorf("unmarshal key=%s failed: %w", key, err)
	}
	return true, nil
}

func (s *TiKVStore) batchPutJSON(b *kvstore.Batch, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal batch value for key=%s failed: %w", key, err)
	}
	if err := b.Set([]byte(key), data, kvstore.NoSync); err != nil {
		return fmt.Errorf("batch set key=%s failed: %w", key, err)
	}
	return nil
}

func (s *TiKVStore) batchDeletePrefix(b *kvstore.Batch, prefix string) error {
	it, err := s.newPrefixIter(prefix)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if err := b.Delete(it.Key(), kvstore.NoSync); err != nil {
			return fmt.Errorf("batch delete key=%s failed: %w", string(it.Key()), err)
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("iterate prefix=%s for delete failed: %w", prefix, err)
	}
	return nil
}

func (s *TiKVStore) newPrefixIter(prefix string) (*kvstore.Iterator, error) {
	upper := tiKVPrefixUpperBound([]byte(prefix))
	return s.kv.NewIter(&kvstore.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: upper,
	})
}

func tiKVPrefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func toTieringTaskFromTiKV(r tiKVTaskRecord) TieringTask {
	out := TieringTask{
		TaskID:      r.TaskID,
		ObjectID:    r.ObjectID,
		Version:     r.Version,
		TaskType:    r.TaskType,
		TaskState:   r.TaskState,
		Priority:    r.Priority,
		RetryCount:  r.RetryCount,
		ScheduledAt: r.ScheduledAt,
	}
	if r.LastError != nil {
		lastError := *r.LastError
		out.LastError = &lastError
	}
	if r.StartedAt != nil {
		startedAt := *r.StartedAt
		out.StartedAt = &startedAt
	}
	if r.FinishedAt != nil {
		finishedAt := *r.FinishedAt
		out.FinishedAt = &finishedAt
	}
	return out
}

func tiKVObjectKey(objectID string) string {
	return tiKVPrefixObject + objectID
}

func tiKVObjectVersionKey(objectID string, version int64) string {
	return tiKVPrefixObjVer + objectID + "/" + tiKVEncodeInt64(version)
}

func tiKVObjectVersionPrefix(objectID string) string {
	return tiKVPrefixObjVer + objectID + "/"
}

func tiKVReplicaKey(objectID string, version int64, nodeID string) string {
	return tiKVPrefixReplica + objectID + "/" + tiKVEncodeInt64(version) + "/" + nodeID
}

func tiKVReplicaPrefix(objectID string) string {
	return tiKVPrefixReplica + objectID + "/"
}

func tiKVReplicaVersionPrefix(objectID string, version int64) string {
	return tiKVPrefixReplica + objectID + "/" + tiKVEncodeInt64(version) + "/"
}

func tiKVECShardKey(objectID string, version int64, shardIndex int) string {
	return tiKVPrefixECShard + objectID + "/" + tiKVEncodeInt64(version) + "/" + tiKVEncodeInt(shardIndex)
}

func tiKVECShardPrefix(objectID string) string {
	return tiKVPrefixECShard + objectID + "/"
}

func tiKVECShardVersionPrefix(objectID string, version int64) string {
	return tiKVPrefixECShard + objectID + "/" + tiKVEncodeInt64(version) + "/"
}

func tiKVTaskKey(taskID string) string {
	return tiKVPrefixTask + taskID
}

func tiKVHeartbeatKey(nodeID string) string {
	return tiKVPrefixHB + nodeID
}

func tiKVLeaderKey(lockKey int64) string {
	return tiKVPrefixLeader + strconv.FormatInt(lockKey, 10)
}

func tiKVLeaderLockKey(lockKey int64) string {
	return tiKVPrefixLk + strconv.FormatInt(lockKey, 10)
}

func tiKVNewLockOwnerToken() []byte {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return []byte(fmt.Sprintf("owner-%d", time.Now().UnixNano()))
	}
	return []byte(hex.EncodeToString(b))
}

func tiKVEncodeInt64(v int64) string {
	return fmt.Sprintf("%020d", v)
}

func tiKVEncodeInt(v int) string {
	return fmt.Sprintf("%010d", v)
}
