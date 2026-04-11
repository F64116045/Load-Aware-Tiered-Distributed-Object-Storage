package meta

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"hybrid_distributed_store/internal/config"
	kvstore "hybrid_distributed_store/internal/meta/kvstore"
)

const (
	tiKVPrefixObject  = "obj/"
	tiKVPrefixObjVer  = "objv/"
	tiKVPrefixReplica = "repl/"
	tiKVPrefixECShard = "ec/"
	tiKVPrefixTask    = "task/"
	tiKVPrefixHB      = "hb/"
	tiKVPrefixLeader  = "leader/"
	tiKVPrefixLk      = "leader_lock/"
)

type tiKVObjectRecord struct {
	ObjectID       string    `json:"object_id"`
	TenantID       string    `json:"tenant_id"`
	CurrentVersion int64     `json:"current_version"`
	State          string    `json:"state"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type tiKVObjectVersionRecord struct {
	ObjectID       string    `json:"object_id"`
	Version        int64     `json:"version"`
	SizeBytes      int64     `json:"size_bytes"`
	ChecksumSHA256 string    `json:"checksum_sha256"`
	Tier           string    `json:"tier"`
	ContentType    *string   `json:"content_type,omitempty"`
	EncodingK      *int      `json:"encoding_k,omitempty"`
	EncodingM      *int      `json:"encoding_m,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type tiKVReplicaRecord struct {
	ObjectID string `json:"object_id"`
	Version  int64  `json:"version"`
	NodeID   string `json:"node_id"`
	Path     string `json:"path"`
	Status   string `json:"status"`
}

type tiKVECShardRecord struct {
	ObjectID   string `json:"object_id"`
	Version    int64  `json:"version"`
	ShardIndex int    `json:"shard_index"`
	NodeID     string `json:"node_id"`
	Path       string `json:"path"`
	Status     string `json:"status"`
}

type tiKVTaskRecord struct {
	TaskID      string     `json:"task_id"`
	ObjectID    string     `json:"object_id"`
	Version     int64      `json:"version"`
	TaskType    string     `json:"task_type"`
	TaskState   string     `json:"task_state"`
	Priority    int        `json:"priority"`
	RetryCount  int        `json:"retry_count"`
	LastError   *string    `json:"last_error,omitempty"`
	ScheduledAt time.Time  `json:"scheduled_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

type tiKVLeaderLock struct {
	store    *TiKVStore
	lockKey  []byte
	owner    []byte
	ttl      time.Duration
	released bool
}

func (l *tiKVLeaderLock) Ping(ctx context.Context) error {
	if l == nil || l.store == nil || l.released {
		return fmt.Errorf("tikv leader lock is nil")
	}
	ok, err := l.store.kv.RefreshLock(ctx, l.lockKey, l.owner, l.ttl)
	if err != nil {
		return fmt.Errorf("tikv leader lock refresh failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("tikv leader lock was lost")
	}
	return nil
}

func (l *tiKVLeaderLock) Release(ctx context.Context) error {
	if l == nil || l.store == nil || l.released {
		return nil
	}
	if err := l.store.kv.ReleaseLock(ctx, l.lockKey, l.owner); err != nil {
		return fmt.Errorf("tikv leader lock release failed: %w", err)
	}
	l.released = true
	return nil
}

// TiKVStore is a TiKV-backed metadata repository.
type TiKVStore struct {
	kv      *kvstore.Client
	mu      sync.RWMutex
	lockTTL time.Duration
}

func NewTiKVStore(cfg Config) (*TiKVStore, error) {
	if !cfg.Enabled {
		return &TiKVStore{}, nil
	}

	dsn, err := resolveTiKVEndpoints(cfg.DSN)
	if err != nil {
		return nil, err
	}

	kvClient, err := kvstore.Open(dsn, &kvstore.Options{})
	if err != nil {
		return nil, fmt.Errorf("open tikv store failed: %w", err)
	}

	return &TiKVStore{
		kv:      kvClient,
		lockTTL: 10 * time.Second,
	}, nil
}

func resolveTiKVEndpoints(dsn string) (string, error) {
	raw := strings.TrimSpace(dsn)
	if raw == "" {
		return "", fmt.Errorf("meta dsn is required for tikv backend")
	}
	lower := strings.ToLower(raw)
	if lower == "memory" || lower == "mem" || strings.HasPrefix(lower, "memory://") || strings.HasPrefix(lower, "mem://") {
		return raw, nil
	}
	if strings.HasPrefix(strings.ToLower(raw), "tikv://") {
		raw = strings.TrimPrefix(raw, "tikv://")
	}
	if raw == "" {
		return "", fmt.Errorf("invalid tikv dsn: %q", dsn)
	}
	return raw, nil
}

func (s *TiKVStore) Ping(ctx context.Context) error {
	if s == nil || s.kv == nil {
		return nil
	}
	return s.kv.Ping(ctx)
}

func (s *TiKVStore) Close() error {
	if s == nil || s.kv == nil {
		return nil
	}
	return s.kv.Close()
}

func (s *TiKVStore) TryAcquireLeaderLock(ctx context.Context, key int64) (LeaderLock, bool, error) {
	if s == nil || s.kv == nil {
		return nil, false, nil
	}
	lockKey := []byte(tiKVLeaderLockKey(key))
	owner := tiKVNewLockOwnerToken()
	acquired, err := s.kv.TryAcquireLockWithTTL(ctx, lockKey, owner, s.lockTTL)
	if err != nil {
		return nil, false, err
	}
	if !acquired {
		return nil, false, nil
	}
	return &tiKVLeaderLock{
		store:   s,
		lockKey: lockKey,
		owner:   owner,
		ttl:     s.lockTTL,
	}, true, nil
}

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

func (s *TiKVStore) UpsertNormalizedMetadata(ctx context.Context, objectID string, metadata map[string]interface{}) error {
	if s == nil || s.kv == nil {
		return nil
	}
	if objectID == "" {
		return fmt.Errorf("object id is empty")
	}

	version := resolveVersion(metadata)
	state := resolveState(metadata)
	tier := resolveTier(metadata)
	sizeBytes := toInt64(metadata["original_length"], 0)
	checksum := toString(metadata["cold_hash"], "")
	contentType := toNullableString(metadata["content_type"])
	encodingK := toNullableInt(metadata["k"])
	encodingM := toNullableInt(metadata["m"])

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	objKey := tiKVObjectKey(objectID)
	obj, found, err := s.getObjectRecord(objKey)
	if err != nil {
		return err
	}
	if !found {
		obj = &tiKVObjectRecord{
			ObjectID:  objectID,
			TenantID:  "default",
			CreatedAt: now,
		}
	}
	obj.CurrentVersion = version
	obj.State = state
	if obj.TenantID == "" {
		obj.TenantID = "default"
	}
	obj.UpdatedAt = now

	verRec := &tiKVObjectVersionRecord{
		ObjectID:       objectID,
		Version:        version,
		SizeBytes:      sizeBytes,
		ChecksumSHA256: checksum,
		Tier:           tier,
		CreatedAt:      now,
	}
	if v, ok := contentType.(string); ok {
		verRec.ContentType = &v
	}
	if v, ok := encodingK.(int); ok {
		vv := v
		verRec.EncodingK = &vv
	}
	if v, ok := encodingM.(int); ok {
		vv := v
		verRec.EncodingM = &vv
	}

	b := s.kv.NewBatch()
	defer b.Close()

	if err := s.batchPutJSON(b, objKey, obj); err != nil {
		return err
	}
	if err := s.batchPutJSON(b, tiKVObjectVersionKey(objectID, version), verRec); err != nil {
		return err
	}

	if tier == "HOT" {
		replicaNodes := toStringSlice(metadata["replica_nodes"])
		for _, nodeID := range replicaNodes {
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
			if err := s.batchPutJSON(b, tiKVReplicaKey(objectID, version, nodeID), &rec); err != nil {
				return err
			}
		}
	}

	if err := b.Commit(kvstore.Sync); err != nil {
		return fmt.Errorf("commit normalized metadata batch failed: %w", err)
	}
	return nil
}

func (s *TiKVStore) GetNormalizedMetadata(ctx context.Context, objectID string) (map[string]interface{}, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, found, err := s.getObjectRecord(tiKVObjectKey(objectID))
	if err != nil || !found {
		return nil, err
	}
	ver, found, err := s.getObjectVersionRecord(tiKVObjectVersionKey(objectID, obj.CurrentVersion))
	if err != nil || !found {
		return nil, err
	}

	meta := map[string]interface{}{
		"key_name":     objectID,
		"strategy":     strategyFromTier(ver.Tier),
		"cold_hash":    ver.ChecksumSHA256,
		"hot_key":      fmt.Sprintf("%s_hot", objectID),
		"cold_prefix":  fmt.Sprintf("%s_cold_chunk_", objectID),
		"chunk_prefix": fmt.Sprintf("%s_cold_chunk_", objectID),
	}

	switch obj.State {
	case "EC_ACTIVE":
		meta["hot_version"] = int64(0)
		meta["cold_version"] = obj.CurrentVersion
	default:
		meta["hot_version"] = obj.CurrentVersion
		meta["cold_version"] = int64(0)
	}

	if ver.SizeBytes > 0 {
		meta["original_length"] = ver.SizeBytes
	}
	if ver.ContentType != nil && *ver.ContentType != "" {
		meta["content_type"] = *ver.ContentType
	}
	if ver.EncodingK != nil {
		meta["k"] = *ver.EncodingK
	}
	if ver.EncodingM != nil {
		meta["m"] = *ver.EncodingM
	}
	if _, ok := meta["k"]; !ok {
		meta["k"] = 4
	}
	if _, ok := meta["m"]; !ok {
		meta["m"] = 2
	}
	return meta, nil
}

func (s *TiKVStore) DeleteNormalizedMetadata(ctx context.Context, objectID string) error {
	if s == nil || s.kv == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.kv.NewBatch()
	defer b.Close()

	_ = b.Delete([]byte(tiKVObjectKey(objectID)), kvstore.NoSync)
	if err := s.batchDeletePrefix(b, tiKVObjectVersionPrefix(objectID)); err != nil {
		return err
	}
	if err := s.batchDeletePrefix(b, tiKVReplicaPrefix(objectID)); err != nil {
		return err
	}
	if err := s.batchDeletePrefix(b, tiKVECShardPrefix(objectID)); err != nil {
		return err
	}
	if err := b.Commit(kvstore.Sync); err != nil {
		return fmt.Errorf("commit delete normalized metadata failed: %w", err)
	}
	return nil
}

func (s *TiKVStore) GetObjectAdminView(ctx context.Context, objectID string) (*ObjectAdminView, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, found, err := s.getObjectRecord(tiKVObjectKey(objectID))
	if err != nil || !found {
		return nil, err
	}

	out := &ObjectAdminView{
		ObjectID:       obj.ObjectID,
		CurrentVersion: obj.CurrentVersion,
		State:          obj.State,
		CreatedAt:      obj.CreatedAt,
		UpdatedAt:      obj.UpdatedAt,
	}

	ver, found, err := s.getObjectVersionRecord(tiKVObjectVersionKey(objectID, obj.CurrentVersion))
	if err != nil {
		return nil, err
	}
	if found {
		v := &ObjectVersionAdminView{
			Version:        ver.Version,
			SizeBytes:      ver.SizeBytes,
			ChecksumSHA256: ver.ChecksumSHA256,
			Tier:           ver.Tier,
			CreatedAt:      ver.CreatedAt,
		}
		if ver.ContentType != nil {
			contentType := *ver.ContentType
			v.ContentType = &contentType
		}
		if ver.EncodingK != nil {
			encodingK := *ver.EncodingK
			v.EncodingK = &encodingK
		}
		if ver.EncodingM != nil {
			encodingM := *ver.EncodingM
			v.EncodingM = &encodingM
		}
		out.Version = v
	}

	replicas, err := s.listReplicaRecords(objectID, obj.CurrentVersion, "")
	if err != nil {
		return nil, err
	}
	repOut := make([]ReplicaLocationAdminView, 0, len(replicas))
	for _, r := range replicas {
		repOut = append(repOut, ReplicaLocationAdminView{
			NodeID: r.NodeID,
			Path:   r.Path,
			Status: r.Status,
		})
	}
	sort.Slice(repOut, func(i, j int) bool { return repOut[i].NodeID < repOut[j].NodeID })
	out.ReplicaLocations = repOut

	shards, err := s.listECShardRecords(objectID, obj.CurrentVersion)
	if err != nil {
		return nil, err
	}
	ecOut := make([]ECShardLocationAdminView, 0, len(shards))
	for _, sh := range shards {
		ecOut = append(ecOut, ECShardLocationAdminView{
			ShardIndex: sh.ShardIndex,
			NodeID:     sh.NodeID,
			Path:       sh.Path,
			Status:     sh.Status,
		})
	}
	sort.Slice(ecOut, func(i, j int) bool { return ecOut[i].ShardIndex < ecOut[j].ShardIndex })
	out.ECShardLocations = ecOut

	return out, nil
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

func (s *TiKVStore) EnqueueTieringCandidatesA1(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error) {
	return s.enqueueTieringCandidates(ctx, ageThresholdSec, maxObjects, 0, 0, false)
}

func (s *TiKVStore) EnqueueTieringCandidatesA2(ctx context.Context, ageThresholdSec int, sizeThresholdBytes int64, maxObjects int) (int, error) {
	if sizeThresholdBytes < 0 {
		sizeThresholdBytes = 0
	}
	return s.enqueueTieringCandidates(ctx, ageThresholdSec, maxObjects, sizeThresholdBytes, 0, false)
}

func (s *TiKVStore) EnqueueTieringCandidatesA3(ctx context.Context, ageThresholdSec int, maxObjects int, maxBytes int64) (int, error) {
	return s.enqueueTieringCandidates(ctx, ageThresholdSec, maxObjects, 0, maxBytes, true)
}

type tiKVTieringCandidate struct {
	ObjectID  string
	Version   int64
	SizeBytes int64
	UpdatedAt time.Time
}

func (s *TiKVStore) enqueueTieringCandidates(
	ctx context.Context,
	ageThresholdSec int,
	maxObjects int,
	minSizeBytes int64,
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

	objects, err := s.listObjects()
	if err != nil {
		return 0, err
	}
	eligibleBefore := time.Now().Add(-time.Duration(ageThresholdSec) * time.Second)
	candidates := make([]tiKVTieringCandidate, 0, len(objects))
	for _, o := range objects {
		if o.State != "HOT_ACTIVE" {
			continue
		}
		if o.UpdatedAt.After(eligibleBefore) {
			continue
		}
		ver, found, err := s.getObjectVersionRecord(tiKVObjectVersionKey(o.ObjectID, o.CurrentVersion))
		if err != nil {
			return 0, err
		}
		if !found || ver.Tier != "HOT" {
			continue
		}
		if minSizeBytes > 0 && ver.SizeBytes < minSizeBytes {
			continue
		}
		candidates = append(candidates, tiKVTieringCandidate{
			ObjectID:  o.ObjectID,
			Version:   o.CurrentVersion,
			SizeBytes: ver.SizeBytes,
			UpdatedAt: o.UpdatedAt,
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
				ScheduledAt: time.Now(),
			}
			if err := s.putJSON(taskKey, task); err != nil {
				return enqueued, err
			}
			enqueued++
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
		obj.UpdatedAt = time.Now()
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
		if err := s.putJSON(key, task); err != nil {
			return false, err
		}
		return true, nil
	}

	switch rec.TaskState {
	case "DONE", "FAILED":
		rec.TaskState = "PENDING"
		rec.Priority = 200
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
