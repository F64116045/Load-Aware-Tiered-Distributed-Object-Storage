package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"hybrid_distributed_store/internal/config"
)

const (
	rocksPrefixObject  = "obj/"
	rocksPrefixObjVer  = "objv/"
	rocksPrefixReplica = "repl/"
	rocksPrefixECShard = "ec/"
	rocksPrefixTask    = "task/"
	rocksPrefixHB      = "hb/"
	rocksPrefixLeader  = "leader/"
)

type rocksObjectRecord struct {
	ObjectID       string    `json:"object_id"`
	TenantID       string    `json:"tenant_id"`
	CurrentVersion int64     `json:"current_version"`
	State          string    `json:"state"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type rocksObjectVersionRecord struct {
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

type rocksReplicaRecord struct {
	ObjectID string `json:"object_id"`
	Version  int64  `json:"version"`
	NodeID   string `json:"node_id"`
	Path     string `json:"path"`
	Status   string `json:"status"`
}

type rocksECShardRecord struct {
	ObjectID   string `json:"object_id"`
	Version    int64  `json:"version"`
	ShardIndex int    `json:"shard_index"`
	NodeID     string `json:"node_id"`
	Path       string `json:"path"`
	Status     string `json:"status"`
}

type rocksTaskRecord struct {
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

type rocksLeaderLock struct {
	file *os.File
}

func (l *rocksLeaderLock) Ping(ctx context.Context) error {
	if l == nil || l.file == nil {
		return fmt.Errorf("rocks leader lock is nil")
	}
	if _, err := l.file.Stat(); err != nil {
		return fmt.Errorf("rocks leader lock stat failed: %w", err)
	}
	return nil
}

func (l *rocksLeaderLock) Release(ctx context.Context) error {
	if l == nil || l.file == nil {
		return nil
	}
	fd := int(l.file.Fd())
	_ = syscall.Flock(fd, syscall.LOCK_UN)
	err := l.file.Close()
	l.file = nil
	if err != nil {
		return fmt.Errorf("close rocks leader lock failed: %w", err)
	}
	return nil
}

// RocksStore is a pebble-backed metadata repository.
// Note: pebble itself is single-process per DB path. For multi-process metadata,
// run a dedicated metadata service process and route all metadata operations through it.
type RocksStore struct {
	db   *pebble.DB
	path string
	mu   sync.RWMutex
}

func NewRocksStore(cfg Config) (*RocksStore, error) {
	if !cfg.Enabled {
		return &RocksStore{}, nil
	}

	path, err := resolveRocksPath(cfg.DSN)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("create rocks path failed: %w", err)
	}

	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open rocks store failed: %w", err)
	}

	return &RocksStore{
		db:   db,
		path: path,
	}, nil
}

func resolveRocksPath(dsn string) (string, error) {
	raw := strings.TrimSpace(dsn)
	if raw == "" {
		return "", fmt.Errorf("meta dsn path is required for rocks backend")
	}
	if strings.HasPrefix(raw, "file://") {
		p := strings.TrimPrefix(raw, "file://")
		if p == "" {
			return "", fmt.Errorf("invalid rocks file DSN: %q", dsn)
		}
		return p, nil
	}
	if strings.Contains(raw, "://") {
		return "", fmt.Errorf("rocks backend expects filesystem path DSN, got %q", dsn)
	}
	return raw, nil
}

func (s *RocksStore) Ping(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	// Keep ping read-only to avoid introducing write amplification during health checks.
	it, err := s.db.NewIter(nil)
	if err != nil {
		return fmt.Errorf("rocks ping iterator failed: %w", err)
	}
	defer it.Close()
	return nil
}

func (s *RocksStore) DB() *sql.DB {
	return nil
}

func (s *RocksStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *RocksStore) TryAcquireLeaderLock(ctx context.Context, key int64) (LeaderLock, bool, error) {
	if s == nil || s.db == nil {
		return nil, false, nil
	}

	lockDir := filepath.Join(s.path, "locks")
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		return nil, false, fmt.Errorf("create lock dir failed: %w", err)
	}
	lockPath := filepath.Join(lockDir, fmt.Sprintf("leader_%d.lock", key))
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, false, fmt.Errorf("open lock file failed: %w", err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("acquire flock failed: %w", err)
	}
	return &rocksLeaderLock{file: f}, true, nil
}

func (s *RocksStore) UpsertTieringLeaderState(ctx context.Context, lockKey int64, leaderID, status string) error {
	if s == nil || s.db == nil {
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

	key := rocksLeaderKey(lockKey)
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

func (s *RocksStore) MarkTieringLeaderStopped(ctx context.Context, lockKey int64, leaderID, status string) error {
	if s == nil || s.db == nil {
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
	key := rocksLeaderKey(lockKey)
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

func (s *RocksStore) GetTieringLeaderState(ctx context.Context, lockKey int64) (*TieringLeaderState, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := rocksLeaderKey(lockKey)
	rec, found, err := s.getLeaderRecord(key)
	if err != nil || !found {
		return nil, err
	}
	return rec, nil
}

func (s *RocksStore) UpsertNodeHeartbeat(ctx context.Context, nodeID string, freeBytes int64, ioQueueDepth int, cpuLoad float64, status string) error {
	if s == nil || s.db == nil {
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
		IOQueueDepth: ioQueueDepth,
		CPULoad:      cpuLoad,
		Status:       status,
	}
	return s.putJSON(rocksHeartbeatKey(nodeID), &rec)
}

func (s *RocksStore) ListHealthyNodeIDs(ctx context.Context, staleSec int) ([]string, error) {
	if s == nil || s.db == nil {
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

func (s *RocksStore) ListNodeHeartbeats(ctx context.Context, limit int) ([]NodeHeartbeatSnapshot, error) {
	if s == nil || s.db == nil {
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

func (s *RocksStore) UpsertNormalizedMetadata(ctx context.Context, objectID string, metadata map[string]interface{}) error {
	if s == nil || s.db == nil {
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

	objKey := rocksObjectKey(objectID)
	obj, found, err := s.getObjectRecord(objKey)
	if err != nil {
		return err
	}
	if !found {
		obj = &rocksObjectRecord{
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

	verRec := &rocksObjectVersionRecord{
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

	b := s.db.NewBatch()
	defer b.Close()

	if err := s.batchPutJSON(b, objKey, obj); err != nil {
		return err
	}
	if err := s.batchPutJSON(b, rocksObjectVersionKey(objectID, version), verRec); err != nil {
		return err
	}

	if tier == "HOT" {
		replicaNodes := toStringSlice(metadata["replica_nodes"])
		for _, nodeID := range replicaNodes {
			if nodeID == "" {
				continue
			}
			rec := rocksReplicaRecord{
				ObjectID: objectID,
				Version:  version,
				NodeID:   nodeID,
				Path:     objectID,
				Status:   "ACTIVE",
			}
			if err := s.batchPutJSON(b, rocksReplicaKey(objectID, version, nodeID), &rec); err != nil {
				return err
			}
		}
	}

	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit normalized metadata batch failed: %w", err)
	}
	return nil
}

func (s *RocksStore) GetNormalizedMetadata(ctx context.Context, objectID string) (map[string]interface{}, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, found, err := s.getObjectRecord(rocksObjectKey(objectID))
	if err != nil || !found {
		return nil, err
	}
	ver, found, err := s.getObjectVersionRecord(rocksObjectVersionKey(objectID, obj.CurrentVersion))
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

func (s *RocksStore) DeleteNormalizedMetadata(ctx context.Context, objectID string) error {
	if s == nil || s.db == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.db.NewBatch()
	defer b.Close()

	_ = b.Delete([]byte(rocksObjectKey(objectID)), pebble.NoSync)
	if err := s.batchDeletePrefix(b, rocksObjectVersionPrefix(objectID)); err != nil {
		return err
	}
	if err := s.batchDeletePrefix(b, rocksReplicaPrefix(objectID)); err != nil {
		return err
	}
	if err := s.batchDeletePrefix(b, rocksECShardPrefix(objectID)); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit delete normalized metadata failed: %w", err)
	}
	return nil
}

func (s *RocksStore) GetObjectAdminView(ctx context.Context, objectID string) (*ObjectAdminView, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, found, err := s.getObjectRecord(rocksObjectKey(objectID))
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

	ver, found, err := s.getObjectVersionRecord(rocksObjectVersionKey(objectID, obj.CurrentVersion))
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
			v.ContentType = sql.NullString{String: *ver.ContentType, Valid: true}
		}
		if ver.EncodingK != nil {
			v.EncodingK = sql.NullInt64{Int64: int64(*ver.EncodingK), Valid: true}
		}
		if ver.EncodingM != nil {
			v.EncodingM = sql.NullInt64{Int64: int64(*ver.EncodingM), Valid: true}
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

func (s *RocksStore) EnqueueTieringTask(ctx context.Context, taskID, objectID string, version int64, taskType string, priority int, scheduledAt time.Time) error {
	if s == nil || s.db == nil {
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

	key := rocksTaskKey(taskID)
	_, found, err := s.getTaskRecord(key)
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	rec := &rocksTaskRecord{
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

func (s *RocksStore) ListTieringTasks(ctx context.Context, taskState, taskType string, limit int) ([]TieringTask, error) {
	if s == nil || s.db == nil {
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
	filtered := make([]rocksTaskRecord, 0, len(recs))
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
		out = append(out, toTieringTask(r))
	}
	return out, nil
}

func (s *RocksStore) ListTieringTaskStateCounts(ctx context.Context, taskType string) (map[string]int64, error) {
	if s == nil || s.db == nil {
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

func (s *RocksStore) RequeueTieringTaskNow(ctx context.Context, taskID string) (bool, error) {
	if s == nil || s.db == nil {
		return false, nil
	}
	if taskID == "" {
		return false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := rocksTaskKey(taskID)
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

func (s *RocksStore) CancelTieringTask(ctx context.Context, taskID, reason string) (bool, error) {
	if s == nil || s.db == nil {
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
	key := rocksTaskKey(taskID)
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

func (s *RocksStore) ClaimNextTieringTask(ctx context.Context, taskType string) (*TieringTask, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	recs, err := s.listTaskRecords()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	candidates := make([]rocksTaskRecord, 0, len(recs))
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
	if err := s.putJSON(rocksTaskKey(selected.TaskID), &selected); err != nil {
		return nil, err
	}
	t := toTieringTask(selected)
	return &t, nil
}

func (s *RocksStore) MarkTieringTaskDone(ctx context.Context, taskID string) error {
	if s == nil || s.db == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := rocksTaskKey(taskID)
	rec, found, err := s.getTaskRecord(key)
	if err != nil || !found {
		return err
	}
	now := time.Now()
	rec.TaskState = "DONE"
	rec.FinishedAt = &now
	return s.putJSON(key, rec)
}

func (s *RocksStore) MarkTieringTaskRetry(ctx context.Context, taskID, lastErr string, nextRunAt time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	if nextRunAt.IsZero() {
		nextRunAt = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := rocksTaskKey(taskID)
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

func (s *RocksStore) MarkTieringTaskFailed(ctx context.Context, taskID, lastErr string) error {
	if s == nil || s.db == nil {
		return nil
	}
	if lastErr == "" {
		lastErr = "failed_without_error_message"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := rocksTaskKey(taskID)
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

func (s *RocksStore) EnqueueTieringCandidatesA1(ctx context.Context, ageThresholdSec int, maxObjects int) (int, error) {
	if s == nil || s.db == nil {
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
	candidates := make([]rocksObjectRecord, 0, len(objects))
	for _, o := range objects {
		if o.State != "HOT_ACTIVE" {
			continue
		}
		if o.UpdatedAt.After(eligibleBefore) {
			continue
		}
		candidates = append(candidates, o)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].UpdatedAt.Before(candidates[j].UpdatedAt)
	})
	if len(candidates) > maxObjects {
		candidates = candidates[:maxObjects]
	}

	enqueued := 0
	for _, c := range candidates {
		taskID := fmt.Sprintf("repl2ec:%s:%d", c.ObjectID, c.CurrentVersion)
		taskKey := rocksTaskKey(taskID)
		_, found, err := s.getTaskRecord(taskKey)
		if err != nil {
			return enqueued, err
		}
		if !found {
			task := &rocksTaskRecord{
				TaskID:      taskID,
				ObjectID:    c.ObjectID,
				Version:     c.CurrentVersion,
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

		c.State = "MIGRATION_PENDING"
		c.UpdatedAt = time.Now()
		if err := s.putJSON(rocksObjectKey(c.ObjectID), &c); err != nil {
			return enqueued, err
		}
	}

	return enqueued, nil
}

func (s *RocksStore) EnqueueRepairCandidates(ctx context.Context, maxObjects int) (int, error) {
	if s == nil || s.db == nil {
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

		ver, found, err := s.getObjectVersionRecord(rocksObjectVersionKey(o.ObjectID, o.CurrentVersion))
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

func (s *RocksStore) enqueueRepairTask(taskID, objectID string, version int64) (bool, error) {
	now := time.Now()
	key := rocksTaskKey(taskID)
	rec, found, err := s.getTaskRecord(key)
	if err != nil {
		return false, err
	}
	if !found {
		task := &rocksTaskRecord{
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

func (s *RocksStore) GetObjectVersionSnapshot(ctx context.Context, objectID string, taskVersion int64) (*ObjectVersionSnapshot, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, found, err := s.getObjectRecord(rocksObjectKey(objectID))
	if err != nil || !found {
		return nil, err
	}
	ver, found, err := s.getObjectVersionRecord(rocksObjectVersionKey(objectID, taskVersion))
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

func (s *RocksStore) MarkObjectMigrating(ctx context.Context, objectID string, version int64) error {
	if s == nil || s.db == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, found, err := s.getObjectRecord(rocksObjectKey(objectID))
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
	return s.putJSON(rocksObjectKey(objectID), obj)
}

func (s *RocksStore) PromoteObjectVersionToEC(ctx context.Context, objectID string, version int64, checksum string, k int, m int, locations []ECShardLocation) error {
	if s == nil || s.db == nil {
		return nil
	}
	if len(locations) == 0 {
		return fmt.Errorf("no ec shard locations to commit")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	obj, found, err := s.getObjectRecord(rocksObjectKey(objectID))
	if err != nil || !found {
		return fmt.Errorf("object version is no longer current during ec promotion")
	}
	if obj.CurrentVersion != version {
		return fmt.Errorf("object version is no longer current during ec promotion")
	}
	ver, found, err := s.getObjectVersionRecord(rocksObjectVersionKey(objectID, version))
	if err != nil || !found {
		return fmt.Errorf("object version missing during ec promotion")
	}

	b := s.db.NewBatch()
	defer b.Close()

	for _, loc := range locations {
		status := loc.Status
		if status == "" {
			status = "ACTIVE"
		}
		rec := rocksECShardRecord{
			ObjectID:   objectID,
			Version:    version,
			ShardIndex: loc.ShardIndex,
			NodeID:     loc.NodeID,
			Path:       loc.Path,
			Status:     status,
		}
		if err := s.batchPutJSON(b, rocksECShardKey(objectID, version, loc.ShardIndex), &rec); err != nil {
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
	if err := s.batchPutJSON(b, rocksObjectVersionKey(objectID, version), ver); err != nil {
		return err
	}

	obj.State = "EC_ACTIVE"
	obj.UpdatedAt = time.Now()
	if err := s.batchPutJSON(b, rocksObjectKey(objectID), obj); err != nil {
		return err
	}

	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit promote ec batch failed: %w", err)
	}
	return nil
}

func (s *RocksStore) ListActiveReplicaLocations(ctx context.Context, objectID string, version int64) ([]ReplicaLocation, error) {
	if s == nil || s.db == nil {
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

func (s *RocksStore) UpsertReplicaLocations(ctx context.Context, objectID string, version int64, nodeIDs []string) error {
	if s == nil || s.db == nil {
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
		rec := rocksReplicaRecord{
			ObjectID: objectID,
			Version:  version,
			NodeID:   nodeID,
			Path:     objectID,
			Status:   "ACTIVE",
		}
		if err := s.putJSON(rocksReplicaKey(objectID, version, nodeID), &rec); err != nil {
			return err
		}
	}
	return nil
}

func (s *RocksStore) MarkReplicaLocationsDeleted(ctx context.Context, objectID string, version int64, nodeIDs []string) error {
	if s == nil || s.db == nil {
		return nil
	}
	if len(nodeIDs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, nodeID := range nodeIDs {
		key := rocksReplicaKey(objectID, version, nodeID)
		rec := &rocksReplicaRecord{}
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

func (s *RocksStore) listObjects() ([]rocksObjectRecord, error) {
	it, err := s.newPrefixIter(rocksPrefixObject)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]rocksObjectRecord, 0)
	for it.First(); it.Valid(); it.Next() {
		var rec rocksObjectRecord
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

func (s *RocksStore) listHeartbeats() ([]NodeHeartbeatSnapshot, error) {
	it, err := s.newPrefixIter(rocksPrefixHB)
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

func (s *RocksStore) listTaskRecords() ([]rocksTaskRecord, error) {
	it, err := s.newPrefixIter(rocksPrefixTask)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]rocksTaskRecord, 0)
	for it.First(); it.Valid(); it.Next() {
		var rec rocksTaskRecord
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

func (s *RocksStore) listReplicaRecords(objectID string, version int64, status string) ([]rocksReplicaRecord, error) {
	prefix := rocksReplicaVersionPrefix(objectID, version)
	it, err := s.newPrefixIter(prefix)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]rocksReplicaRecord, 0)
	for it.First(); it.Valid(); it.Next() {
		var rec rocksReplicaRecord
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

func (s *RocksStore) listECShardRecords(objectID string, version int64) ([]rocksECShardRecord, error) {
	prefix := rocksECShardVersionPrefix(objectID, version)
	it, err := s.newPrefixIter(prefix)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]rocksECShardRecord, 0)
	for it.First(); it.Valid(); it.Next() {
		var rec rocksECShardRecord
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

func (s *RocksStore) getObjectRecord(key string) (*rocksObjectRecord, bool, error) {
	var rec rocksObjectRecord
	found, err := s.getJSON(key, &rec)
	if err != nil || !found {
		return nil, found, err
	}
	return &rec, true, nil
}

func (s *RocksStore) getObjectVersionRecord(key string) (*rocksObjectVersionRecord, bool, error) {
	var rec rocksObjectVersionRecord
	found, err := s.getJSON(key, &rec)
	if err != nil || !found {
		return nil, found, err
	}
	return &rec, true, nil
}

func (s *RocksStore) getTaskRecord(key string) (*rocksTaskRecord, bool, error) {
	var rec rocksTaskRecord
	found, err := s.getJSON(key, &rec)
	if err != nil || !found {
		return nil, found, err
	}
	return &rec, true, nil
}

func (s *RocksStore) getLeaderRecord(key string) (*TieringLeaderState, bool, error) {
	var rec TieringLeaderState
	found, err := s.getJSON(key, &rec)
	if err != nil || !found {
		return nil, found, err
	}
	return &rec, true, nil
}

func (s *RocksStore) putJSON(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value for key=%s failed: %w", key, err)
	}
	if err := s.db.Set([]byte(key), data, pebble.Sync); err != nil {
		return fmt.Errorf("set key=%s failed: %w", key, err)
	}
	return nil
}

func (s *RocksStore) getJSON(key string, out interface{}) (bool, error) {
	v, closer, err := s.db.Get([]byte(key))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
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

func (s *RocksStore) batchPutJSON(b *pebble.Batch, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal batch value for key=%s failed: %w", key, err)
	}
	if err := b.Set([]byte(key), data, pebble.NoSync); err != nil {
		return fmt.Errorf("batch set key=%s failed: %w", key, err)
	}
	return nil
}

func (s *RocksStore) batchDeletePrefix(b *pebble.Batch, prefix string) error {
	it, err := s.newPrefixIter(prefix)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if err := b.Delete(it.Key(), pebble.NoSync); err != nil {
			return fmt.Errorf("batch delete key=%s failed: %w", string(it.Key()), err)
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("iterate prefix=%s for delete failed: %w", prefix, err)
	}
	return nil
}

func (s *RocksStore) newPrefixIter(prefix string) (*pebble.Iterator, error) {
	upper := prefixUpperBound([]byte(prefix))
	return s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: upper,
	})
}

func prefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func toTieringTask(r rocksTaskRecord) TieringTask {
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
		out.LastError = sql.NullString{String: *r.LastError, Valid: true}
	}
	if r.StartedAt != nil {
		out.StartedAt = sql.NullTime{Time: *r.StartedAt, Valid: true}
	}
	if r.FinishedAt != nil {
		out.FinishedAt = sql.NullTime{Time: *r.FinishedAt, Valid: true}
	}
	return out
}

func rocksObjectKey(objectID string) string {
	return rocksPrefixObject + objectID
}

func rocksObjectVersionKey(objectID string, version int64) string {
	return rocksPrefixObjVer + objectID + "/" + encodeInt64(version)
}

func rocksObjectVersionPrefix(objectID string) string {
	return rocksPrefixObjVer + objectID + "/"
}

func rocksReplicaKey(objectID string, version int64, nodeID string) string {
	return rocksPrefixReplica + objectID + "/" + encodeInt64(version) + "/" + nodeID
}

func rocksReplicaPrefix(objectID string) string {
	return rocksPrefixReplica + objectID + "/"
}

func rocksReplicaVersionPrefix(objectID string, version int64) string {
	return rocksPrefixReplica + objectID + "/" + encodeInt64(version) + "/"
}

func rocksECShardKey(objectID string, version int64, shardIndex int) string {
	return rocksPrefixECShard + objectID + "/" + encodeInt64(version) + "/" + encodeInt(shardIndex)
}

func rocksECShardPrefix(objectID string) string {
	return rocksPrefixECShard + objectID + "/"
}

func rocksECShardVersionPrefix(objectID string, version int64) string {
	return rocksPrefixECShard + objectID + "/" + encodeInt64(version) + "/"
}

func rocksTaskKey(taskID string) string {
	return rocksPrefixTask + taskID
}

func rocksHeartbeatKey(nodeID string) string {
	return rocksPrefixHB + nodeID
}

func rocksLeaderKey(lockKey int64) string {
	return rocksPrefixLeader + strconv.FormatInt(lockKey, 10)
}

func encodeInt64(v int64) string {
	return fmt.Sprintf("%020d", v)
}

func encodeInt(v int) string {
	return fmt.Sprintf("%010d", v)
}
