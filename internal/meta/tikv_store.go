package meta

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

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
