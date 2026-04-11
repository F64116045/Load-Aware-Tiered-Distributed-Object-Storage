package meta

import (
	"context"
	"fmt"
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
