package meta

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	kvstore "hybrid_distributed_store/internal/meta/kvstore"
)

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
