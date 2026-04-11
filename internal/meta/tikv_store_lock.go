package meta

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

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

func tiKVNewLockOwnerToken() []byte {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return []byte(fmt.Sprintf("owner-%d", time.Now().UnixNano()))
	}
	return []byte(hex.EncodeToString(b))
}
