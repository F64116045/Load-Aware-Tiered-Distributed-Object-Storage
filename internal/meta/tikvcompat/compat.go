package tikvcompat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
)

type Options struct{}

type WriteOptions int

const (
	NoSync WriteOptions = iota
	Sync
)

var ErrNotFound = errors.New("not found")

type DB struct {
	client *txnkv.Client
}

func Open(dsn string, _ *Options) (*DB, error) {
	pdAddrs, err := parsePDAddrs(dsn)
	if err != nil {
		return nil, err
	}
	c, err := txnkv.NewClient(pdAddrs)
	if err != nil {
		return nil, fmt.Errorf("create tikv client failed: %w", err)
	}
	return &DB{client: c}, nil
}

func (d *DB) Ping(ctx context.Context) error {
	if d == nil || d.client == nil {
		return nil
	}
	txn, err := d.client.Begin()
	if err != nil {
		return fmt.Errorf("tikv begin for ping failed: %w", err)
	}
	_, err = txn.Get(ctx, []byte("__meta_tikv_ping__"))
	_ = txn.Rollback()
	if err != nil && !tikverr.IsErrNotFound(err) {
		return fmt.Errorf("tikv ping get failed: %w", err)
	}
	return nil
}

func (d *DB) Close() error {
	if d == nil || d.client == nil {
		return nil
	}
	return d.client.Close()
}

func (d *DB) Set(key []byte, value []byte, _ WriteOptions) error {
	if d == nil || d.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	txn, err := d.client.Begin()
	if err != nil {
		return fmt.Errorf("tikv begin set failed: %w", err)
	}
	if err := txn.Set(key, value); err != nil {
		_ = txn.Rollback()
		return fmt.Errorf("tikv set failed: %w", err)
	}
	if err := txn.Commit(ctx); err != nil {
		_ = txn.Rollback()
		return fmt.Errorf("tikv commit set failed: %w", err)
	}
	return nil
}

func (d *DB) Delete(key []byte, _ WriteOptions) error {
	if d == nil || d.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	txn, err := d.client.Begin()
	if err != nil {
		return fmt.Errorf("tikv begin delete failed: %w", err)
	}
	if err := txn.Delete(key); err != nil {
		_ = txn.Rollback()
		return fmt.Errorf("tikv delete failed: %w", err)
	}
	if err := txn.Commit(ctx); err != nil {
		_ = txn.Rollback()
		return fmt.Errorf("tikv commit delete failed: %w", err)
	}
	return nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func (d *DB) Get(key []byte) ([]byte, io.Closer, error) {
	if d == nil || d.client == nil {
		return nil, nopCloser{}, ErrNotFound
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	txn, err := d.client.Begin()
	if err != nil {
		return nil, nopCloser{}, fmt.Errorf("tikv begin get failed: %w", err)
	}
	v, err := txn.Get(ctx, key)
	_ = txn.Rollback()
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, nopCloser{}, ErrNotFound
		}
		return nil, nopCloser{}, fmt.Errorf("tikv get failed: %w", err)
	}
	out := append([]byte(nil), v...)
	return out, nopCloser{}, nil
}

type batchOp struct {
	key    []byte
	value  []byte
	delete bool
}

type Batch struct {
	db  *DB
	ops []batchOp
}

func (d *DB) NewBatch() *Batch {
	return &Batch{db: d}
}

func (b *Batch) Set(key []byte, value []byte, _ WriteOptions) error {
	if b == nil {
		return nil
	}
	b.ops = append(b.ops, batchOp{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	})
	return nil
}

func (b *Batch) Delete(key []byte, _ WriteOptions) error {
	if b == nil {
		return nil
	}
	b.ops = append(b.ops, batchOp{
		key:    append([]byte(nil), key...),
		delete: true,
	})
	return nil
}

func (b *Batch) Commit(_ WriteOptions) error {
	if b == nil || b.db == nil || b.db.client == nil || len(b.ops) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	txn, err := b.db.client.Begin()
	if err != nil {
		return fmt.Errorf("tikv begin batch failed: %w", err)
	}
	for _, op := range b.ops {
		if op.delete {
			if err := txn.Delete(op.key); err != nil {
				_ = txn.Rollback()
				return fmt.Errorf("tikv batch delete failed: %w", err)
			}
			continue
		}
		if err := txn.Set(op.key, op.value); err != nil {
			_ = txn.Rollback()
			return fmt.Errorf("tikv batch set failed: %w", err)
		}
	}
	if err := txn.Commit(ctx); err != nil {
		_ = txn.Rollback()
		return fmt.Errorf("tikv batch commit failed: %w", err)
	}
	return nil
}

func (b *Batch) Close() error {
	if b == nil {
		return nil
	}
	b.ops = nil
	return nil
}

type IterOptions struct {
	LowerBound []byte
	UpperBound []byte
}

type Iterator struct {
	keys [][]byte
	vals [][]byte
	idx  int
	err  error
}

func (d *DB) NewIter(opts *IterOptions) (*Iterator, error) {
	if d == nil || d.client == nil {
		return &Iterator{idx: -1}, nil
	}

	var lower []byte
	var upper []byte
	if opts != nil {
		lower = append([]byte(nil), opts.LowerBound...)
		upper = append([]byte(nil), opts.UpperBound...)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	txn, err := d.client.Begin()
	if err != nil {
		return nil, fmt.Errorf("tikv begin iter failed: %w", err)
	}

	it, err := txn.Iter(lower, upper)
	if err != nil {
		_ = txn.Rollback()
		return nil, fmt.Errorf("tikv create iter failed: %w", err)
	}
	defer it.Close()
	defer txn.Rollback()

	out := &Iterator{idx: -1}
	for it.Valid() {
		key := append([]byte(nil), it.Key()...)
		val := append([]byte(nil), it.Value()...)
		out.keys = append(out.keys, key)
		out.vals = append(out.vals, val)
		if err := it.Next(); err != nil {
			out.err = fmt.Errorf("tikv iter next failed: %w", err)
			break
		}
	}
	_ = ctx
	return out, nil
}

func (d *DB) TryAcquireLock(ctx context.Context, key []byte, owner []byte) (bool, error) {
	return d.TryAcquireLockWithTTL(ctx, key, owner, 10*time.Second)
}

func (d *DB) TryAcquireLockWithTTL(ctx context.Context, key []byte, owner []byte, ttl time.Duration) (bool, error) {
	if d == nil || d.client == nil {
		return false, nil
	}
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	now := time.Now()
	newVal, err := marshalLockValue(owner, now.Add(ttl))
	if err != nil {
		return false, err
	}

	txn, err := d.client.Begin()
	if err != nil {
		return false, fmt.Errorf("tikv begin try-acquire-lock failed: %w", err)
	}
	v, err := txn.Get(ctx, key)
	switch {
	case err == nil:
		existingOwner, expiresAt, decErr := unmarshalLockValue(v)
		if decErr != nil {
			_ = txn.Rollback()
			return false, decErr
		}
		if existingOwner == string(owner) && expiresAt > now.UnixNano() {
			if err := txn.Set(key, newVal); err != nil {
				_ = txn.Rollback()
				return false, fmt.Errorf("tikv refresh owned lock failed: %w", err)
			}
			if err := txn.Commit(ctx); err != nil {
				_ = txn.Rollback()
				if tikverr.IsErrWriteConflict(err) {
					return false, nil
				}
				return false, fmt.Errorf("tikv commit refresh owned lock failed: %w", err)
			}
			return true, nil
		}
		if expiresAt > now.UnixNano() {
			_ = txn.Rollback()
			return false, nil
		}
		if err := txn.Set(key, newVal); err != nil {
			_ = txn.Rollback()
			return false, fmt.Errorf("tikv takeover stale lock failed: %w", err)
		}
		if err := txn.Commit(ctx); err != nil {
			_ = txn.Rollback()
			if tikverr.IsErrWriteConflict(err) {
				return false, nil
			}
			return false, fmt.Errorf("tikv commit stale lock takeover failed: %w", err)
		}
		return true, nil
	case tikverr.IsErrNotFound(err):
		if err := txn.Set(key, newVal); err != nil {
			_ = txn.Rollback()
			return false, fmt.Errorf("tikv set lock failed: %w", err)
		}
		if err := txn.Commit(ctx); err != nil {
			_ = txn.Rollback()
			if tikverr.IsErrWriteConflict(err) {
				return false, nil
			}
			return false, fmt.Errorf("tikv commit lock failed: %w", err)
		}
		return true, nil
	default:
		_ = txn.Rollback()
		return false, fmt.Errorf("tikv get lock failed: %w", err)
	}
}

func (d *DB) IsLockOwner(ctx context.Context, key []byte, owner []byte) (bool, error) {
	if d == nil || d.client == nil {
		return false, nil
	}
	txn, err := d.client.Begin()
	if err != nil {
		return false, fmt.Errorf("tikv begin is-lock-owner failed: %w", err)
	}
	v, err := txn.Get(ctx, key)
	_ = txn.Rollback()
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("tikv get lock owner failed: %w", err)
	}
	lockOwner, expiresAt, decErr := unmarshalLockValue(v)
	if decErr != nil {
		return false, decErr
	}
	if expiresAt <= time.Now().UnixNano() {
		return false, nil
	}
	return lockOwner == string(owner), nil
}

func (d *DB) RefreshLock(ctx context.Context, key []byte, owner []byte, ttl time.Duration) (bool, error) {
	if d == nil || d.client == nil {
		return false, nil
	}
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	txn, err := d.client.Begin()
	if err != nil {
		return false, fmt.Errorf("tikv begin refresh-lock failed: %w", err)
	}
	v, err := txn.Get(ctx, key)
	if err != nil {
		_ = txn.Rollback()
		if tikverr.IsErrNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("tikv get lock before refresh failed: %w", err)
	}
	lockOwner, expiresAt, decErr := unmarshalLockValue(v)
	if decErr != nil {
		_ = txn.Rollback()
		return false, decErr
	}
	now := time.Now()
	if lockOwner != string(owner) || expiresAt <= now.UnixNano() {
		_ = txn.Rollback()
		return false, nil
	}
	newVal, err := marshalLockValue(owner, now.Add(ttl))
	if err != nil {
		_ = txn.Rollback()
		return false, err
	}
	if err := txn.Set(key, newVal); err != nil {
		_ = txn.Rollback()
		return false, fmt.Errorf("tikv refresh lock set failed: %w", err)
	}
	if err := txn.Commit(ctx); err != nil {
		_ = txn.Rollback()
		if tikverr.IsErrWriteConflict(err) {
			return false, nil
		}
		return false, fmt.Errorf("tikv refresh lock commit failed: %w", err)
	}
	return true, nil
}

func (d *DB) ReleaseLock(ctx context.Context, key []byte, owner []byte) error {
	if d == nil || d.client == nil {
		return nil
	}
	txn, err := d.client.Begin()
	if err != nil {
		return fmt.Errorf("tikv begin release-lock failed: %w", err)
	}
	v, err := txn.Get(ctx, key)
	if err != nil {
		_ = txn.Rollback()
		if tikverr.IsErrNotFound(err) {
			return nil
		}
		return fmt.Errorf("tikv get lock before release failed: %w", err)
	}
	lockOwner, _, decErr := unmarshalLockValue(v)
	if decErr != nil {
		_ = txn.Rollback()
		return decErr
	}
	if !bytes.Equal([]byte(lockOwner), owner) {
		_ = txn.Rollback()
		return nil
	}
	if err := txn.Delete(key); err != nil {
		_ = txn.Rollback()
		return fmt.Errorf("tikv delete lock failed: %w", err)
	}
	if err := txn.Commit(ctx); err != nil {
		_ = txn.Rollback()
		return fmt.Errorf("tikv commit release-lock failed: %w", err)
	}
	return nil
}

type lockValue struct {
	Owner             string `json:"owner"`
	ExpiresAtUnixNano int64  `json:"expires_at_unix_nano"`
}

func marshalLockValue(owner []byte, expiresAt time.Time) ([]byte, error) {
	v := lockValue{
		Owner:             string(owner),
		ExpiresAtUnixNano: expiresAt.UnixNano(),
	}
	out, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal lock value failed: %w", err)
	}
	return out, nil
}

func unmarshalLockValue(raw []byte) (string, int64, error) {
	var v lockValue
	if err := json.Unmarshal(raw, &v); err != nil {
		// Backward-compatible fallback: raw bytes were owner token only.
		// Treat as expired to avoid permanently stuck locks after upgrades.
		if len(raw) == 0 {
			return "", 0, nil
		}
		return string(raw), 0, nil
	}
	return v.Owner, v.ExpiresAtUnixNano, nil
}

func (it *Iterator) First() bool {
	if it == nil || len(it.keys) == 0 {
		if it != nil {
			it.idx = -1
		}
		return false
	}
	it.idx = 0
	return true
}

func (it *Iterator) Valid() bool {
	return it != nil && it.idx >= 0 && it.idx < len(it.keys)
}

func (it *Iterator) Next() bool {
	if it == nil {
		return false
	}
	it.idx++
	return it.idx >= 0 && it.idx < len(it.keys)
}

func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.keys[it.idx]
}

func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.vals[it.idx]
}

func (it *Iterator) Error() error {
	if it == nil {
		return nil
	}
	return it.err
}

func (it *Iterator) Close() error {
	return nil
}

func parsePDAddrs(dsn string) ([]string, error) {
	raw := strings.TrimSpace(dsn)
	if raw == "" {
		return nil, fmt.Errorf("empty tikv dsn")
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("invalid tikv dsn: %q", dsn)
	}
	return out, nil
}
