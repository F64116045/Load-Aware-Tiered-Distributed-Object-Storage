package meta

import (
	"context"
	"database/sql"
	"fmt"
)

// AdvisoryLock holds a PostgreSQL session-level advisory lock.
// The lock remains held as long as this connection stays alive.
type AdvisoryLock struct {
	key  int64
	conn *sql.Conn
}

// TryAcquireAdvisoryLock attempts to acquire a session-level advisory lock.
// Returns (nil, false, nil) when lock is held by another session.
func (s *Store) TryAcquireAdvisoryLock(ctx context.Context, key int64) (*AdvisoryLock, bool, error) {
	if s == nil || s.db == nil {
		return nil, false, nil
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("open advisory lock conn failed: %w", err)
	}

	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, false, fmt.Errorf("try advisory lock failed: %w", err)
	}
	if !acquired {
		_ = conn.Close()
		return nil, false, nil
	}

	return &AdvisoryLock{
		key:  key,
		conn: conn,
	}, true, nil
}

// Ping checks whether the underlying lock session is still alive.
func (l *AdvisoryLock) Ping(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return fmt.Errorf("advisory lock conn is nil")
	}
	if err := l.conn.PingContext(ctx); err != nil {
		return fmt.Errorf("advisory lock conn ping failed: %w", err)
	}
	return nil
}

// Release unlocks advisory lock and closes the held session connection.
func (l *AdvisoryLock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}

	var unlockErr error
	var unlocked bool
	if err := l.conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", l.key).Scan(&unlocked); err != nil {
		unlockErr = fmt.Errorf("advisory unlock failed: %w", err)
	}
	if closeErr := l.conn.Close(); closeErr != nil && unlockErr == nil {
		unlockErr = fmt.Errorf("advisory lock conn close failed: %w", closeErr)
	}
	l.conn = nil
	return unlockErr
}
