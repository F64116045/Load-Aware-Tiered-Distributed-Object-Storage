package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// TieringLeaderState is the admin-visible scanner leadership snapshot.
type TieringLeaderState struct {
	LockKey         int64
	LeaderID        string
	ScannerStatus   string
	AcquiredAt      time.Time
	LastHeartbeatAt time.Time
}

// UpsertTieringLeaderState writes/refreshes scanner leadership heartbeat.
func (s *Store) UpsertTieringLeaderState(ctx context.Context, lockKey int64, leaderID, status string) error {
	if s == nil || s.db == nil {
		return nil
	}
	if leaderID == "" {
		return fmt.Errorf("leader id is empty")
	}
	if status == "" {
		status = "LEADING"
	}

	const q = `
INSERT INTO tiering_leader_state(lock_key, leader_id, scanner_status, acquired_at, last_heartbeat_at)
VALUES ($1, $2, $3, NOW(), NOW())
ON CONFLICT (lock_key)
DO UPDATE SET
	leader_id = EXCLUDED.leader_id,
	scanner_status = EXCLUDED.scanner_status,
	last_heartbeat_at = NOW()
`
	if _, err := s.db.ExecContext(ctx, q, lockKey, leaderID, status); err != nil {
		return fmt.Errorf("upsert tiering leader state failed: %w", err)
	}
	return nil
}

// MarkTieringLeaderStopped updates scanner status when lock holder stops.
func (s *Store) MarkTieringLeaderStopped(ctx context.Context, lockKey int64, leaderID, status string) error {
	if s == nil || s.db == nil {
		return nil
	}
	if leaderID == "" {
		return nil
	}
	if status == "" {
		status = "STOPPED"
	}

	const q = `
UPDATE tiering_leader_state
SET scanner_status = $3,
	last_heartbeat_at = NOW()
WHERE lock_key = $1
  AND leader_id = $2
`
	if _, err := s.db.ExecContext(ctx, q, lockKey, leaderID, status); err != nil {
		return fmt.Errorf("mark tiering leader stopped failed: %w", err)
	}
	return nil
}

// GetTieringLeaderState fetches current scanner leader state for a lock key.
func (s *Store) GetTieringLeaderState(ctx context.Context, lockKey int64) (*TieringLeaderState, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	const q = `
SELECT lock_key, leader_id, scanner_status, acquired_at, last_heartbeat_at
FROM tiering_leader_state
WHERE lock_key = $1
`
	var out TieringLeaderState
	if err := s.db.QueryRowContext(ctx, q, lockKey).Scan(
		&out.LockKey,
		&out.LeaderID,
		&out.ScannerStatus,
		&out.AcquiredAt,
		&out.LastHeartbeatAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query tiering leader state failed: %w", err)
	}
	return &out, nil
}
