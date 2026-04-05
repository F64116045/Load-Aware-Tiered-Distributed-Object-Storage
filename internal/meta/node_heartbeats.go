package meta

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// NodeHeartbeatSnapshot is an admin-facing view of node liveness/load.
type NodeHeartbeatSnapshot struct {
	NodeID       string
	LastSeenAt   time.Time
	FreeBytes    int64
	TotalBytes   int64
	IOQueueDepth int
	CPULoad      float64
	Status       string
}

// UpsertNodeHeartbeat writes node liveness and load stats.
func (s *Store) UpsertNodeHeartbeat(
	ctx context.Context,
	nodeID string,
	freeBytes int64,
	totalBytes int64,
	ioQueueDepth int,
	cpuLoad float64,
	status string,
) error {
	if s == nil || s.db == nil {
		return nil
	}

	const q = `
INSERT INTO node_heartbeats (node_id, last_seen_at, free_bytes, total_bytes, io_queue_depth, cpu_load, status)
VALUES ($1, NOW(), $2, $3, $4, $5, $6)
ON CONFLICT (node_id)
DO UPDATE SET
	last_seen_at = NOW(),
	free_bytes = EXCLUDED.free_bytes,
	total_bytes = EXCLUDED.total_bytes,
	io_queue_depth = EXCLUDED.io_queue_depth,
	cpu_load = EXCLUDED.cpu_load,
	status = EXCLUDED.status
`
	if _, err := s.db.ExecContext(ctx, q, nodeID, freeBytes, totalBytes, ioQueueDepth, cpuLoad, status); err != nil {
		return fmt.Errorf("upsert node heartbeat failed: %w", err)
	}
	return nil
}

// ListHealthyNodeIDs returns live node IDs observed within staleSec.
func (s *Store) ListHealthyNodeIDs(ctx context.Context, staleSec int) ([]string, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	if staleSec <= 0 {
		staleSec = 15
	}

	const q = `
SELECT node_id
FROM node_heartbeats
WHERE status = 'UP'
  AND last_seen_at >= NOW() - ($1 * INTERVAL '1 second')
`

	rows, err := s.db.QueryContext(ctx, q, staleSec)
	if err != nil {
		return nil, fmt.Errorf("list healthy nodes failed: %w", err)
	}
	defer rows.Close()

	nodes := make([]string, 0)
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, fmt.Errorf("scan healthy node failed: %w", err)
		}
		nodes = append(nodes, nodeID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate healthy nodes failed: %w", err)
	}
	sort.Strings(nodes)
	return nodes, nil
}

// ListNodeHeartbeats returns recent heartbeat snapshots ordered by last_seen_at desc.
func (s *Store) ListNodeHeartbeats(ctx context.Context, limit int) ([]NodeHeartbeatSnapshot, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	const q = `
SELECT node_id, last_seen_at, free_bytes, total_bytes, io_queue_depth, cpu_load, status
FROM node_heartbeats
ORDER BY last_seen_at DESC
LIMIT $1
`
	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("list node heartbeats failed: %w", err)
	}
	defer rows.Close()

	out := make([]NodeHeartbeatSnapshot, 0, limit)
	for rows.Next() {
		var n NodeHeartbeatSnapshot
		if err := rows.Scan(
			&n.NodeID,
			&n.LastSeenAt,
			&n.FreeBytes,
			&n.TotalBytes,
			&n.IOQueueDepth,
			&n.CPULoad,
			&n.Status,
		); err != nil {
			return nil, fmt.Errorf("scan node heartbeat failed: %w", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate node heartbeats failed: %w", err)
	}
	return out, nil
}
