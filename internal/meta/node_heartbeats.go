package meta

import (
	"context"
	"fmt"
	"sort"
)

// UpsertNodeHeartbeat writes node liveness and load stats.
func (s *Store) UpsertNodeHeartbeat(
	ctx context.Context,
	nodeID string,
	freeBytes int64,
	ioQueueDepth int,
	cpuLoad float64,
	status string,
) error {
	if s == nil || s.db == nil {
		return nil
	}

	const q = `
INSERT INTO node_heartbeats (node_id, last_seen_at, free_bytes, io_queue_depth, cpu_load, status)
VALUES ($1, NOW(), $2, $3, $4, $5)
ON CONFLICT (node_id)
DO UPDATE SET
	last_seen_at = NOW(),
	free_bytes = EXCLUDED.free_bytes,
	io_queue_depth = EXCLUDED.io_queue_depth,
	cpu_load = EXCLUDED.cpu_load,
	status = EXCLUDED.status
`
	if _, err := s.db.ExecContext(ctx, q, nodeID, freeBytes, ioQueueDepth, cpuLoad, status); err != nil {
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
